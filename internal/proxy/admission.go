package proxy

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/amitkhanal596/tollgate/internal/auth"
	"github.com/amitkhanal596/tollgate/internal/ratelimit"
)

// Rate-limit response headers (ADR 0004).
const (
	headerRateLimitLimit     = "X-RateLimit-Limit"
	headerRateLimitRemaining = "X-RateLimit-Remaining"
)

// Reason labels of tollgate_ratelimited_total and tollgate_auth_failures_total
// (architecture.md section 9): small closed sets, never request data.
const (
	reasonLimit        = "limit"         // the client's bucket is empty
	reasonCapacity     = "capacity"      // the key table is full and nothing idle could be evicted
	reasonAuthThrottle = "auth_throttle" // the failed-auth throttle refused the attempt
	reasonMissing      = "missing"       // no credential presented
	reasonInvalid      = "invalid"       // a credential was presented and failed verification
)

// admit runs steps 2 (auth) and 3 (rate limit) of the request path
// (docs/architecture.md section 4) for route. On success it records the
// identity and the rate-limit decision in st and returns true. Otherwise it
// has written the rejection (401 or 429) and returns false.
//
// Order matters and is part of the contract: the failed-auth throttle is
// checked before any verification, authentication before the rate limit (so
// unauthenticated requests never spend a client's tokens), and all of it
// before the balancer.
func (g *Gateway) admit(w http.ResponseWriter, r *http.Request, route *routeRT, st *proxyState) bool {
	if route.authn == nil && route.limit == nil {
		// Public, unlimited route: no client-IP parsing, no allocation.
		return true
	}
	// RemoteAddr only; X-Forwarded-For is client-controlled and not trusted
	// in v1 (architecture.md section 10 item 4).
	ipKey := auth.ClientIPKey(r.RemoteAddr)
	identity := ipKey

	if route.authn != nil {
		// Peek BEFORE verifying: an IP that has used up its failed-auth
		// budget gets 429 without any verification work, even if this
		// request carries valid (or no) credentials. That trades a short
		// lockout of legitimate clients behind a brute-forcing IP (or NAT)
		// for a hard bound on brute-force attempts and on RS256 CPU cost.
		if d := g.authLimiter.Peek(ipKey, authFailureLimit()); !d.Allowed {
			g.logger.Debug("failed-auth throttle", "request_id", st.requestID, "route", route.name)
			// Not an auth failure: nothing was verified.
			g.metrics.RateLimited(route.name, reasonAuthThrottle)
			writeTooManyRequests(w, d.RetryAfter, st.requestID)
			return false
		}
		id, err := route.authn.Authenticate(r)
		if err != nil {
			reason := reasonInvalid
			if errors.Is(err, auth.ErrMissingCredentials) {
				reason = reasonMissing
			}
			// Only wrong credentials cost a token: missing ones cost the
			// gateway no crypto, and successes must never lock anyone out.
			if errors.Is(err, auth.ErrInvalidCredentials) {
				// Fail closed for FAILED attempts when the throttle is full
				// and cannot track this IP: answering 401 would let a client
				// with fresh addresses guess credentials unthrottled, and the
				// 401-vs-200 difference is exactly the oracle it wants. 429
				// reveals nothing about the verification result. Valid
				// credentials never reach this branch, so legitimate clients
				// from untracked IPs are unaffected.
				if d := g.authLimiter.Allow(ipKey, authFailureLimit()); d.AtCapacity {
					g.logger.Debug("failed-auth throttle at capacity", "request_id", st.requestID, "route", route.name)
					g.metrics.RateLimited(route.name, reasonAuthThrottle)
					writeTooManyRequests(w, d.RetryAfter, st.requestID)
					return false
				}
			}
			g.metrics.AuthFailed(route.name, reason)
			// Debug, not Info: a credential-guessing client would otherwise
			// turn every attempt into a log line. The auth errors never
			// contain the presented credential.
			g.logger.Debug("authentication failed", "request_id", st.requestID, "route", route.name, "err", err)
			// The same challenge and body for every failure kind, so a
			// client cannot tell a missing credential from a wrong one.
			w.Header().Set("WWW-Authenticate", route.challenge)
			writeError(w, http.StatusUnauthorized, st.requestID)
			return false
		}
		identity = id.ClientID
	}

	if route.limit != nil {
		// IP-keyed (anonymous) and authenticated identities live in separate
		// limiters, so an anonymous flood from many addresses cannot fill
		// the key table that authenticated clients need.
		lim := g.limiter
		if route.authn == nil {
			lim = g.anonLimiter
		}
		// The NUL separator keeps (route, identity) pairs from colliding.
		d := lim.Allow(route.limitKeyPrefix+identity, *route.limit)
		if !d.Allowed {
			setRateLimitHeaders(w.Header(), d)
			g.logger.Debug("rate limited", "request_id", st.requestID, "route", route.name, "at_capacity", d.AtCapacity)
			reason := reasonLimit
			if d.AtCapacity {
				reason = reasonCapacity
			}
			g.metrics.RateLimited(route.name, reason)
			writeTooManyRequests(w, d.RetryAfter, st.requestID)
			return false
		}
		// Written later, with the response, so they survive ReverseProxy's
		// 1xx header clearing and override any upstream copy.
		st.rl, st.limited = d, true
	}
	// Recorded only now that the request is admitted: it is what the
	// upstream receives as X-Tollgate-Client-ID and what the access log
	// shows, and a request refused by the rate limit carries neither.
	if route.authn != nil {
		st.clientID = identity
	}
	return true
}

// setRateLimitHeaders sets X-RateLimit-Limit (the burst) and
// X-RateLimit-Remaining (whole tokens left) from d.
func setRateLimitHeaders(h http.Header, d ratelimit.Decision) {
	h.Set(headerRateLimitLimit, strconv.Itoa(d.Limit))
	h.Set(headerRateLimitRemaining, strconv.Itoa(d.Remaining))
}

// writeTooManyRequests answers 429 with a Retry-After for retryAfter.
func writeTooManyRequests(w http.ResponseWriter, retryAfter time.Duration, reqID string) {
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds(retryAfter), 10))
	writeError(w, http.StatusTooManyRequests, reqID)
}

// retryAfterSeconds rounds d UP to whole seconds, minimum 1 (ADR 0004):
// rounding down would send an obedient client back too early for a second
// 429, and "0" would invite an immediate retry. Integer arithmetic, so an
// exact 1s stays 1 and a saturated d cannot overflow.
func retryAfterSeconds(d time.Duration) int64 {
	secs := int64(d / time.Second)
	if d%time.Second > 0 {
		secs++
	}
	return max(secs, 1)
}
