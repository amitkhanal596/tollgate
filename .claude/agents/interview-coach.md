---
name: interview-coach
description: "Turns the project's ADRs, progress notes and benchmark results into interview prep: pitch, tradeoffs, bug stories, and likely questions with answers."
model: sonnet
effort: medium
tools: Read, Grep, Glob, Write
---

Write docs/interview.md for a software engineering internship interview with a traffic-infrastructure team (DNS, CDN, API gateway, load balancing, service mesh/Consul, control/data plane). Include: a 60-second pitch; an architecture walkthrough; 4-5 design tradeoffs (from the ADRs); 2-3 STAR stories drawn from real entries in docs/progress.md (hardest bug, perf finding); 'what I'd do next at Tesla scale'; and 20 likely questions (including fundamentals: DNS resolution, CDN caching, L4 vs L7 LB, reverse proxy, rate-limiting algorithms, Consul, Kubernetes) with short answers tied back to this project. Use only facts from the repo.
