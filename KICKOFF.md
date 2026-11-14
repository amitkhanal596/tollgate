# Kickoff

1. Unzip into an empty folder, then `git init`.
2. Install Go 1.23+, Docker, kind and k6.
3. Start the orchestrator on Fable 5.2:
   ```
   claude --model fable
   ```
4. Paste this prompt (or run it with `/goal`):

```
You are the orchestrator for Tollgate. Read CLAUDE.md and PLAN.md first and follow them exactly.

Execute PLAN.md phase by phase, starting at Phase 0. For each phase, run the loop in CLAUDE.md:
break it into tasks, delegate to the subagents in .claude/agents per the routing table (run
independent tasks in parallel), verify with `go vet ./... && go test -race ./...` yourself, get a
reviewer pass, fix CONFIRMED findings, commit, and append to docs/progress.md.

Stop at each owner checkpoint (after Phases 0, 2, 4 and 8), write docs/checkpoints.md, and wait
for me before continuing.

Done = everything in PLAN.md's "Definition of done", plus docs/interview.md from interview-coach.
```

Check the routing anytime with `/agents`.
