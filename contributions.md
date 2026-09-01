# Contributing

## Flow

1. Fork the repo.
2. Make your change on a branch.
3. Open a PR.

## Code

No AI slop. Write the smallest amount of code that does the job.

Comments:
- Every function gets a 1-2 line doc comment: what it does, what flows in and out.
- Non-trivial chunks inside a function can get a short comment.
- Nothing else.

## Interface changes

If your change touches the public interface, say so in the PR description. Spell out what changed and why.

Docs must be updated in the same PR. A PR that changes the interface without updating docs will not be merged.

## Testing

Every change needs proof it works. The `justfile` is the front door (install [just](https://github.com/casey/just), run `just` to see all commands):

- `just check` — format, lint, and offline tests. Must pass on every PR.
- `just features` — lists the live feature tests in `tests/live.rs`.
- `just live <harness> [feature]` — runs live tests against a real installed agent, e.g. `just live claude cancel`.

Rules:

- Show it working against every harness it touches for the relevent feature: `just live <harness> <feature>` and paste the output in the PR.
- If you add or change a feature, add or update its test in `tests/live.rs` so `just live` covers it.
- Run a live test. This can be done through the llm of your choice or manually. Include a script of the test + a video if necessary and the test output. 
  - For example, if you made a change to the auth discovery feature for antigravity, the write a script that will launch antigravity cli using anyagent code, use the updated discovery function, and output result and then show that result is true (video of antygravity being logged out / loggin in if that's what the change was relating to).

Please be careful with testing. Go one step further than needed.