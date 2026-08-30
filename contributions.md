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

Every change needs proof it works.

- Show it working against every harness it touches for the relevent feature.
- Run a live test. This can be done through the llm of your choice or manually. Include a script of the test + a video if necessary and the test output. 
  - For example, if you made a change to the auth discovery feature for antigravity, the write a script that will launch antigravity cli using anyagent code, use the updated discovery function, and output result and then show that result is true (video of antygravity being logged out / loggin in if that's what the change was relating to).

Please be careful with testing. Go one step further than needed.