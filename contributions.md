# Contributing

The full guide lives in the docs: **[anyagent.mintlify.site/contributing](https://anyagent.mintlify.site/contributing)** (source: `docs/contributing.mdx`).

Short version:

1. Open an issue first. Once approved, fork, branch, and open a PR.
2. `just check` must pass. Wire or adapter changes need `just live <harness> <feature>` output in the PR.
3. Interface changes update the docs in the same PR, or they will not be merged.
4. Smallest code that does the job. 1-2 line doc comments per function, nothing more.
