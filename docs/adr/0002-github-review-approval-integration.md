# Integrate GitHub review approval into the PR Approval Queue

The Control Tower PR Approval Queue will support GitHub-native pull request review approval, not only internal autodev release approval. This adds authorization and reviewer-identity complexity, but it keeps the one-stop development flow from breaking at the PR review boundary and lets the release worker proceed against the same approval signal that GitHub branch protection expects.
