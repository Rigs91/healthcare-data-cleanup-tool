# 50-Feature Rollout

This folder tracks phased implementation for the 50-feature roadmap and supports per-feature rollback.

## Artifacts

- `features.json`: canonical feature registry (`F01`..`F50`) with wave and status.
- `signoffs/`: feature-level signoff files (`Fxx.md`).

## Rollback Policy

For each feature:

1. Create tag before work: `pre-fxx`.
2. Implement + validate.
3. Sign off in `signoffs/Fxx.md`.
4. Create tag after work: `post-fxx`.

Rollback target is always `pre-fxx`.

## Expected Status Values

- `planned`
- `in_progress`
- `implemented`
- `validated`
- `rolled_back`

