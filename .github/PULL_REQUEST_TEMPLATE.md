<!--
PR TITLE: write a short, meaningful summary of the change as a whole, in the
imperative mood - don't leave the auto-filled first-commit message. Examples:
  * Fix federation transaction race on retry
  * Add sliding sync v3 endpoint
  * Improve E2EE key claim performance
Individual commits should still follow Conventional Commits.
-->

## Type of change

<!-- Please check all that apply - this drives the PR's type labels. -->

- [ ] **Bug fix** (fixes an issue in client-server or federation behaviour)
- [ ] **Feature** (adds new Matrix API capability)
- [ ] **Enhancement** (improves client-server/federation/sync behaviour)
- [ ] **Refactor** (restructures code without changing behaviour)
- [ ] **Breaking change** (changes API behaviour or D1 schema)
- [ ] **Security** (security-related fix or hardening)

## Description

### Why is this change needed?

<!-- Explain the motivation and context for this change -->

### Related Issues

<!-- Link to related issues using "Fixes #123", "Closes #123", or "Relates to #123" -->

## Testing

<!-- Describe how you tested your changes -->

- [ ] `npm run typecheck`
- [ ] `npx wrangler deploy --dry-run`

## Checklist

<!-- Ensure all applicable items are completed before requesting review -->

- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Type check and dry-run deploy pass
- [ ] Documentation updated (if applicable)
