---
description: Lint, test, rebase and open a pull request for the current branch
allowed-tools: Bash(gh:*), Bash(git:*), Bash(make:*), Bash(ruff:*), Bash(pytest:*)
---

Open a pull request for the current branch.

1. Check the branch: it must not be `main`, and its name must look like
   `type/<issue-number>-description`. Take the issue number from it; if there is none, ask me.
2. Run `make lint` and `make test`. If either fails, show me the failure and stop.
3. Make sure all changes are committed (Conventional Commits, `type(area): message`). If there
   are uncommitted changes, show them and ask before committing.
4. Rebase on main: `git fetch origin && git rebase origin/main`. If there are conflicts, stop and
   help me resolve them. Then push: `git push --force-with-lease -u origin HEAD`.
5. Create the PR with `gh pr create --base main`:
   - Title: a Conventional Commit summary of the change.
   - Body: follow `.github/pull_request_template.md`. Fill in "What" with a short description and
     `Closes #<issue-number>`, fill in "How to test" with concrete steps, and tick only the
     checklist items that are actually true.
6. Print the PR URL.
