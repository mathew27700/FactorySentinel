---
description: Start work on a GitHub issue (sync main, create branch, self-assign)
argument-hint: <issue-number>
allowed-tools: Bash(gh:*), Bash(git:*)
---

Start work on issue #$ARGUMENTS in this repository.

1. Read the issue: `gh issue view $ARGUMENTS --json number,title,body,labels,state`.
   If it does not exist or is closed, stop and tell me.
2. Make sure the working tree is clean (`git status --porcelain`). If it is not, stop and ask me
   what to do with the changes. Then sync main:
   `git fetch origin && git checkout main && git pull --ff-only origin main`.
3. Create the branch `type/$ARGUMENTS-short-description`:
   - `type` comes from the labels: `type:bug` → `fix`, `type:chore` → `chore`, `area:docs` with
     no other type → `docs`, otherwise `feat`.
   - `short-description` is 2–5 lowercase, hyphenated words from the issue title.
   - Example: `feat/12-anomaly-detection`. If the branch already exists, check it out instead.
4. Assign the issue to me: `gh issue edit $ARGUMENTS --add-assignee @me`.
5. Summarise the issue in a few lines: the goal, then each item of its "Done when" checklist,
   and which folders it is likely to touch. Do not start writing code until I ask.
