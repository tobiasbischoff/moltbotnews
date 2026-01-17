# Clawdbot Daily

Lobster-grade daily summaries of the `clawdbot/clawdbot` repository. 🦞

## What it does
- Pulls commits, issues, and pull requests from GitHub and groups them by day.
- Renders a fast, clean daily log with a “commit summary” activity feed.
- Uses MiniMax (Anthropic-compatible) to generate AI summaries per day.

## Local setup
```bash
npm install
cp .env.example .env
npm run dev
```

Then open `http://localhost:3000`.

## Environment variables
- `ANTHROPIC_BASE_URL` (required for AI): `https://api.minimax.io/anthropic` (global) or `https://api.minimaxi.com/anthropic` (China).
- `ANTHROPIC_API_KEY` (required for AI): your MiniMax key.
- `ANTHROPIC_MODEL` (optional): defaults to `MiniMax-M2.1`.
- `GITHUB_REPO` (optional): defaults to `clawdbot/clawdbot`.
- `GITHUB_BRANCH` (optional): defaults to `main`.
- `GITHUB_TOKEN` (optional): increases GitHub rate limits.
- `CACHE_DB_PATH` (optional): SQLite location (default `./data/clawdbotnews.sqlite`).
- `SYNC_INTERVAL_MINUTES` (optional): how often GitHub is polled (default `10`).
- `MAX_COMMITS_PER_PROMPT` (optional): commit threshold before chunking (default `2000`).
- `CHUNK_SIZE` (optional): commits per chunk when chunking is needed (default `200`).
- `SUMMARY_MAX_TOKENS` (optional): AI summary output cap (default `1200`).
- `DB_PERSIST_DEBOUNCE_MS` (optional): debounce for SQLite export-to-disk (default `1000`).

## Notes
- Without `ANTHROPIC_API_KEY`, the UI still renders commit history but AI summaries are disabled.
- GitHub API rate limits apply. Add `GITHUB_TOKEN` for higher limits.
- Commit data, issues/PRs, and AI summaries are cached in SQLite for faster reloads.
- SQLite runs via `sql.js` (WASM), so there is no native build step.
- New items are synced incrementally; set `SYNC_INTERVAL_MINUTES` to control how often GitHub is polled.

## Debugging summaries
Use `/api/summary?date=YYYY-MM-DD` to fetch the cached prompt + model response for a given day.
