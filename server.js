import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import Anthropic from "@anthropic-ai/sdk";
import { config } from "dotenv";
import fs from "fs";
import initSqlJs from "sql.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

config({ path: path.join(__dirname, ".env") });

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;
const REPO = process.env.GITHUB_REPO || "clawdbot/clawdbot";
const BRANCH = process.env.GITHUB_BRANCH || "main";
const GH_BASE = `https://api.github.com/repos/${REPO}`;
const SYNC_INTERVAL_MINUTES = Number(process.env.SYNC_INTERVAL_MINUTES || 10);
const CACHE_DB_PATH =
  process.env.CACHE_DB_PATH ||
  path.join(__dirname, "data", "clawdbotnews.sqlite");
const CACHE_SCHEMA_VERSION = "2";

const AI_MODEL = process.env.ANTHROPIC_MODEL || "MiniMax-M2.1";
const AI_BASE_URL = process.env.ANTHROPIC_BASE_URL;
const AI_KEY = process.env.ANTHROPIC_API_KEY;
const MAX_COMMITS_PER_PROMPT = Number(
  process.env.MAX_COMMITS_PER_PROMPT || 2000
);
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 200);
const SUMMARY_MAX_TOKENS = Number(process.env.SUMMARY_MAX_TOKENS || 1200);

const summarizer = AI_KEY
  ? new Anthropic({
      apiKey: AI_KEY,
      ...(AI_BASE_URL ? { baseURL: AI_BASE_URL } : {}),
    })
  : null;

if (!AI_KEY) {
  console.warn("ANTHROPIC_API_KEY is missing; AI summaries are disabled.");
} else if (!AI_BASE_URL) {
  console.warn("ANTHROPIC_BASE_URL is missing; using Anthropic default URL.");
}

const dataDir = path.dirname(CACHE_DB_PATH);
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const SQL = await initSqlJs({
  locateFile: (file) => path.join(__dirname, "node_modules", "sql.js", "dist", file),
});

const db = fs.existsSync(CACHE_DB_PATH)
  ? new SQL.Database(fs.readFileSync(CACHE_DB_PATH))
  : new SQL.Database();

db.run(`
  CREATE TABLE IF NOT EXISTS commits (
    sha TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    branch TEXT NOT NULL,
    date TEXT NOT NULL,
    data TEXT NOT NULL,
    fetched_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS commits_repo_branch_date
    ON commits (repo, branch, date DESC);
  CREATE TABLE IF NOT EXISTS issues (
    id INTEGER PRIMARY KEY,
    repo TEXT NOT NULL,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state TEXT NOT NULL,
    user_login TEXT,
    is_pr INTEGER NOT NULL,
    data TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS issues_repo_created
    ON issues (repo, created_at DESC);
  CREATE INDEX IF NOT EXISTS issues_repo_ispr_created
    ON issues (repo, is_pr, created_at DESC);
  CREATE TABLE IF NOT EXISTS daily_summaries (
    repo TEXT NOT NULL,
    date TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    summary_json TEXT,
    raw_text TEXT,
    prompt_text TEXT,
    response_text TEXT,
    response_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (repo, date)
  );
  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

let saveTimer = null;
function persistDb() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const data = db.export();
    fs.writeFileSync(CACHE_DB_PATH, Buffer.from(data));
  }, 150);
}

function ensureSummaryColumns() {
  const columns = dbAll("PRAGMA table_info(daily_summaries)").map(
    (col) => col.name
  );
  const additions = [
    ["prompt_text", "TEXT"],
    ["response_text", "TEXT"],
    ["response_json", "TEXT"],
  ];
  for (const [name, type] of additions) {
    if (!columns.includes(name)) {
      dbRun(`ALTER TABLE daily_summaries ADD COLUMN ${name} ${type}`);
    }
  }
  if (additions.some(([name]) => !columns.includes(name))) {
    persistDb();
  }
}

ensureSummaryColumns();

function ensureCacheVersion() {
  const version = getMeta("cache_schema_version");
  if (!version || version !== CACHE_SCHEMA_VERSION) {
    setMeta("cache_schema_version", CACHE_SCHEMA_VERSION);
  }
}

ensureCacheVersion();

function dbRun(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.run(params);
  stmt.free();
}

function dbGet(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const row = stmt.step() ? stmt.getAsObject() : null;
  stmt.free();
  return row;
}

function dbAll(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) {
    rows.push(stmt.getAsObject());
  }
  stmt.free();
  return rows;
}

function getMeta(key) {
  const row = dbGet("SELECT value FROM meta WHERE key = ?", [key]);
  return row?.value || null;
}

function setMeta(key, value) {
  dbRun(
    `
    INSERT INTO meta (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `,
    [key, value]
  );
  persistDb();
}

function ghHeaders() {
  const headers = {
    Accept: "application/vnd.github+json",
    "User-Agent": "clawdbotnews-dashboard",
  };
  if (process.env.GITHUB_TOKEN) {
    headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  }
  return headers;
}

async function ghFetch(url) {
  const response = await fetch(url, { headers: ghHeaders() });
  if (!response.ok) {
    const text = await response.text();
    const error = new Error(`GitHub API error ${response.status}`);
    error.status = response.status;
    error.details = text;
    throw error;
  }
  return response.json();
}

function classifyCommit(message) {
  const text = message.toLowerCase();
  if (text.startsWith("fix") || text.includes("bug") || text.includes("hotfix")) {
    return "fix";
  }
  if (text.startsWith("feat") || text.includes("feature") || text.includes("add ")) {
    return "feature";
  }
  if (text.startsWith("refactor") || text.includes("cleanup")) {
    return "refactor";
  }
  if (text.startsWith("docs") || text.includes("readme")) {
    return "docs";
  }
  if (text.startsWith("test") || text.includes("spec")) {
    return "test";
  }
  if (
    text.startsWith("chore") ||
    text.startsWith("build") ||
    text.includes("deps") ||
    text.includes("bump")
  ) {
    return "chore";
  }
  return "other";
}

function mapCommitDetail(detail) {
  const message = detail.commit?.message || "";
  const title = message.split("\n")[0] || "(no message)";
  const date =
    detail.commit?.author?.date || detail.commit?.committer?.date || null;
  const files = Array.isArray(detail.files)
    ? detail.files.map((file) => ({
        filename: file.filename,
        status: file.status,
        additions: file.additions,
        deletions: file.deletions,
        changes: file.changes,
      }))
    : [];
  return {
    sha: detail.sha,
    shortSha: detail.sha?.slice(0, 7),
    title,
    message,
    author:
      detail.commit?.author?.name ||
      detail.commit?.committer?.name ||
      detail.author?.login ||
      "Unknown",
    date,
    url: detail.html_url,
    stats: detail.stats || { additions: 0, deletions: 0, total: 0 },
    files,
    type: classifyCommit(title),
  };
}

function mapIssue(item) {
  return {
    id: item.id,
    number: item.number,
    title: item.title,
    url: item.html_url,
    created_at: item.created_at,
    updated_at: item.updated_at,
    state: item.state,
    user_login: item.user?.login || "unknown",
    is_pr: item.pull_request ? 1 : 0,
  };
}

function groupByDate(commits) {
  const grouped = new Map();
  for (const commit of commits) {
    if (!commit.date) continue;
    const key = commit.date.split("T")[0];
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(commit);
  }
  return Array.from(grouped.entries())
    .map(([date, entries]) => ({
      date,
      commits: entries.sort((a, b) => b.date.localeCompare(a.date)),
    }))
    .sort((a, b) => b.date.localeCompare(a.date));
}

function groupByDateKey(items, dateKey) {
  const grouped = new Map();
  for (const item of items) {
    const value = item[dateKey];
    if (!value) continue;
    const key = value.split("T")[0];
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  }
  return grouped;
}

function normalizeCommitsForPrompt(commits) {
  return commits.map((commit) => ({
    sha: commit.shortSha,
    title: commit.title,
    type: commit.type,
    additions: commit.stats?.additions ?? 0,
    deletions: commit.stats?.deletions ?? 0,
    files: commit.files?.slice(0, 8).map((file) => file.filename) ?? [],
  }));
}

function formatCommitLines(commits, { includeFiles = true } = {}) {
  return normalizeCommitsForPrompt(commits)
    .map((commit, index) => {
      const fileText =
        includeFiles && commit.files.length
          ? ` | files: ${commit.files.join(", ")}`
          : "";
      return `${index + 1}. [${commit.sha}] ${commit.title} | type: ${
        commit.type
      } | +${commit.additions} -${commit.deletions}${fileText}`;
    })
    .join("\n");
}

function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function extractJson(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) return null;
  const snippet = text.slice(start, end + 1);
  try {
    return JSON.parse(snippet);
  } catch {
    return null;
  }
}

function extractJsonFromCodeFence(text) {
  const match = text.match(/```json\\s*([\\s\\S]*?)```/i);
  if (!match) return null;
  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
}

function buildTypeBreakdown(commits) {
  const breakdown = {
    feature: 0,
    fix: 0,
    refactor: 0,
    docs: 0,
    test: 0,
    chore: 0,
    other: 0,
  };
  for (const commit of commits) {
    const type = commit.type || "other";
    breakdown[type] = (breakdown[type] || 0) + 1;
  }
  return breakdown;
}

function extractTypeBreakdownFromText(text) {
  const breakdown = {
    feature: 0,
    fix: 0,
    refactor: 0,
    docs: 0,
    test: 0,
    chore: 0,
    other: 0,
  };
  const regex =
    /^(feature|fix|refactor|docs|test|chore|other)\s*:\s*(\d+)/gim;
  let match = null;
  while ((match = regex.exec(text))) {
    breakdown[match[1].toLowerCase()] = Number(match[2]);
  }
  return breakdown;
}

function extractSummaryLine(text) {
  const summaryMatch = text.match(/"summary"\s*:\s*"([^"]+)"/i);
  if (summaryMatch?.[1]) return summaryMatch[1].trim();
  const cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();
  const firstLine = cleaned.split("\n").find((line) => line.trim().length);
  return firstLine ? firstLine.trim() : "";
}

function buildFallbackSummary(commits) {
  const breakdown = buildTypeBreakdown(commits);
  const total = commits.length;
  const themes = new Map();
  const themeMatchers = [
    { key: "discord", label: "Discord integration" },
    { key: "telegram", label: "Telegram command handling" },
    { key: "plugin", label: "Plugin architecture" },
    { key: "dm", label: "Direct message history limits" },
    { key: "changelog", label: "Changelog updates" },
    { key: "docs", label: "Documentation updates" },
    { key: "test", label: "Testing coverage" },
  ];

  for (const commit of commits) {
    const title = (commit.title || "").toLowerCase();
    for (const matcher of themeMatchers) {
      if (title.includes(matcher.key)) {
        themes.set(matcher.label, (themes.get(matcher.label) || 0) + 1);
      }
    }
  }

  const topThemes = Array.from(themes.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 2)
    .map(([label]) => label);

  const summaryThemes =
    topThemes.length > 0 ? topThemes.join(" and ") : "core platform changes";
  const summary = `Focused on ${summaryThemes} with ${total} commits (${breakdown.feature} features, ${breakdown.fix} fixes, ${breakdown.refactor} refactors).`;

  const highlights = commits
    .filter(
      (commit) =>
        commit.title &&
        !commit.title.toLowerCase().startsWith("merge pull request") &&
        !commit.title.toLowerCase().startsWith("merge branch")
    )
    .slice(0, 3)
    .map((commit) => commit.title);

  return {
    summary,
    highlights,
    type_breakdown: breakdown,
    risks: [],
  };
}

function buildThemeHighlights(commits, maxItems = 6) {
  const themes = [
    { key: "discord", label: "Discord integration" },
    { key: "telegram", label: "Telegram integration" },
    { key: "plugin", label: "Plugin architecture" },
    { key: "dm", label: "Direct message history limits" },
    { key: "docs", label: "Documentation updates" },
    { key: "test", label: "Testing coverage" },
    { key: "config", label: "Configuration management" },
    { key: "release", label: "Release tooling" },
    { key: "ci", label: "Build and CI" },
    { key: "cli", label: "CLI tooling" },
  ];

  const counts = new Map();
  for (const commit of commits) {
    const title = (commit.title || "").toLowerCase();
    for (const theme of themes) {
      if (title.includes(theme.key)) {
        counts.set(theme.label, (counts.get(theme.label) || 0) + 1);
      }
    }
  }

  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, maxItems)
    .map(([label, count]) => `${label} updates (${count} commits).`);
}

function parseAiResponse(rawText, commits) {
  let parsed = extractJson(rawText);
  if (!parsed) {
    parsed = extractJsonFromCodeFence(rawText);
  }

  const fallbackBreakdown = buildTypeBreakdown(commits);
  const parsedBreakdownFromText = extractTypeBreakdownFromText(rawText);
  const summaryLine = extractSummaryLine(rawText);
  if (!parsed && rawText) {
    parsed = {
      summary: summaryLine || rawText,
      highlights: [],
      type_breakdown:
        Object.values(parsedBreakdownFromText).some((value) => value > 0)
          ? parsedBreakdownFromText
          : fallbackBreakdown,
      risks: [],
    };
  } else if (!parsed) {
    parsed = buildFallbackSummary(commits);
  } else if (parsed) {
    parsed.summary =
      parsed.summary || summaryLine || rawText || "Summary unavailable.";
    parsed.highlights = Array.isArray(parsed.highlights)
      ? parsed.highlights
      : [];
    parsed.type_breakdown =
      parsed.type_breakdown && typeof parsed.type_breakdown === "object"
        ? parsed.type_breakdown
        : Object.values(parsedBreakdownFromText).some((value) => value > 0)
        ? parsedBreakdownFromText
        : fallbackBreakdown;
    parsed.risks = Array.isArray(parsed.risks) ? parsed.risks : [];
  }
  return parsed;
}

function loadCachedCommits({ sinceIso }) {
  const rows = dbAll(
    `
    SELECT data FROM commits
    WHERE repo = ? AND branch = ? AND date >= ?
    ORDER BY date DESC
  `,
    [REPO, BRANCH, sinceIso]
  );
  return rows.map((row) => JSON.parse(row.data));
}

function hasCommitsSince(sinceIso) {
  const row = dbGet(
    `
    SELECT COUNT(*) as count
    FROM commits
    WHERE repo = ? AND branch = ? AND date >= ?
  `,
    [REPO, BRANCH, sinceIso]
  );
  return Number(row?.count || 0) > 0;
}

function getLatestCommitDate() {
  const row = dbGet(
    `
    SELECT MAX(date) as latest
    FROM commits
    WHERE repo = ? AND branch = ?
  `,
    [REPO, BRANCH]
  );
  return row?.latest || null;
}

function cacheCommits(commits) {
  const fetchedAt = new Date().toISOString();
  dbRun("BEGIN");
  for (const commit of commits) {
    dbRun(
      `
      INSERT INTO commits (sha, repo, branch, date, data, fetched_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(sha) DO UPDATE SET
        repo = excluded.repo,
        branch = excluded.branch,
        date = excluded.date,
        data = excluded.data,
        fetched_at = excluded.fetched_at
    `,
      [
        commit.sha,
        REPO,
        BRANCH,
        commit.date || fetchedAt,
        JSON.stringify(commit),
        fetchedAt,
      ]
    );
  }
  dbRun("COMMIT");
  persistDb();
}

function cacheIssues(items) {
  dbRun("BEGIN");
  for (const issue of items) {
    dbRun(
      `
      INSERT INTO issues (
        id, repo, number, title, url, created_at, updated_at, state, user_login, is_pr, data
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        repo = excluded.repo,
        number = excluded.number,
        title = excluded.title,
        url = excluded.url,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at,
        state = excluded.state,
        user_login = excluded.user_login,
        is_pr = excluded.is_pr,
        data = excluded.data
    `,
      [
        issue.id,
        REPO,
        issue.number,
        issue.title,
        issue.url,
        issue.created_at,
        issue.updated_at,
        issue.state,
        issue.user_login,
        issue.is_pr,
        JSON.stringify(issue),
      ]
    );
  }
  dbRun("COMMIT");
  persistDb();
}

function loadIssues({ sinceIso, isPr }) {
  const rows = dbAll(
    `
    SELECT data FROM issues
    WHERE repo = ? AND created_at >= ? AND is_pr = ?
    ORDER BY created_at DESC
  `,
    [REPO, sinceIso, isPr ? 1 : 0]
  );
  return rows.map((row) => JSON.parse(row.data));
}

function hasIssuesSince(sinceIso, isPr) {
  const row = dbGet(
    `
    SELECT COUNT(*) as count
    FROM issues
    WHERE repo = ? AND created_at >= ? AND is_pr = ?
  `,
    [REPO, sinceIso, isPr ? 1 : 0]
  );
  return Number(row?.count || 0) > 0;
}

function shouldSync({ sinceIso, force }) {
  if (force) return true;
  const version = getMeta("cache_schema_version");
  if (!version || version !== CACHE_SCHEMA_VERSION) return true;
  if (!hasCommitsSince(sinceIso)) return true;
  const lastSync = getMeta("last_sync_at");
  if (!lastSync) return true;
  const ageMs = Date.now() - new Date(lastSync).getTime();
  return ageMs > SYNC_INTERVAL_MINUTES * 60 * 1000;
}

async function syncCommits({ sinceIso, force }) {
  if (!shouldSync({ sinceIso, force })) {
    return { synced: false, source: "sqlite", lastSyncAt: getMeta("last_sync_at") };
  }

  const overlapMs = 5 * 60 * 1000;
  const latest = getLatestCommitDate();
  const sinceMs = new Date(sinceIso).getTime();
  const fetchSince = latest
    ? new Date(Math.max(new Date(latest).getTime() - overlapMs, sinceMs)).toISOString()
    : sinceIso;

  const detailed = [];
  let page = 1;
  const perPage = 100;

  while (true) {
    const listUrl = new URL(`${GH_BASE}/commits`);
    listUrl.searchParams.set("sha", BRANCH);
    listUrl.searchParams.set("per_page", String(perPage));
    listUrl.searchParams.set("since", fetchSince);
    listUrl.searchParams.set("page", String(page));

    const list = await ghFetch(listUrl.toString());
    if (!Array.isArray(list) || list.length === 0) break;

    for (const item of list) {
      const detail = await ghFetch(`${GH_BASE}/commits/${item.sha}`);
      detailed.push(mapCommitDetail(detail));
    }

    if (list.length < perPage) break;
    page += 1;
  }

  if (detailed.length) {
    cacheCommits(detailed);
  }

  const now = new Date().toISOString();
  setMeta("last_sync_at", now);
  return {
    synced: true,
    source: "github",
    lastSyncAt: now,
    fetchSince,
    fetchedCount: detailed.length,
  };
}

function shouldSyncIssues({ sinceIso, force }) {
  if (force) return true;
  const version = getMeta("cache_schema_version");
  if (!version || version !== CACHE_SCHEMA_VERSION) return true;
  if (!hasIssuesSince(sinceIso, false) && !hasIssuesSince(sinceIso, true)) {
    return true;
  }
  const lastSync = getMeta("last_issues_sync_at");
  if (!lastSync) return true;
  const ageMs = Date.now() - new Date(lastSync).getTime();
  return ageMs > SYNC_INTERVAL_MINUTES * 60 * 1000;
}

async function syncIssuesAndPRs({ sinceIso, force }) {
  if (!shouldSyncIssues({ sinceIso, force })) {
    return { synced: false, source: "sqlite", lastSyncAt: getMeta("last_issues_sync_at") };
  }

  const overlapMs = 5 * 60 * 1000;
  const lastSync = getMeta("last_issues_sync_at");
  const sinceMs = new Date(sinceIso).getTime();
  const fetchSince = lastSync
    ? new Date(Math.max(new Date(lastSync).getTime() - overlapMs, sinceMs)).toISOString()
    : sinceIso;

  const fetched = [];
  let page = 1;
  const perPage = 100;

  while (true) {
    const listUrl = new URL(`${GH_BASE}/issues`);
    listUrl.searchParams.set("state", "all");
    listUrl.searchParams.set("per_page", String(perPage));
    listUrl.searchParams.set("since", fetchSince);
    listUrl.searchParams.set("page", String(page));

    const list = await ghFetch(listUrl.toString());
    if (!Array.isArray(list) || list.length === 0) break;

    for (const item of list) {
      fetched.push(mapIssue(item));
    }

    if (list.length < perPage) break;
    page += 1;
  }

  if (fetched.length) {
    cacheIssues(fetched);
  }

  const now = new Date().toISOString();
  setMeta("last_issues_sync_at", now);
  return {
    synced: true,
    source: "github",
    lastSyncAt: now,
    fetchSince,
    fetchedCount: fetched.length,
  };
}

app.get("/api/commits", async (req, res) => {
  try {
    const days = Number(req.query.days || 7);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const sinceIso = since.toISOString();
    const cachedBefore = loadCachedCommits({ sinceIso });
    let sync = null;
    try {
      sync = await syncCommits({ sinceIso, force: req.query.force === "1" });
    } catch (error) {
      if (cachedBefore.length === 0) {
        throw error;
      }
      sync = {
        source: "sqlite-stale",
        synced: false,
        lastSyncAt: getMeta("last_sync_at"),
        error: error.message,
      };
    }
    let issueSync = null;
    try {
      issueSync = await syncIssuesAndPRs({
        sinceIso,
        force: req.query.force === "1",
      });
    } catch (error) {
      issueSync = {
        source: "sqlite-stale",
        synced: false,
        lastSyncAt: getMeta("last_issues_sync_at"),
        error: error.message,
      };
    }
    const responseCommits = loadCachedCommits({ sinceIso });
    const issues = loadIssues({ sinceIso, isPr: false });
    const pullRequests = loadIssues({ sinceIso, isPr: true });
    const issuesByDate = groupByDateKey(issues, "created_at");
    const prsByDate = groupByDateKey(pullRequests, "created_at");
    const daysWithExtras = groupByDate(responseCommits).map((day) => ({
      ...day,
      issues: issuesByDate.get(day.date) || [],
      pullRequests: prsByDate.get(day.date) || [],
    }));
    res.json({
      repo: REPO,
      branch: BRANCH,
      since: sinceIso,
      aiAvailable: Boolean(summarizer),
      cache: {
        source: sync.source,
        synced: sync.synced,
        lastSyncAt: sync.lastSyncAt || getMeta("last_sync_at"),
        fetchSince: sync.fetchSince || null,
        fetchedCount: sync.fetchedCount || 0,
        error: sync.error || null,
        issues: {
          source: issueSync.source,
          synced: issueSync.synced,
          lastSyncAt: issueSync.lastSyncAt || getMeta("last_issues_sync_at"),
          fetchSince: issueSync.fetchSince || null,
          fetchedCount: issueSync.fetchedCount || 0,
          error: issueSync.error || null,
        },
      },
      days: daysWithExtras,
    });
  } catch (error) {
    res.status(error.status || 500).json({
      error: "Failed to load commits",
      details: error.details || error.message,
    });
  }
});

app.post("/api/summarize", async (req, res) => {
  const { date, commits, force, debug } = req.body || {};
  if (!Array.isArray(commits) || commits.length === 0) {
    return res.status(400).json({ error: "No commits provided" });
  }
  const fingerprint = commits.map((commit) => commit.sha).join("|");
  if (!force) {
    const cached = dbGet(
      `
      SELECT fingerprint, summary_json, raw_text, prompt_text, response_text, response_json, updated_at
      FROM daily_summaries
      WHERE repo = ? AND date = ?
    `,
      [REPO, date]
    );
    if (cached && cached.fingerprint === fingerprint) {
      const payload = {
        summary: cached.summary_json ? JSON.parse(cached.summary_json) : null,
        raw: cached.raw_text || "",
        cache: { source: "sqlite" },
      };
      if (debug) {
        payload.debug = {
          prompt: cached.prompt_text || "",
          responseText: cached.response_text || "",
          responseJson: cached.response_json
            ? JSON.parse(cached.response_json)
            : null,
          updatedAt: cached.updated_at,
        };
      }
      return res.json(payload);
    }
  }

  if (!summarizer) {
    return res.status(503).json({
      error: "AI summary not configured",
      details: "Set ANTHROPIC_API_KEY and ANTHROPIC_BASE_URL",
    });
  }

  const systemPrompt =
    "You are a lobster keeping a clear, friendly daily change log. " +
    "Summarize changes in plain language without corporate tone. " +
    "Classify work as feature, fix, refactor, docs, test, or chore. " +
    "Be concise, factual, and avoid speculation.";

  const summaryShape = `{\n  \"summary\": \"1-2 sentence executive summary\",\n  \"highlights\": [\"bullet\", \"bullet\"],\n  \"type_breakdown\": { \"feature\": 0, \"fix\": 0, \"refactor\": 0, \"docs\": 0, \"test\": 0, \"chore\": 0, \"other\": 0 },\n  \"risks\": [\"short risk if any\", \"or empty array if none\"]\n}`;

  try {
    const attempts = [];
    const callModel = async ({ overrideSystem, prompt, maxTokens }) => {
      const response = await summarizer.messages.create({
        model: AI_MODEL,
        max_tokens: maxTokens ?? SUMMARY_MAX_TOKENS,
        temperature: 0.1,
        thinking: { type: "disabled" },
        system: overrideSystem || systemPrompt,
        messages: [
          {
            role: "user",
            content: [{ type: "text", text: prompt }],
          },
        ],
      });
      attempts.push({
        id: response.id,
        model: response.model,
        content: response.content,
        stop_reason: response.stop_reason,
        usage: response.usage,
        system: overrideSystem || systemPrompt,
      });
      return response;
    };

    const makePrompt = (commitLines, extraContext = "") => `
Date: ${date}
Repository: ${REPO}
${extraContext}
Commits (newest first):
${commitLines}

Guidelines:
- Provide 6-10 highlights describing distinct themes (not individual commits).
- Avoid listing raw commit titles.

Return JSON only with this shape:
${summaryShape}
Return a single valid JSON object only. Do not wrap it in markdown code fences.
`;

    const chunks =
      commits.length > MAX_COMMITS_PER_PROMPT
        ? chunkArray(commits, CHUNK_SIZE)
        : [commits];
    const chunkSummaries = [];
    const chunkResponses = [];
    let finalPrompt = "";
    let rawText = "";
    let parsed = null;

    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      const extraContext =
        chunks.length > 1
          ? `Chunk ${index + 1} of ${chunks.length}.`
          : "";
      const commitLines = formatCommitLines(chunk, { includeFiles: false });
      const chunkPrompt = makePrompt(commitLines, extraContext);
      let response = await callModel({
        prompt: chunkPrompt,
        maxTokens: Math.min(SUMMARY_MAX_TOKENS, 800),
      });

      let chunkText = response.content
        ?.filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("\n")
        .trim();

      if (!chunkText) {
        const retrySystem =
          systemPrompt +
          " Return ONLY a valid JSON object. Do not include analysis or reasoning.";
        response = await callModel({
          overrideSystem: retrySystem,
          prompt: chunkPrompt,
          maxTokens: Math.min(SUMMARY_MAX_TOKENS, 800),
        });
        chunkText = response.content
          ?.filter((block) => block.type === "text")
          .map((block) => block.text)
          .join("\n")
          .trim();
      }

      const parsedChunk = parseAiResponse(chunkText || "", chunk);
      chunkSummaries.push(parsedChunk);
      chunkResponses.push({ prompt: chunkPrompt, rawText: chunkText || "" });
    }

    if (chunkSummaries.length === 1) {
      parsed = chunkSummaries[0];
      rawText = chunkResponses[0]?.rawText || "";
      finalPrompt = chunkResponses[0]?.prompt || "";
    } else {
      const combinedPrompt = `
Date: ${date}
Repository: ${REPO}
Total commits: ${commits.length}
Type breakdown (authoritative): ${JSON.stringify(buildTypeBreakdown(commits))}
Chunk summaries (for context):
${chunkSummaries
  .map(
    (summary, idx) =>
      `${idx + 1}. Summary: ${summary.summary}\nHighlights: ${
        summary.highlights?.join("; ") || "n/a"
      }\nRisks: ${summary.risks?.join("; ") || "n/a"}`
  )
  .join("\n\n")}

Guidelines:
- Provide 6-10 highlights describing distinct themes (not individual commits).
- Avoid listing raw commit titles.

Return JSON only with this shape:
${summaryShape}
Return a single valid JSON object only. Do not wrap it in markdown code fences.
`;

      const response = await callModel({
        prompt: combinedPrompt,
        maxTokens: SUMMARY_MAX_TOKENS,
        overrideSystem:
          systemPrompt +
          " Return ONLY a valid JSON object. Do not include analysis or reasoning.",
      });

      rawText = response.content
        ?.filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("\n")
        .trim();

      parsed = parseAiResponse(rawText || "", commits);
      if (parsed) {
        parsed.type_breakdown = buildTypeBreakdown(commits);
      }
      finalPrompt = combinedPrompt.trim();
    }

    if (parsed) {
      parsed.type_breakdown = buildTypeBreakdown(commits);
      const minHighlights = 6;
      const existingHighlights = Array.isArray(parsed.highlights)
        ? parsed.highlights
        : [];
      let mergedHighlights = [...existingHighlights];

      if (mergedHighlights.length < minHighlights && chunkSummaries.length > 1) {
        const chunkHighlights = chunkSummaries
          .map((summary) => summary.summary)
          .filter(Boolean)
          .filter((text) => text !== parsed.summary);
        mergedHighlights = mergedHighlights.concat(chunkHighlights);
      }

      if (mergedHighlights.length < minHighlights) {
        mergedHighlights = mergedHighlights.concat(
          buildThemeHighlights(commits, minHighlights)
        );
      }

      const deduped = [];
      const seen = new Set();
      for (const item of mergedHighlights) {
        const normalized = String(item).trim();
        if (!normalized) continue;
        const key = normalized.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        deduped.push(normalized);
      }
      parsed.highlights = deduped.slice(0, Math.max(minHighlights, deduped.length));
    }

    const responseSnapshot = {
      attempts,
      chunks: chunkSummaries.length,
      chunkResponses,
    };

    const payload = {
      summary: parsed || null,
      raw: rawText,
      cache: { source: "minimax", chunks: chunkSummaries.length },
    };

    dbRun(
      `
      INSERT INTO daily_summaries (
        repo, date, fingerprint, summary_json, raw_text,
        prompt_text, response_text, response_json, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(repo, date) DO UPDATE SET
        fingerprint = excluded.fingerprint,
        summary_json = excluded.summary_json,
        raw_text = excluded.raw_text,
        prompt_text = excluded.prompt_text,
        response_text = excluded.response_text,
        response_json = excluded.response_json,
        updated_at = excluded.updated_at
    `,
      [
        REPO,
        date,
        fingerprint,
        parsed ? JSON.stringify(parsed) : null,
        rawText,
        finalPrompt || "",
        rawText,
        JSON.stringify(responseSnapshot),
        new Date().toISOString(),
      ]
    );
    persistDb();

    if (debug) {
      payload.debug = {
        prompt: finalPrompt || "",
        responseText: rawText,
        responseJson: responseSnapshot,
      };
    }
    res.json(payload);
  } catch (error) {
    res.status(500).json({
      error: "AI summary failed",
      details: error.message,
    });
  }
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, repo: REPO, aiAvailable: Boolean(summarizer) });
});

app.get("/api/summary", (req, res) => {
  const date = req.query.date;
  if (!date) {
    return res.status(400).json({ error: "Missing date query param" });
  }
  const cached = dbGet(
    `
    SELECT fingerprint, summary_json, raw_text, prompt_text, response_text, response_json, updated_at
    FROM daily_summaries
    WHERE repo = ? AND date = ?
  `,
    [REPO, date]
  );
  if (!cached) {
    return res.status(404).json({ error: "No cached summary for date" });
  }
  res.json({
    date,
    fingerprint: cached.fingerprint,
    summary: cached.summary_json ? JSON.parse(cached.summary_json) : null,
    raw: cached.raw_text || "",
    prompt: cached.prompt_text || "",
    responseText: cached.response_text || "",
    responseJson: cached.response_json ? JSON.parse(cached.response_json) : null,
    updatedAt: cached.updated_at,
  });
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Clawdbot news dashboard running on http://localhost:${PORT}`);
});
