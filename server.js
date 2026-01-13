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
const GH_GRAPHQL = "https://api.github.com/graphql";
const SYNC_INTERVAL_MINUTES = Number(process.env.SYNC_INTERVAL_MINUTES || 10);
const INITIAL_SYNC_TIMEOUT_MS = Number(
  process.env.INITIAL_SYNC_TIMEOUT_MS || 12000
);
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
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE || 80);
const SUMMARY_MAX_TOKENS = Number(process.env.SUMMARY_MAX_TOKENS || 1200);
const SUMMARY_FILE_COMMITS = Number(process.env.SUMMARY_FILE_COMMITS || 8);
const SUMMARY_CONCURRENCY = Number(process.env.SUMMARY_CONCURRENCY || 3);

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
  CREATE TABLE IF NOT EXISTS commit_ai (
    sha TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    branch TEXT NOT NULL,
    label TEXT NOT NULL,
    sentence TEXT NOT NULL,
    model TEXT,
    updated_at TEXT NOT NULL
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

function dbInsertMany(sql, rows) {
  dbRun("BEGIN");
  for (const params of rows) {
    dbRun(sql, params);
  }
  dbRun("COMMIT");
  persistDb();
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
    error.rateLimitRemaining = response.headers.get("x-ratelimit-remaining");
    error.rateLimitReset = response.headers.get("x-ratelimit-reset");
    throw error;
  }
  return response.json();
}

async function ghGraphql(query, variables) {
  const response = await fetch(GH_GRAPHQL, {
    method: "POST",
    headers: {
      ...ghHeaders(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    const text = await response.text();
    const error = new Error(`GitHub API error ${response.status}`);
    error.status = response.status;
    error.details = text;
    error.rateLimitRemaining = response.headers.get("x-ratelimit-remaining");
    error.rateLimitReset = response.headers.get("x-ratelimit-reset");
    throw error;
  }

  const payload = await response.json();
  if (payload.errors?.length) {
    const error = new Error("GitHub GraphQL error");
    error.status = 500;
    error.details = JSON.stringify(payload.errors);
    throw error;
  }
  return payload.data;
}

function parseRepo() {
  const [owner, name] = REPO.split("/");
  if (!owner || !name) return null;
  return { owner, name };
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
  const authorLogin = detail.author?.login || null;
  const committerLogin = detail.committer?.login || null;
  const authorAvatar = detail.author?.avatar_url || null;
  const committerAvatar = detail.committer?.avatar_url || null;
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
    authorLogin,
    committerLogin,
    authorAvatar,
    committerAvatar,
    date,
    url: detail.html_url,
    stats: detail.stats || { additions: 0, deletions: 0, total: 0 },
    files,
    type: classifyCommit(title),
  };
}

function mapCommitNode(node) {
  const message = node.message || node.messageHeadline || "";
  const title = node.messageHeadline || message.split("\n")[0] || "(no message)";
  const additions = Number(node.additions || 0);
  const deletions = Number(node.deletions || 0);
  return {
    sha: node.oid,
    shortSha: node.oid?.slice(0, 7),
    title,
    message,
    author:
      node.author?.name ||
      node.committer?.name ||
      node.author?.user?.login ||
      "Unknown",
    authorLogin: node.author?.user?.login || null,
    committerLogin: node.committer?.user?.login || null,
    authorAvatar: node.author?.user?.avatarUrl || null,
    committerAvatar: node.committer?.user?.avatarUrl || null,
    date: node.committedDate || null,
    url: node.url,
    stats: {
      total: additions + deletions,
      additions,
      deletions,
    },
    files: [],
    type: classifyCommit(title),
  };
}

function selectTopCommits(commits, limit) {
  return [...commits]
    .filter((commit) => commit?.sha)
    .sort((a, b) => (b.stats?.total || 0) - (a.stats?.total || 0))
    .slice(0, limit);
}

async function enrichCommitFiles(commits, limit) {
  const targets = selectTopCommits(commits, limit).filter(
    (commit) => !Array.isArray(commit.files) || commit.files.length === 0
  );
  for (const commit of targets) {
    try {
      const detail = await ghFetch(`${GH_BASE}/commits/${commit.sha}`);
      const mapped = mapCommitDetail(detail);
      commit.files = mapped.files || [];
    } catch (error) {
      console.warn(`Failed to enrich files for ${commit.sha}:`, error.message);
    }
  }
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
    fullSha: commit.sha,
    title: commit.title,
    type: commit.type,
    additions: commit.stats?.additions ?? 0,
    deletions: commit.stats?.deletions ?? 0,
    files: commit.files?.slice(0, 8).map((file) => file.filename) ?? [],
  }));
}

function formatCommitLines(
  commits,
  { includeFiles = true, filesForShas = null } = {}
) {
  return normalizeCommitsForPrompt(commits)
    .map((commit, index) => {
      const allowFiles =
        includeFiles &&
        (!filesForShas || (commit.fullSha && filesForShas.has(commit.fullSha)));
      const fileText =
        allowFiles && commit.files.length
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

function guessHighlightType(text = "") {
  const value = String(text).toLowerCase();
  if (value.includes("fix") || value.includes("bug")) return "fix";
  if (value.includes("refactor")) return "refactor";
  if (value.includes("doc")) return "docs";
  if (value.includes("test")) return "test";
  if (value.includes("chore")) return "chore";
  if (value.includes("feature") || value.includes("add")) return "feature";
  return "update";
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
    { key: "changelog", label: "Changelog entries" },
    { key: "docs", label: "Documentation" },
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
    topThemes.length > 0 ? topThemes.join(" and ") : "core platform work";
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
    {
      key: "discord",
      label: "Discord",
      template: (count) =>
        `Discord: ${count} commits across provider actions and tools.`,
    },
    {
      key: "telegram",
      label: "Telegram",
      template: (count) =>
        `Telegram: ${count} commits around command handling and routing.`,
    },
    {
      key: "plugin",
      label: "Plugins",
      template: (count) =>
        `Plugins: ${count} commits in plugin loading and tooling.`,
    },
    {
      key: "dm",
      label: "Direct messages",
      template: (count) =>
        `Direct messages: ${count} commits on history limits and behavior.`,
    },
    {
      key: "docs",
      label: "Docs",
      template: (count) =>
        `Docs: ${count} commits across guides, references, and changelog notes.`,
    },
    {
      key: "test",
      label: "Tests",
      template: (count) =>
        `Tests: ${count} commits improving coverage and fixtures.`,
    },
    {
      key: "config",
      label: "Config",
      template: (count) =>
        `Config: ${count} commits in schema, defaults, and validation.`,
    },
    {
      key: "release",
      label: "Release",
      template: (count) =>
        `Release: ${count} commits in packaging and release flow.`,
    },
    {
      key: "ci",
      label: "CI",
      template: (count) =>
        `CI: ${count} commits for pipelines and automation.`,
    },
    {
      key: "cli",
      label: "CLI",
      template: (count) =>
        `CLI: ${count} commits for commands and onboarding flow.`,
    },
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
    .map(([label, count]) => {
      const theme = themes.find((item) => item.label === label);
      return theme?.template ? theme.template(count) : `${label}: ${count} commits.`;
    });
}

function sanitizeCommitTitle(title = "") {
  const cleaned = String(title)
    .replace(/^merge pull request.*$/i, "")
    .replace(/^merge branch.*$/i, "")
    .replace(/^(feat|fix|docs|test|chore|refactor|perf|style)(\([^)]+\))?:\s*/i, "")
    .trim();
  return cleaned || title.trim();
}

function buildCommitPrompt(commit) {
  const files = (commit.files || [])
    .map((file) => (typeof file === "string" ? file : file.filename || ""))
    .filter(Boolean)
    .slice(0, 12);
  return `Commit:
Title: ${commit.title || ""}
Message: ${commit.message || ""}
Files: ${files.length ? files.join(", ") : "n/a"}
Stats: +${commit.stats?.additions || 0} -${commit.stats?.deletions || 0}

Return JSON only:
{
  "label": "feature|fix|refactor|docs|test|chore|other|update",
  "sentence": "One sentence describing what this commit did in plain language."
}
No markdown, no extra keys.`;
}

async function classifyCommitWithAi(commit) {
  const prompt = buildCommitPrompt(commit);
  const response = await summarizer.messages.create({
    model: AI_MODEL,
    max_tokens: 200,
    temperature: 0.1,
    thinking: { type: "disabled" },
    system:
      "You label a single commit. Be concrete, avoid generic wording. " +
      "Return only JSON with label + sentence.",
    messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
  });
  const rawText = response.content
    ?.filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n")
    .trim();
  const parsed = extractJson(rawText || "");
  const label = String(parsed?.label || classifyCommit(commit.title || ""))
    .toLowerCase()
    .trim();
  const sentence =
    String(parsed?.sentence || sanitizeCommitTitle(commit.title || "")).trim() ||
    "No summary available.";
  return {
    sha: commit.sha,
    label,
    sentence,
    model: response.model,
  };
}

async function runWithConcurrency(items, limit, handler) {
  const results = [];
  let index = 0;
  const workers = Array.from({ length: limit }).map(async () => {
    while (index < items.length) {
      const current = items[index];
      index += 1;
      results.push(await handler(current));
    }
  });
  await Promise.all(workers);
  return results;
}

async function getCommitSummaries(commits, forceCommitAi) {
  const pending = [];
  const results = [];
  for (const commit of commits) {
    const cached = !forceCommitAi ? getCommitAi(commit.sha) : null;
    if (cached) {
      results.push({
        sha: commit.sha,
        label: cached.label,
        sentence: cached.sentence,
        model: cached.model,
      });
      continue;
    }
    pending.push(commit);
  }

  if (!pending.length) return results;
  const generated = await runWithConcurrency(
    pending,
    SUMMARY_CONCURRENCY,
    async (commit) => classifyCommitWithAi(commit)
  );
  cacheCommitAi(generated);
  return results.concat(generated);
}

function truncateWords(text, maxWords) {
  const words = String(text).trim().split(/\s+/).filter(Boolean);
  if (words.length <= maxWords) return String(text).trim();
  return `${words.slice(0, maxWords).join(" ")}.`;
}

async function summarizeLabel(label, sentences) {
  if (!sentences.length) return "";
  if (sentences.length === 1) return sentences[0];
  const prompt = `Label: ${label}
Sentences:
${sentences.map((line, idx) => `${idx + 1}. ${line}`).join("\n")}

Return JSON only:
{
  "text": "Combine these into 1-3 concise sentences (max 100 words). Focus on the most important changes only, drop minor test/doc churn unless it is substantial, and keep it concrete."
}`;
  const response = await summarizer.messages.create({
    model: AI_MODEL,
    max_tokens: 300,
    temperature: 0.1,
    thinking: { type: "disabled" },
    system:
      "You merge short commit sentences into a concise label summary. " +
      "Be specific, remove noise, and keep it under 100 words. " +
      "Return only JSON.",
    messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
  });
  const rawText = response.content
    ?.filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n")
    .trim();
  const parsed = extractJson(rawText || "");
  return truncateWords(String(parsed?.text || sentences.join(" ")).trim(), 100);
}

async function buildLabelSummary(commitSummaries) {
  const buckets = new Map();
  for (const entry of commitSummaries) {
    const label = entry.label || "other";
    if (!buckets.has(label)) buckets.set(label, []);
    buckets.get(label).push(entry.sentence);
  }

  const output = [];
  for (const [label, sentences] of buckets.entries()) {
    const text = await summarizeLabel(label, sentences);
    output.push({ label, text, count: sentences.length });
  }

  return output.sort((a, b) => b.count - a.count);
}

async function buildOverallSummary(labelSummaries) {
  if (!labelSummaries.length) return "Summary unavailable.";
  const prompt = `Label summaries:
${labelSummaries
  .map((entry) => `- ${entry.label}: ${entry.text}`)
  .join("\n")}

Return JSON only:
{
  "summary": "1-2 sentence daily summary."
}`;
  const response = await summarizer.messages.create({
    model: AI_MODEL,
    max_tokens: 240,
    temperature: 0.1,
    thinking: { type: "disabled" },
    system:
      "You write a short daily summary from label summaries. " +
      "No generic filler. Return only JSON.",
    messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
  });
  const rawText = response.content
    ?.filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n")
    .trim();
  const parsed = extractJson(rawText || "");
  return String(parsed?.summary || labelSummaries[0].text).trim();
}

function extractFileHint(files = [], { allowDocs = true } = {}) {
  if (!files.length) return "";
  const skipNames = new Set([
    "CHANGELOG.md",
    "README.md",
    "pnpm-lock.yaml",
    "package-lock.json",
  ]);
  const cleaned = files
    .map((file) => String(file || "").trim())
    .filter(Boolean)
    .filter((file) => !skipNames.has(file));
  let pick =
    cleaned.find((file) => allowDocs || !file.startsWith("docs/")) ||
    cleaned[0] ||
    files.find(Boolean) ||
    "";
  if (!pick) return "";
  const parts = pick.split("/");
  if (parts.length >= 2) {
    return `${parts[0]}/${parts[1]}`;
  }
  return parts[0] || "";
}

function detectTheme(commit) {
  const title = (commit.title || "").toLowerCase();
  const files = (commit.files || []).map((file) =>
    typeof file === "string" ? file : file.filename || ""
  );
  const pathText = files.join(" ").toLowerCase();
  if (pathText.includes("docs/") || title.includes("docs") || title.includes("readme")) {
    return "Docs";
  }
  if (
    pathText.includes(".github/") ||
    title.includes("ci") ||
    title.includes("pipeline") ||
    title.includes("workflow")
  ) {
    return "CI";
  }
  if (pathText.includes("src/config") || title.includes("config")) {
    return "Config";
  }
  if (pathText.includes("src/plugins") || pathText.includes("extensions") || title.includes("plugin")) {
    return "Plugins";
  }
  if (pathText.includes("src/cli") || title.includes("cli") || title.includes("command")) {
    return "CLI";
  }
  if (
    pathText.includes("ui/") ||
    pathText.includes("apps/macos") ||
    title.includes("ui") ||
    title.includes("wizard")
  ) {
    return "UI";
  }
  if (pathText.includes("src/agents") || title.includes("agent")) {
    return "Agents";
  }
  if (pathText.includes("src/gateway") || title.includes("gateway")) {
    return "Gateway";
  }
  if (pathText.includes("providers") || title.includes("provider") || title.includes("moonshot")) {
    return "Providers";
  }
  if (title.includes("test") || pathText.includes(".test.")) {
    return "Tests";
  }
  return "Other";
}

function buildConcreteThemeHighlights(commits, maxItems = 6) {
  const buckets = new Map();
  for (const commit of commits) {
    const theme = detectTheme(commit);
    if (!buckets.has(theme)) buckets.set(theme, []);
    buckets.get(theme).push(commit);
  }

  const ranked = Array.from(buckets.entries())
    .map(([theme, items]) => ({
      theme,
      items,
      score: items.reduce((sum, item) => sum + (item.stats?.total || 0), 0),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, maxItems);

  return ranked.map(({ theme, items }) => {
    const top = [...items].sort(
      (a, b) => (b.stats?.total || 0) - (a.stats?.total || 0)
    );
    const titleBits = top
      .slice(0, 2)
      .map((item) => sanitizeCommitTitle(item.title || ""))
      .filter(Boolean);
    const fileHint = extractFileHint(
      (top[0]?.files || []).map((file) =>
        typeof file === "string" ? file : file.filename || ""
      )
    );
    const detail = titleBits.length
      ? titleBits.join(" + ")
      : "Key changes across core files";
    const hintText = fileHint ? ` (${fileHint})` : "";
    return `${theme}: ${detail}${hintText}.`;
  });
}

function isGenericHighlight(text = "") {
  const value = text.toLowerCase();
  if (!value.includes("commits")) return false;
  return (
    value.startsWith("docs") ||
    value.startsWith("tests") ||
    value.startsWith("config") ||
    value.startsWith("plugins") ||
    value.startsWith("cli") ||
    value.startsWith("ci") ||
    value.startsWith("documentation") ||
    value.startsWith("testing")
  );
}

function buildConcreteHighlightObjects(commits, maxItems = 6) {
  const buckets = new Map();
  for (const commit of commits) {
    const theme = detectTheme(commit);
    if (!buckets.has(theme)) buckets.set(theme, []);
    buckets.get(theme).push(commit);
  }

  const otherBucket = buckets.get("Other") || [];
  const ranked = Array.from(buckets.entries())
    .filter(([theme]) => theme !== "Other")
    .map(([theme, items]) => ({
      theme,
      items,
      score: items.reduce((sum, item) => sum + (item.stats?.total || 0), 0),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, maxItems);

  if (ranked.length < maxItems && otherBucket.length) {
    ranked.push({
      theme: "Other",
      items: otherBucket,
      score: otherBucket.reduce((sum, item) => sum + (item.stats?.total || 0), 0),
    });
  }

  const highlights = ranked.map(({ theme, items }) => {
    const top = [...items].sort(
      (a, b) => (b.stats?.total || 0) - (a.stats?.total || 0)
    );
    const titleBits = top
      .slice(0, 2)
      .map((item) => sanitizeCommitTitle(item.title || ""))
      .filter(Boolean);
    const fileHint = extractFileHint(
      (top[0]?.files || []).map((file) =>
        typeof file === "string" ? file : file.filename || ""
      ),
      { allowDocs: theme === "Docs" }
    );
    const typeCounts = top.reduce((acc, item) => {
      const t = item.type || "other";
      acc[t] = (acc[t] || 0) + 1;
      return acc;
    }, {});
    const type = Object.entries(typeCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || "update";
    const detail = titleBits.length
      ? titleBits.join(" + ")
      : "Key changes across core files";
    return {
      type,
      what: theme === "Other" ? `Misc: ${detail}` : `${theme}: ${detail}`,
      where: fileHint || "",
    };
  });

  if (highlights.length >= maxItems) {
    return highlights.slice(0, maxItems);
  }

  const usedShas = new Set();
  ranked.forEach(({ items }) => items.forEach((item) => usedShas.add(item.sha)));
  const topCommits = selectTopCommits(commits, maxItems * 2).filter(
    (commit) => !usedShas.has(commit.sha)
  );
  for (const commit of topCommits) {
    if (highlights.length >= maxItems) break;
    const fileHint = extractFileHint(
      (commit.files || []).map((file) =>
        typeof file === "string" ? file : file.filename || ""
      ),
      { allowDocs: false }
    );
    const what = sanitizeCommitTitle(commit.title || "");
    if (!what) continue;
    highlights.push({
      type: commit.type || "other",
      what: `Misc: ${what}`,
      where: fileHint || "",
    });
  }

  return highlights;
}

function getCachedSummaryRow(date) {
  return dbGet(
    `
    SELECT fingerprint, summary_json, raw_text, prompt_text, response_text, response_json, updated_at
    FROM daily_summaries
    WHERE repo = ? AND date = ?
  `,
    [REPO, date]
  );
}

function buildCachedSummaryPayload(cached, debug) {
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
  return payload;
}

async function createSummaryPayload({ date, commits, force = false, debug = false }) {
  if (!Array.isArray(commits) || commits.length === 0) {
    const error = new Error("No commits provided");
    error.status = 400;
    throw error;
  }

  const fingerprint = commits.map((commit) => commit.sha).join("|");
  if (!force) {
    const cached = getCachedSummaryRow(date);
    if (cached && cached.fingerprint === fingerprint) {
      return buildCachedSummaryPayload(cached, debug);
    }
  }

  if (!summarizer) {
    const error = new Error("AI summary not configured");
    error.status = 503;
    throw error;
  }

  const commitSummaries = await getCommitSummaries(commits, false);
  const labelSummaries = await buildLabelSummary(commitSummaries);
  const summaryText = await buildOverallSummary(labelSummaries);
  const payload = {
    summary: {
      summary: summaryText,
      buckets: labelSummaries,
      type_breakdown: buildTypeBreakdown(commits),
      risks: [],
    },
    raw: "",
    cache: { source: "minimax" },
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
      JSON.stringify(payload.summary),
      "",
      "",
      "",
      JSON.stringify({ labelSummaries }),
      new Date().toISOString(),
    ]
  );
  persistDb();

  return payload;
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
  const normalizedHighlights = Array.isArray(parsed.highlights)
    ? parsed.highlights
        .map((item) => {
          if (!item) return null;
          if (typeof item === "string") {
            return {
              type: guessHighlightType(item),
              what: item,
              where: "",
            };
          }
          const type = item.type || item.kind || item.category || "update";
          const what =
            item.what ||
            item.summary ||
            item.text ||
            item.title ||
            "";
          const where = item.where || item.area || item.path || "";
          return {
            type: String(type).toLowerCase(),
            what: String(what),
            where: String(where),
          };
        })
        .filter(Boolean)
    : [];
  parsed.highlights = normalizedHighlights;
  return parsed;
}

async function awaitWithTimeout(promise, timeoutMs) {
  let timeoutId = null;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        timeoutId = setTimeout(() => resolve({ __timeout: true }), timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
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

function loadCommitsForDate(dateKey) {
  const start = new Date(`${dateKey}T00:00:00.000Z`);
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);
  const rows = dbAll(
    `
    SELECT data FROM commits
    WHERE repo = ? AND branch = ? AND date >= ? AND date < ?
    ORDER BY date DESC
  `,
    [REPO, BRANCH, start.toISOString(), end.toISOString()]
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

function countCommitsSince(sinceIso) {
  const row = dbGet(
    `
    SELECT COUNT(*) as count
    FROM commits
    WHERE repo = ? AND branch = ? AND date >= ?
  `,
    [REPO, BRANCH, sinceIso]
  );
  return Number(row?.count || 0);
}

function hasCommit(sha) {
  const row = dbGet("SELECT sha FROM commits WHERE sha = ?", [sha]);
  return Boolean(row?.sha);
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

function getCommitAi(sha) {
  const row = dbGet(
    `
    SELECT label, sentence, model, updated_at
    FROM commit_ai
    WHERE sha = ?
  `,
    [sha]
  );
  return row || null;
}

function getSummaryFingerprint(dateKey) {
  const row = dbGet(
    `
    SELECT fingerprint
    FROM daily_summaries
    WHERE repo = ? AND date = ?
  `,
    [REPO, dateKey]
  );
  if (!row?.fingerprint) return new Set();
  return new Set(row.fingerprint.split("|").filter(Boolean));
}

async function ensureCommitAiForDate(dateKey, commits = null) {
  if (!summarizer) return;
  const list = commits || loadCommitsForDate(dateKey);
  const pending = list.filter((commit) => !getCommitAi(commit.sha));
  if (!pending.length) return;
  const generated = await runWithConcurrency(
    pending,
    SUMMARY_CONCURRENCY,
    async (commit) => classifyCommitWithAi(commit)
  );
  cacheCommitAi(generated);
}

function countQualifiedNewCommits(commits, summarySet) {
  let count = 0;
  for (const commit of commits) {
    if (summarySet.has(commit.sha)) continue;
    if (getCommitAi(commit.sha)) count += 1;
  }
  return count;
}

function cacheCommitAi(entries) {
  if (!entries.length) return;
  const now = new Date().toISOString();
  dbInsertMany(
    `
    INSERT INTO commit_ai (sha, repo, branch, label, sentence, model, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sha) DO UPDATE SET
      repo = excluded.repo,
      branch = excluded.branch,
      label = excluded.label,
      sentence = excluded.sentence,
      model = excluded.model,
      updated_at = excluded.updated_at
  `,
    entries.map((entry) => [
      entry.sha,
      REPO,
      BRANCH,
      entry.label,
      entry.sentence,
      entry.model || null,
      now,
    ])
  );
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

function countIssuesSince(sinceIso) {
  const row = dbGet(
    `
    SELECT COUNT(*) as count
    FROM issues
    WHERE repo = ? AND created_at >= ?
  `,
    [REPO, sinceIso]
  );
  return Number(row?.count || 0);
}

function loadSummaries({ sinceIso }) {
  const rows = dbAll(
    `
    SELECT date, summary_json, fingerprint
    FROM daily_summaries
    WHERE repo = ? AND date >= ?
  `,
    [REPO, sinceIso]
  );
  return rows.map((row) => ({
    date: row.date,
    summary: row.summary_json ? JSON.parse(row.summary_json) : null,
    fingerprint: row.fingerprint,
  }));
}

function shouldSync({ sinceIso, force }) {
  if (force) return true;
  const backfillSince = getMeta("commits_backfill_since");
  if (!backfillSince) return true;
  if (new Date(backfillSince).getTime() > new Date(sinceIso).getTime()) {
    return true;
  }
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

  const repoInfo = parseRepo();
  if (!repoInfo) {
    const error = new Error("Invalid GITHUB_REPO format. Expected owner/name.");
    error.status = 500;
    throw error;
  }

  const backfillSince = getMeta("commits_backfill_since");
  const backfillNeeded =
    !backfillSince ||
    new Date(backfillSince).getTime() > new Date(sinceIso).getTime();

  const overlapMs = 5 * 60 * 1000;
  const latest = getLatestCommitDate();
  const sinceMs = new Date(sinceIso).getTime();
  const fetchSince = latest
    ? new Date(Math.max(new Date(latest).getTime() - overlapMs, sinceMs)).toISOString()
    : sinceIso;

  const perPage = 100;
  let fetchedCount = 0;
  let cursor = null;
  const cutoffIso = backfillNeeded ? sinceIso : fetchSince;
  const cutoffMs = new Date(cutoffIso).getTime();
  let reachedCutoff = false;
  const query = `
    query ($owner: String!, $name: String!, $branch: String!, $cursor: String, $perPage: Int!) {
      repository(owner: $owner, name: $name) {
        ref(qualifiedName: $branch) {
          target {
            ... on Commit {
              history(first: $perPage, after: $cursor) {
                nodes {
                  oid
                  message
                  messageHeadline
                  committedDate
                  url
                  additions
                  deletions
                  author { name user { login avatarUrl } }
                  committer { name user { login avatarUrl } }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
      }
    }
  `;

  while (true) {
    const data = await ghGraphql(query, {
      owner: repoInfo.owner,
      name: repoInfo.name,
      branch: BRANCH,
      cursor,
      perPage,
    });

    if (!data?.repository?.ref && !cursor) {
      const error = new Error(`Branch '${BRANCH}' not found.`);
      error.status = 404;
      throw error;
    }

    const history = data?.repository?.ref?.target?.history;

    const nodes = history?.nodes || [];
    if (!nodes.length) break;

    const mapped = nodes.map(mapCommitNode);
    const withinCutoff = mapped.filter((commit) => {
      if (!commit.date) return true;
      return new Date(commit.date).getTime() >= cutoffMs;
    });
    const fresh = withinCutoff.filter((commit) => !hasCommit(commit.sha));
    if (fresh.length) {
      cacheCommits(fresh);
      fetchedCount += fresh.length;
    }

    const oldestDate = mapped[mapped.length - 1]?.date;
    const oldestMs = oldestDate ? new Date(oldestDate).getTime() : null;
    if (!history?.pageInfo?.hasNextPage || (oldestMs !== null && oldestMs < cutoffMs)) {
      reachedCutoff = true;
      break;
    }
    cursor = history.pageInfo.endCursor;
  }

  const now = new Date().toISOString();
  setMeta("last_sync_at", now);
  setMeta("last_sync_fetched_count", String(fetchedCount));
  if (backfillNeeded && reachedCutoff) {
    const backfillMs = backfillSince ? new Date(backfillSince).getTime() : null;
    const sinceCutoffMs = new Date(sinceIso).getTime();
    const earliestMs =
      backfillMs && backfillMs < sinceCutoffMs ? backfillMs : sinceCutoffMs;
    setMeta("commits_backfill_since", new Date(earliestMs).toISOString());
  }
  return {
    synced: true,
    source: "github",
    lastSyncAt: now,
    fetchSince: cutoffIso,
    fetchedCount,
  };
}

let commitsSyncPromise = null;
let issuesSyncPromise = null;
const summaryJobs = new Map();

function scheduleSummaryForDate(dateKey) {
  if (!summarizer || !dateKey) return;
  if (summaryJobs.has(dateKey)) return;
  const commits = loadCommitsForDate(dateKey);
  if (!commits.length) return;
  const cached = getCachedSummaryRow(dateKey);
  const summarySet = cached?.fingerprint
    ? new Set(cached.fingerprint.split("|"))
    : new Set();
  const currentFingerprint = commits.map((commit) => commit.sha).join("|");
  const hasSummary = Boolean(cached?.summary_json);
  const isStale =
    Boolean(hasSummary && cached?.fingerprint) &&
    cached.fingerprint !== currentFingerprint;
  const todayKey = new Date().toISOString().split("T")[0];
  const isPast = dateKey < todayKey;
  const newQualified = countQualifiedNewCommits(commits, summarySet);
  const shouldFinalize = isPast && (!hasSummary || isStale);
  const shouldRefreshToday = !shouldFinalize && dateKey === todayKey && newQualified >= 10;

  if (!shouldFinalize && !shouldRefreshToday) {
    ensureCommitAiForDate(dateKey, commits).catch((error) => {
      console.warn(`Commit AI failed for ${dateKey}:`, error.message);
    });
    return;
  }

  const job = (async () => {
    try {
      await ensureCommitAiForDate(dateKey, commits);
      if (!shouldFinalize) {
        const updatedSet = getSummaryFingerprint(dateKey);
        const qualifiedAfter = countQualifiedNewCommits(commits, updatedSet);
        if (qualifiedAfter < 10) return;
      }
      await createSummaryPayload({ date: dateKey, commits });
    } catch (error) {
      console.warn(`Summary job failed for ${dateKey}:`, error.message);
    } finally {
      summaryJobs.delete(dateKey);
    }
  })();

  summaryJobs.set(dateKey, job);
}

async function runCommitsSync({ sinceIso, force }) {
  if (!commitsSyncPromise) {
    commitsSyncPromise = syncCommits({ sinceIso, force })
      .then((result) => {
        const todayKey = new Date().toISOString().split("T")[0];
        scheduleSummaryForDate(todayKey);
        return result;
      })
      .finally(() => {
      commitsSyncPromise = null;
      });
  }
  return commitsSyncPromise;
}

async function runIssuesSync({ sinceIso, force }) {
  if (!issuesSyncPromise) {
    issuesSyncPromise = syncIssuesAndPRs({ sinceIso, force }).finally(() => {
      issuesSyncPromise = null;
    });
  }
  return issuesSyncPromise;
}

function shouldSyncIssues({ sinceIso, force }) {
  if (force) return true;
  const backoffUntil = getMeta("issues_sync_backoff_until");
  if (backoffUntil && Date.now() < new Date(backoffUntil).getTime()) {
    return false;
  }
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

  try {
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
  } catch (error) {
    if (error?.status === 403) {
      const remaining = Number(error.rateLimitRemaining || 0);
      const resetSeconds = Number(error.rateLimitReset || 0);
      const isRateLimit =
        remaining === 0 ||
        String(error.details || "").toLowerCase().includes("rate limit");
      if (isRateLimit) {
        const resetAt = resetSeconds
          ? new Date(resetSeconds * 1000).toISOString()
          : new Date(Date.now() + 15 * 60 * 1000).toISOString();
        setMeta("issues_sync_backoff_until", resetAt);
      }
    }
    throw error;
  }

  if (fetched.length) {
    cacheIssues(fetched);
  }

  const now = new Date().toISOString();
  setMeta("last_issues_sync_at", now);
  setMeta("last_issues_sync_fetched_count", String(fetched.length));
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
    const forceSync = req.query.force === "1";
    const backfillPending =
      !getMeta("commits_backfill_since") ||
      new Date(getMeta("commits_backfill_since")).getTime() >
        new Date(sinceIso).getTime();

    let sync = {
      source: "sqlite",
      synced: false,
      lastSyncAt: getMeta("last_sync_at"),
      pending: false,
      error: null,
    };
    let issueSync = {
      source: "sqlite",
      synced: false,
      lastSyncAt: getMeta("last_issues_sync_at"),
      pending: false,
      error: null,
    };

    if (forceSync) {
      try {
        const commitSync = await awaitWithTimeout(
          runCommitsSync({ sinceIso, force: true }),
          INITIAL_SYNC_TIMEOUT_MS
        );
        if (commitSync?.__timeout) {
          sync = {
            source: "sqlite",
            synced: false,
            lastSyncAt: getMeta("last_sync_at"),
            pending: true,
            timeout: true,
          };
        } else {
          sync = commitSync;
        }
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
      try {
        const issuesResult = await awaitWithTimeout(
          runIssuesSync({ sinceIso, force: true }),
          INITIAL_SYNC_TIMEOUT_MS
        );
        if (issuesResult?.__timeout) {
          issueSync = {
            source: "sqlite",
            synced: false,
            lastSyncAt: getMeta("last_issues_sync_at"),
            pending: true,
            timeout: true,
          };
        } else {
          issueSync = issuesResult;
        }
      } catch (error) {
        issueSync = {
          source: "sqlite-stale",
          synced: false,
          lastSyncAt: getMeta("last_issues_sync_at"),
          error: error.message,
        };
      }
    } else {
      const shouldSyncCommits = shouldSync({ sinceIso, force: false });
      const shouldSyncIssuesFlag = shouldSyncIssues({ sinceIso, force: false });
      if (shouldSyncCommits) {
        runCommitsSync({ sinceIso, force: false }).catch((error) => {
          console.warn("Commit sync failed:", error.message);
        });
      }
      if (shouldSyncIssuesFlag) {
        runIssuesSync({ sinceIso, force: false }).catch((error) => {
          console.warn("Issue sync failed:", error.message);
        });
      }
      sync.pending = shouldSyncCommits;
      issueSync.pending = shouldSyncIssuesFlag;
    }

    const responseCommits = loadCachedCommits({ sinceIso });
    const todayKey = new Date().toISOString().split("T")[0];
    if (responseCommits.length > 0) {
      scheduleSummaryForDate(todayKey);
    }
    const issues = loadIssues({ sinceIso, isPr: false });
    const pullRequests = loadIssues({ sinceIso, isPr: true });
    const summaries = loadSummaries({ sinceIso });
    const summariesByDate = new Map(
      summaries.map((entry) => [entry.date, entry])
    );
    const issuesByDate = groupByDateKey(issues, "created_at");
    const prsByDate = groupByDateKey(pullRequests, "created_at");
    const daysWithExtras = groupByDate(responseCommits).map((day) => ({
      ...day,
      issues: issuesByDate.get(day.date) || [],
      pullRequests: prsByDate.get(day.date) || [],
      summary: summariesByDate.get(day.date)?.summary || null,
      summaryFingerprint: summariesByDate.get(day.date)?.fingerprint || null,
    }));
    daysWithExtras.forEach((day) => scheduleSummaryForDate(day.date));
    const initializing =
      responseCommits.length === 0 && (sync.pending || sync.timeout);

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
        lastSyncFetchedCount: Number(getMeta("last_sync_fetched_count") || 0),
        pending: sync.pending || false,
        syncing: Boolean(commitsSyncPromise),
        initializing,
        timeout: sync.timeout || false,
        commitCount: responseCommits.length,
        backfill: backfillPending,
        backfillSince: getMeta("commits_backfill_since"),
        error: sync.error || null,
        issues: {
          source: issueSync.source,
          synced: issueSync.synced,
          lastSyncAt: issueSync.lastSyncAt || getMeta("last_issues_sync_at"),
          fetchSince: issueSync.fetchSince || null,
          fetchedCount: issueSync.fetchedCount || 0,
          lastSyncFetchedCount: Number(
            getMeta("last_issues_sync_fetched_count") || 0
          ),
          pending: issueSync.pending || false,
          syncing: Boolean(issuesSyncPromise),
          timeout: issueSync.timeout || false,
          error: issueSync.error || null,
          backoffUntil: getMeta("issues_sync_backoff_until"),
          count: issues.length + pullRequests.length,
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
  try {
    const payload = await createSummaryPayload({ date, commits, force, debug });
    res.json(payload);
  } catch (error) {
    if (error.status === 503) {
      return res.status(503).json({
        error: "AI summary not configured",
        details: "Set ANTHROPIC_API_KEY and ANTHROPIC_BASE_URL",
      });
    }
    res.status(error.status || 500).json({
      error: error.status === 400 ? error.message : "AI summary failed",
      details: error.message,
    });
  }
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, repo: REPO, aiAvailable: Boolean(summarizer) });
});

app.get("/api/status", (req, res) => {
  const days = Number(req.query.days || 7);
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const sinceIso = since.toISOString();
  res.json({
    repo: REPO,
    branch: BRANCH,
    since: sinceIso,
    cache: {
      lastSyncAt: getMeta("last_sync_at"),
      lastSyncFetchedCount: Number(getMeta("last_sync_fetched_count") || 0),
      syncing: Boolean(commitsSyncPromise),
      commitCount: countCommitsSince(sinceIso),
      latestCommitDate: getLatestCommitDate(),
      issues: {
        lastSyncAt: getMeta("last_issues_sync_at"),
        lastSyncFetchedCount: Number(
          getMeta("last_issues_sync_fetched_count") || 0
        ),
        syncing: Boolean(issuesSyncPromise),
        count: countIssuesSince(sinceIso),
        backoffUntil: getMeta("issues_sync_backoff_until"),
      },
    },
  });
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

function escapeXml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function wrapCdata(text) {
  const safe = String(text ?? "").replaceAll("]]>", "]]]]><![CDATA[>");
  return `<![CDATA[${safe}]]>`;
}

function formatRssDate(dateValue) {
  const date = new Date(dateValue);
  return date.toUTCString();
}

app.get("/feed.xml", (req, res) => {
  const days = 7;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  const sinceIso = since.toISOString();
  const commits = loadCachedCommits({ sinceIso });
  const summaries = loadSummaries({ sinceIso });
  const summariesByDate = new Map(
    summaries.map((entry) => [entry.date, entry.summary])
  );
  const grouped = groupByDate(commits);
  const baseUrl = `${req.protocol}://${req.get("host")}`;
  const feedUrl = `${baseUrl}/feed.xml`;
  const now = new Date();

  const items = grouped.map((day) => {
    const summary = summariesByDate.get(day.date);
    const additions = day.commits.reduce(
      (total, commit) => total + (commit.stats?.additions || 0),
      0
    );
    const deletions = day.commits.reduce(
      (total, commit) => total + (commit.stats?.deletions || 0),
      0
    );
    const title = `Clawdbot news — ${day.date} (${day.commits.length} commits)`;
    const summaryText = summary?.summary || "Summary not available yet.";
    const buckets = summary?.buckets || [];
    const bucketText = buckets.length
      ? buckets.map((bucket) => `• ${bucket.label.toUpperCase()}: ${bucket.text}`).join("\n")
      : "";
    const bucketHtml = buckets.length
      ? `<ul>${buckets
          .map(
            (bucket) =>
              `<li><strong>${escapeXml(
                bucket.label.toUpperCase()
              )}:</strong> ${escapeXml(bucket.text)}</li>`
          )
          .join("")}</ul>`
      : "";
    const description = `
${summaryText}
${bucketHtml}
<p><strong>Commits:</strong> ${day.commits.length} (+${additions} / -${deletions})</p>
    `.trim();
    const pubDate = formatRssDate(`${day.date}T12:00:00.000Z`);
    return `
      <item>
        <title>${escapeXml(title)}</title>
        <link>${escapeXml(`${baseUrl}#day-${day.date}`)}</link>
        <guid isPermaLink="false">${escapeXml(`${REPO}:${BRANCH}:${day.date}`)}</guid>
        <pubDate>${escapeXml(pubDate)}</pubDate>
        <description>${wrapCdata(description)}</description>
      </item>
    `;
  });

  const rss = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>${escapeXml(`Clawdbot news — ${REPO}`)}</title>
    <link>${escapeXml(baseUrl)}</link>
    <atom:link href="${escapeXml(feedUrl)}" rel="self" type="application/rss+xml" />
    <description>${escapeXml(
      "Daily lobster log of commits with AI summaries."
    )}</description>
    <lastBuildDate>${escapeXml(formatRssDate(now))}</lastBuildDate>
    ${items.join("\n")}
  </channel>
</rss>`;

  res.type("application/rss+xml; charset=utf-8").send(rss);
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Clawdbot news dashboard running on http://localhost:${PORT}`);
});
