const state = {
  days: 7,
  data: null,
  initializing: false,
  lastSyncAt: null,
  lastIssuesSyncAt: null,
  statusTimer: null,
  newDataAvailable: false,
};

const elements = {
  timeline: document.getElementById("timeline"),
  loadingCard: document.getElementById("loadingCard"),
  repoPill: document.getElementById("repoPill"),
  repoLink: document.getElementById("repoLink"),
  syncPill: document.getElementById("syncPill"),
};

function escapeHtml(value) {
  const text = String(value ?? "");
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatDate(dateValue) {
  const date = new Date(dateValue);
  return date.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatTime(dateValue) {
  const date = new Date(dateValue);
  return date.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDelta(stats = {}) {
  const additions = stats.additions ?? 0;
  const deletions = stats.deletions ?? 0;
  return `+${additions} / -${deletions}`;
}

function renderEmpty(message, title = "No commits found") {
  elements.timeline.innerHTML = `
    <div class="loading-card">
      <div>
        <h3>${escapeHtml(title)}</h3>
        <p>${escapeHtml(message)}</p>
      </div>
    </div>
  `;
}

function setLoadingMessage(title, message) {
  const titleEl = elements.loadingCard.querySelector("h3");
  const messageEl = elements.loadingCard.querySelector("p");
  if (titleEl) titleEl.textContent = title;
  if (messageEl) messageEl.textContent = message;
}

function getCommitFingerprint(commits = []) {
  return commits.map((commit) => commit.sha).join("|");
}

function mergeSummaries(prevData, nextData) {
  if (!prevData) return nextData;
  const prevByDate = new Map(prevData.days.map((day) => [day.date, day]));
  const mergedDays = nextData.days.map((day) => {
    const prevDay = prevByDate.get(day.date);
    if (!day.summary && prevDay?.summary) {
      return {
        ...day,
        summary: prevDay.summary,
        summaryFingerprint: prevDay.summaryFingerprint,
        summaryStale: true,
      };
    }
    return day;
  });
  return { ...nextData, days: mergedDays };
}

function buildDayCard(day, index) {
  const aiEnabled = Boolean(state.data?.aiAvailable);
  const totalCommits = day.commits.length;
  const additions = day.commits.reduce(
    (total, commit) => total + (commit.stats?.additions || 0),
    0
  );
  const deletions = day.commits.reduce(
    (total, commit) => total + (commit.stats?.deletions || 0),
    0
  );
  const dayFingerprint = getCommitFingerprint(day.commits);
  const summaryFingerprint = day.summaryFingerprint || null;
  const isToday = day.date === new Date().toISOString().split("T")[0];
  const summaryStale =
    Boolean(day.summaryStale) ||
    Boolean(day.summary && summaryFingerprint && summaryFingerprint !== dayFingerprint);
  let statusText = "Disabled";
  if (day.summary) {
    if (summaryStale && isToday && aiEnabled) {
      statusText = "Refreshing";
    } else if (summaryStale) {
      statusText = "Outdated";
    } else {
      statusText = "Stored";
    }
  } else if (aiEnabled) {
    statusText = "Awaiting AI";
  }

  const authorsMap = new Map();
  day.commits.forEach((commit) => {
    const candidates = [
      {
        login: commit.authorLogin,
        avatar: commit.authorAvatar,
      },
      {
        login: commit.committerLogin,
        avatar: commit.committerAvatar,
      },
    ];
    candidates.forEach((user) => {
      if (!user.login || authorsMap.has(user.login)) return;
      authorsMap.set(user.login, {
        login: user.login,
        avatar:
          user.avatar ||
          `https://github.com/${user.login}.png?size=64`,
      });
    });
  });
  const authors = Array.from(authorsMap.values());
  const maxAvatars = 10;
  const visibleAuthors = authors.slice(0, maxAvatars);
  const overflowCount = Math.max(0, authors.length - maxAvatars);
  const avatarStack = visibleAuthors.length
    ? `
      <div class="avatar-stack">
        ${visibleAuthors
          .map(
            (author) => `
          <a class="avatar" href="https://github.com/${author.login}" target="_blank" rel="noreferrer" title="${author.login}">
            <img src="${author.avatar}" alt="${author.login}" loading="lazy" />
          </a>
        `
          )
          .join("")}
        ${
          overflowCount
            ? `<span class="avatar avatar-more">+${overflowCount}</span>`
            : ""
        }
      </div>
    `
    : "";
  return `
    <article class="day-card" id="day-${day.date}" style="--delay: ${index * 0.05}s">
      <div class="day-header">
        <div>
          <div class="day-title">${formatDate(day.date)}</div>
          <div class="day-meta">
            <span>${totalCommits} commits</span>
            <span><span class="delta-plus">+${additions}</span> / <span class="delta-minus">-${deletions}</span></span>
          </div>
        </div>
      </div>
      <div class="ai-box" data-ai-box="${day.date}">
        <div class="ai-header">
          <span>Commit summary</span>
          <span class="ai-status">${statusText}</span>
        </div>
        <div class="ai-content">
          ${
            day.summary
              ? buildSummaryHtml(day.summary)
              : `<p>${
                  aiEnabled
                    ? "🦞 Lobster log is cooking in the background."
                    : "AI summaries are disabled until the API key is configured."
                }</p>`
          }
        </div>
        ${avatarStack}
      </div>
      <div class="info-stack">
        <details class="info-card">
          <summary class="info-summary">
            <span class="info-left">
              <span class="info-caret" aria-hidden="true">▸</span>
              <span>${day.issues?.length || 0} new issues today</span>
            </span>
            <span class="info-count">Issues</span>
          </summary>
          <div class="info-body">
            <ul class="info-list">
              ${
                day.issues?.length
                  ? day.issues
                      .map(
                        (issue) =>
                          `<li><a href="${issue.url}" target="_blank" rel="noreferrer">#${issue.number} ${escapeHtml(
                            issue.title
                          )}</a></li>`
                      )
                      .join("")
                  : "<li>No new issues.</li>"
              }
            </ul>
          </div>
        </details>
        <details class="info-card">
          <summary class="info-summary">
            <span class="info-left">
              <span class="info-caret" aria-hidden="true">▸</span>
              <span>${day.pullRequests?.length || 0} new pull requests today</span>
            </span>
            <span class="info-count">PRs</span>
          </summary>
          <div class="info-body">
            <ul class="info-list">
              ${
                day.pullRequests?.length
                  ? day.pullRequests
                      .map(
                        (pr) =>
                          `<li><a href="${pr.url}" target="_blank" rel="noreferrer">#${pr.number} ${escapeHtml(
                            pr.title
                          )}</a></li>`
                      )
                      .join("")
                  : "<li>No new pull requests.</li>"
              }
            </ul>
          </div>
        </details>
      </div>
    </article>
  `;
}

function renderTimeline(data) {
  if (!data.days.length) {
    renderEmpty("Try expanding the date range or fetching more commits.");
    return;
  }

  elements.timeline.innerHTML = data.days
    .map((day, index) => buildDayCard(day, index))
    .join("");
}

function updateMeta(data) {
  elements.repoPill.textContent = `${data.repo} · ${data.branch}`;
  if (elements.repoLink) {
    elements.repoLink.href = `https://github.com/${data.repo}`;
  }
}

function applySyncPillStatus({ syncing, newData, lastSyncAt, error }) {
  const syncPill = elements.syncPill;
  if (!syncPill) return;
  if (newData) {
    syncPill.hidden = false;
    syncPill.textContent = "New data available — Reload";
    syncPill.className = "sync-pill ready";
    syncPill.dataset.state = "reload";
    return;
  }
  if (syncing) {
    syncPill.hidden = false;
    syncPill.textContent = "Syncing in background…";
    syncPill.className = "sync-pill syncing";
    syncPill.dataset.state = "";
    return;
  }
  if (error) {
    syncPill.hidden = false;
    syncPill.textContent = "Sync paused (error)";
    syncPill.className = "sync-pill stale";
    syncPill.dataset.state = "";
    return;
  }
  if (lastSyncAt) {
    syncPill.hidden = false;
    syncPill.textContent = `Up to date · ${formatTime(lastSyncAt)}`;
    syncPill.className = "sync-pill";
    syncPill.dataset.state = "";
    return;
  }
  syncPill.hidden = true;
}

function startStatusPolling() {
  if (state.statusTimer) return;
  state.statusTimer = setInterval(fetchStatus, 20000);
}

async function fetchStatus() {
  try {
    const response = await fetch(`/api/status?days=${state.days}`);
    if (!response.ok) return;
    const data = await response.json();
    const cache = data.cache || {};
    const issuesCache = cache.issues || {};
    const lastSyncAt = cache.lastSyncAt;
    const lastIssuesSyncAt = issuesCache.lastSyncAt;
    const newCommits =
      lastSyncAt &&
      state.lastSyncAt &&
      new Date(lastSyncAt) > new Date(state.lastSyncAt) &&
      Number(cache.lastSyncFetchedCount || 0) > 0;
    const newIssues =
      lastIssuesSyncAt &&
      state.lastIssuesSyncAt &&
      new Date(lastIssuesSyncAt) > new Date(state.lastIssuesSyncAt) &&
      Number(issuesCache.lastSyncFetchedCount || 0) > 0;
    const newData = Boolean(newCommits || newIssues);
    if (!newData) {
      state.lastSyncAt = lastSyncAt || state.lastSyncAt;
      state.lastIssuesSyncAt = lastIssuesSyncAt || state.lastIssuesSyncAt;
    }
    state.newDataAvailable = newData;
    applySyncPillStatus({
      syncing: Boolean(cache.syncing || issuesCache.syncing),
      newData,
      lastSyncAt: lastSyncAt || state.lastSyncAt,
      error: cache.error || issuesCache.error,
    });
  } catch {
    // Ignore status polling errors.
  }
}

async function loadCommits(force = false) {
  const showLoading = force || !state.data;
  if (showLoading) {
    elements.timeline.innerHTML = "";
    elements.timeline.appendChild(elements.loadingCard);
    elements.loadingCard.style.display = "flex";
    if (state.initializing && !force) {
      setLoadingMessage(
        "Syncing from GitHub…",
        "🦞 Syncing the initial lobster log. This can take a bit on first run."
      );
    } else {
      setLoadingMessage(
        "Pulling the latest commits…",
        "Hang tight while we build your daily brief."
      );
    }
  } else {
    elements.loadingCard.style.display = "none";
  }

  const params = new URLSearchParams({
    days: String(state.days),
  });
  if (force) {
    params.set("force", "1");
  }

  try {
    const response = await fetch(`/api/commits?${params.toString()}`);
    const data = await response.json();

    if (!response.ok) {
      renderEmpty(data.error || "Unable to load commits.");
      return;
    }

    state.data = mergeSummaries(state.data, data);
    state.initializing = Boolean(data.cache?.initializing);
    state.lastSyncAt = data.cache?.lastSyncAt || state.lastSyncAt;
    state.lastIssuesSyncAt =
      data.cache?.issues?.lastSyncAt || state.lastIssuesSyncAt;
    state.newDataAvailable = false;
    if (data.cache?.initializing && data.days.length === 0) {
      renderEmpty(
        "Syncing in the background. Refresh when you want the latest.",
        "No commits stored yet"
      );
      updateMeta(state.data);
      applySyncPillStatus({
        syncing: Boolean(data.cache?.syncing || data.cache?.issues?.syncing),
        newData: false,
        lastSyncAt: data.cache?.lastSyncAt || state.lastSyncAt,
        error: data.cache?.error || data.cache?.issues?.error,
      });
      startStatusPolling();
      return;
    }
    renderTimeline(state.data);
    updateMeta(state.data);
    applySyncPillStatus({
      syncing: Boolean(data.cache?.syncing || data.cache?.issues?.syncing),
      newData: false,
      lastSyncAt: data.cache?.lastSyncAt || state.lastSyncAt,
      error: data.cache?.error || data.cache?.issues?.error,
    });
    startStatusPolling();
  } catch (error) {
    renderEmpty(error.message || "Unable to load commits.");
  }
}

function buildSummaryHtml(summary) {
  if (!summary) {
    return "<p>Commit summary unavailable. Try again.</p>";
  }

  const highlightItems = summary.buckets || summary.highlights || [];
  const risks = summary.risks || [];

  const guessType = (text) => {
    const value = text.toLowerCase();
    if (value.includes("fix") || value.includes("bug")) return "fix";
    if (value.includes("refactor")) return "refactor";
    if (value.includes("doc")) return "docs";
    if (value.includes("test")) return "test";
    if (value.includes("chore")) return "chore";
    if (value.includes("feature") || value.includes("add")) return "feature";
    return "update";
  };

  const normalizeHighlight = (item) => {
    if (!item) return null;
    if (typeof item === "string") {
      return { type: guessType(item), what: item, where: "" };
    }
    const type = item.type || item.kind || item.category || guessType(item.what || "");
    const what = item.what || item.text || item.title || "";
    const where = item.where || item.area || item.path || "";
    return { type: String(type).toLowerCase(), what, where };
  };

  const rows = [];
  rows.push({
    type: "summary",
    text: summary.summary || "Summary unavailable.",
  });

  highlightItems.forEach((item) => {
    if (item && item.label && item.text) {
      rows.push({
        type: item.label.toLowerCase(),
        text: item.text,
      });
      return;
    }
    const normalized = normalizeHighlight(item);
    if (!normalized || !normalized.what) return;
    const whereText = normalized.where ? ` — ${normalized.where}` : "";
    rows.push({
      type: normalized.type || guessType(normalized.what),
      text: `${normalized.what}${whereText}`,
    });
  });

  risks.forEach((item) => {
    rows.push({ type: "risk", text: item });
  });

  return `
    <div class="ai-summary">
      <div class="ai-feed">
        ${rows
          .map(
            (row) => `
          <div class="ai-row">
            <span class="ai-tag ${row.type}">${row.type.toUpperCase()}</span>
            <span class="ai-text">${escapeHtml(row.text)}</span>
          </div>
        `
          )
          .join("")}
      </div>
    </div>
  `;
}

function renderSummary(target, summary) {
  target.innerHTML = buildSummaryHtml(summary);
}

async function summarizeDay(date, force = false) {
  if (!state.data?.aiAvailable) return;

  const day = state.data.days.find((entry) => entry.date === date);
  if (!day) return;

  const aiBox = document.querySelector(`[data-ai-box="${date}"]`);
  if (!aiBox) return;

  const aiStatus = aiBox.querySelector(".ai-status");
  const aiContent = aiBox.querySelector(".ai-content");
  const hadSummary = Boolean(day.summary);

  aiStatus.textContent = force ? "Refreshing…" : "Summarizing…";
  if (!hadSummary) {
    aiContent.innerHTML = "<p>🦞 Cooking the lobster log…</p>";
  }

  try {
    const response = await fetch("/api/summarize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        date,
        commits: day.commits,
        force,
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      aiStatus.textContent = "Failed";
      aiContent.innerHTML = `<p>${escapeHtml(
        data.error || "Commit summary failed."
      )}</p>`;
      return;
    }

    aiStatus.textContent =
      data.cache?.source === "sqlite" ? "Cached" : "Done";
    day.summary = data.summary;
    day.summaryFingerprint = getCommitFingerprint(day.commits);
    day.summaryStale = false;
    renderSummary(aiContent, data.summary);
  } catch (error) {
    aiStatus.textContent = "Failed";
    aiContent.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function summarizeAll(force = false) {
  if (!state.data?.aiAvailable) return;
  for (const day of state.data.days) {
    await summarizeDay(day.date, force);
  }
}

if (elements.syncPill) {
  elements.syncPill.addEventListener("click", () => {
    if (elements.syncPill.dataset.state === "reload") {
      loadCommits(false);
    }
  });
}

loadCommits();
