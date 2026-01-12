const state = {
  days: 7,
  data: null,
};

const elements = {
  timeline: document.getElementById("timeline"),
  loadingCard: document.getElementById("loadingCard"),
  repoPill: document.getElementById("repoPill"),
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

function renderEmpty(message) {
  elements.timeline.innerHTML = `
    <div class="loading-card">
      <div>
        <h3>No commits found</h3>
        <p>${escapeHtml(message)}</p>
      </div>
    </div>
  `;
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
  return `
    <article class="day-card" style="--delay: ${index * 0.05}s">
      <div class="day-header">
        <div>
          <div class="day-title">${formatDate(day.date)}</div>
          <div class="day-meta">
            <span>${totalCommits} commits</span>
            <span>+${additions} / -${deletions}</span>
          </div>
        </div>
        <div class="ai-actions">
          <button
            class="btn ghost"
            data-action="regenerate-day"
            data-date="${day.date}"
            ${aiEnabled ? "" : "disabled"}
          >
            Regenerate
          </button>
        </div>
      </div>
      <div class="ai-box" data-ai-box="${day.date}">
        <div class="ai-header">
          <span>Commit summary</span>
          <span class="ai-status">${aiEnabled ? "Awaiting AI" : "Disabled"}</span>
        </div>
        <div class="ai-content">
          <p>
            ${
              aiEnabled
                ? "🦞 Generate the lobster log for this day’s changes."
                : "AI summaries are disabled until the API key is configured."
            }
          </p>
        </div>
      </div>
      <div class="info-stack">
        <details class="info-card">
          <summary class="info-summary">
            <span>${day.issues?.length || 0} new issues today</span>
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
            <span>${day.pullRequests?.length || 0} new pull requests today</span>
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
}

async function loadCommits(force = false) {
  elements.timeline.innerHTML = "";
  elements.timeline.appendChild(elements.loadingCard);
  elements.loadingCard.style.display = "flex";

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

    state.data = data;
    renderTimeline(data);
    updateMeta(data);
    if (data.aiAvailable) {
      summarizeAll();
    }
  } catch (error) {
    renderEmpty(error.message || "Unable to load commits.");
  }
}

function renderSummary(target, summary) {
  if (!summary) {
    target.innerHTML = "<p>Commit summary unavailable. Try again.</p>";
    return;
  }

  const highlightItems = summary.highlights || [];
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

  const rows = [];
  rows.push({
    type: "summary",
    text: summary.summary || "Summary unavailable.",
  });

  highlightItems.forEach((item) => {
    rows.push({ type: guessType(item), text: item });
  });

  risks.forEach((item) => {
    rows.push({ type: "risk", text: item });
  });

  target.innerHTML = `
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

async function summarizeDay(date, force = false) {
  if (!state.data?.aiAvailable) return;

  const day = state.data.days.find((entry) => entry.date === date);
  if (!day) return;

  const aiBox = document.querySelector(`[data-ai-box="${date}"]`);
  if (!aiBox) return;

  const aiStatus = aiBox.querySelector(".ai-status");
  const aiContent = aiBox.querySelector(".ai-content");

  aiStatus.textContent = force ? "Refreshing…" : "Summarizing…";
  aiContent.innerHTML = "<p>🦞 Cooking the lobster log…</p>";

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

elements.timeline.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  if (action === "regenerate-day") {
    summarizeDay(button.dataset.date, true);
  }
});

loadCommits();
