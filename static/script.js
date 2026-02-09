(function () {
  const $ = (s, ctx = document) => ctx.querySelector(s);
  const $$ = (s, ctx = document) => [...ctx.querySelectorAll(s)];

  const statusEl = $("#status");
  const brandSelect = $("#brand-select");
  const eventSelect = $("#event-select");
  const brandMatchBtn = $("#brand-match-btn");
  const eventMatchBtn = $("#event-match-btn");
  const matchList = $("#match-list");
  const resultsHeader = $("#results-header");
  const emptyState = $("#empty-state");
  const loadingIndicator = $("#loading-indicator");

  // Health check
  async function checkHealth() {
    try {
      const r = await fetch("/health");
      const data = await r.json();
      statusEl.textContent = "Live";
      statusEl.classList.toggle("connected", data.database === "connected");
      statusEl.classList.toggle("error", data.database !== "connected");
    } catch {
      statusEl.textContent = "Offline";
      statusEl.classList.remove("connected");
      statusEl.classList.add("error");
    }
  }

  // Load brands
  async function loadBrands() {
    try {
      const r = await fetch("/api/brands");
      if (!r.ok) throw new Error("Failed");
      const { brands } = await r.json();
      brandSelect.innerHTML = '<option value="">—</option>' +
        (brands || []).map((b) => `<option value="${b.id}">${escapeHtml(b.brand_name || "Unnamed")}</option>`).join("");
      brandSelect.disabled = false;
    } catch {
      brandSelect.innerHTML = '<option value="">Error loading</option>';
    }
  }

  // Load events
  async function loadEvents() {
    try {
      const r = await fetch("/api/events");
      if (!r.ok) throw new Error("Failed");
      const { events } = await r.json();
      eventSelect.innerHTML = '<option value="">—</option>' +
        (events || []).map((e) => `<option value="${e.id}">${escapeHtml(e.event_name || "Unnamed")}</option>`).join("");
      eventSelect.disabled = false;
    } catch {
      eventSelect.innerHTML = '<option value="">Error loading</option>';
    }
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function showMatches(matches, nameKey, countLabel) {
    if (!matches || matches.length === 0) {
      matchList.innerHTML = "";
      resultsHeader.textContent = "";
      emptyState.classList.remove("hidden");
      emptyState.querySelector("span:last-child").textContent = "No matches found";
      return;
    }

    emptyState.classList.add("hidden");
    resultsHeader.textContent = `${matches.length} ${countLabel}`;

    matchList.innerHTML = matches.map((m, i) => {
      const pct = Math.round(m.match_percentage || 0);
      const name = m[nameKey] || "—";
      const explanation = m.explanation || "No explanation available.";
      return `
        <article class="match-card" data-idx="${i}">
          <div class="match-main">
            <div class="match-info">
              <div class="match-name">${escapeHtml(name)}</div>
              <div class="match-bar-wrap"><div class="match-bar" style="width:${pct}%"></div></div>
            </div>
            <div class="match-actions">
              <span class="match-pct">${pct}%</span>
              <button class="match-toggle" aria-expanded="false" title="Show details">
                <svg class="chevron" width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3.5 5.25L7 8.75L10.5 5.25"/></svg>
              </button>
            </div>
          </div>
          <div class="match-details" hidden>${escapeHtml(explanation)}</div>
        </article>
      `;
    }).join("");

    matchList.querySelectorAll(".match-toggle").forEach((btn) => {
      btn.addEventListener("click", () => {
        const card = btn.closest(".match-card");
        const details = card.querySelector(".match-details");
        const open = !details.hidden;
        details.hidden = open;
        btn.setAttribute("aria-expanded", !open);
        btn.classList.toggle("open", !open);
      });
    });
  }

  function showLoading() {
    loadingIndicator.classList.remove("hidden");
    matchList.innerHTML = "";
    matchList.classList.add("hidden");
    resultsHeader.textContent = "";
    emptyState.classList.add("hidden");
  }

  function hideLoading() {
    loadingIndicator.classList.add("hidden");
    matchList.classList.remove("hidden");
  }

  function updateEmptyState(msg) {
    matchList.innerHTML = "";
    resultsHeader.textContent = "";
    emptyState.classList.remove("hidden");
    emptyState.querySelector("span:last-child").textContent = msg;
  }

  async function fetchBrandMatches() {
    const id = brandSelect.value;
    if (!id) {
      updateEmptyState("Select a brand above");
      return;
    }
    showLoading();
    try {
      const r = await fetch(`/api/brands/${id}/matches`);
      if (!r.ok) throw new Error("Failed");
      const data = await r.json();
      showMatches(data.matches, "event_name", "events");
    } catch {
      updateEmptyState("Error loading matches");
    }
    hideLoading();
  }

  async function fetchEventMatches() {
    const id = eventSelect.value;
    if (!id) {
      updateEmptyState("Select an event above");
      return;
    }
    showLoading();
    try {
      const r = await fetch(`/api/events/${id}/matches`);
      if (!r.ok) throw new Error("Failed");
      const data = await r.json();
      showMatches(data.matches, "brand_name", "brands");
    } catch {
      updateEmptyState("Error loading matches");
    }
    hideLoading();
  }

  // Tab switch
  function setMode(mode) {
    const brandPicker = $(".picker[data-mode='brand']");
    const eventPicker = $(".picker[data-mode='event']");
    const tabs = $$(".tab");

    tabs.forEach((t) => t.classList.toggle("active", t.dataset.mode === mode));
    brandPicker.classList.toggle("hidden", mode !== "brand");
    eventPicker.classList.toggle("hidden", mode !== "event");

    brandSelect.value = "";
    eventSelect.value = "";
    brandMatchBtn.disabled = true;
    eventMatchBtn.disabled = true;
    updateEmptyState("Select above, then click Match");
  }

  function onBrandSelect() {
    brandMatchBtn.disabled = !brandSelect.value;
    updateEmptyState("Select above, then click Match");
  }

  function onEventSelect() {
    eventMatchBtn.disabled = !eventSelect.value;
    updateEmptyState("Select above, then click Match");
  }

  // Init
  checkHealth();
  setInterval(checkHealth, 30000);
  loadBrands();
  loadEvents();

  $$(".tab").forEach((t) => {
    t.addEventListener("click", () => setMode(t.dataset.mode));
  });
  brandSelect.addEventListener("change", onBrandSelect);
  eventSelect.addEventListener("change", onEventSelect);
  brandMatchBtn.addEventListener("click", fetchBrandMatches);
  eventMatchBtn.addEventListener("click", fetchEventMatches);

  updateEmptyState("Select above, then click Match");
})();
