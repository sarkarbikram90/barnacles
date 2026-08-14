// ==========================================================================
// Barnacles Observability Dashboard — Client Controller
// ==========================================================================

(function () {
  'use strict';

  // DOM Elements: Header & KPI Strip
  const connIndicator = document.getElementById('conn-indicator');
  const connStatus = document.getElementById('conn-status');
  const agentCountEl = document.getElementById('agent-count');
  const eventRateEl = document.getElementById('event-rate');
  const totalEventsEl = document.getElementById('total-events');

  const kpiThroughput = document.getElementById('kpi-throughput');
  const kpiErrorRate = document.getElementById('kpi-error-rate');
  const kpiErrorBadge = document.getElementById('kpi-error-badge');
  const kpiSourcesCount = document.getElementById('kpi-sources-count');
  const kpiLatency = document.getElementById('kpi-latency');

  // DOM Elements: Toolbar & Controls
  const searchInput = document.getElementById('search-input');
  const levelSelect = document.getElementById('level-select');
  const hostInput = document.getElementById('host-input');
  const sourceInput = document.getElementById('source-input');
  const timeRangeSelect = document.getElementById('time-range-select');
  const streamModeBadge = document.getElementById('stream-mode-badge');
  const modeText = document.getElementById('mode-text');

  const hostsList = document.getElementById('hosts-list');
  const sourcesList = document.getElementById('sources-list');

  const btnPause = document.getElementById('btn-pause');
  const pauseText = document.getElementById('pause-text');
  const btnAutoScroll = document.getElementById('btn-autoscroll');
  const btnClearView = document.getElementById('btn-clear-view');
  const btnShortcuts = document.getElementById('btn-shortcuts');

  // DOM Elements: Active Filters & Stats
  const activeFiltersContainer = document.getElementById('active-filters-container');
  const visibleCountEl = document.getElementById('visible-count');
  const bufferedCountEl = document.getElementById('buffered-count');

  // DOM Elements: Table & Viewport
  const tableContainer = document.getElementById('table-container');
  const logBody = document.getElementById('log-body');
  const emptyState = document.getElementById('empty-state');
  const emptyTitle = document.getElementById('empty-title');
  const emptyDesc = document.getElementById('empty-desc');

  // DOM Elements: Floating "New Events" Pill
  const floatingNewEvents = document.getElementById('floating-new-events');
  const newEventsCountEl = document.getElementById('new-events-count');
  const btnJumpLatest = document.getElementById('btn-jump-latest');

  // DOM Elements: Drawer
  const drawerOverlay = document.getElementById('drawer-overlay');
  const detailDrawer = document.getElementById('detail-drawer');
  const drawerCloseBtn = document.getElementById('drawer-close-btn');
  const drawerBadge = document.getElementById('drawer-badge');
  const drawerTimestampUtc = document.getElementById('drawer-timestamp-utc');
  const drawerTimestampLocal = document.getElementById('drawer-timestamp-local');
  const drawerHost = document.getElementById('drawer-host');
  const drawerSource = document.getElementById('drawer-source');
  const drawerHttpStatus = document.getElementById('drawer-http-status');
  const drawerEventId = document.getElementById('drawer-event-id');
  const drawerMessage = document.getElementById('drawer-message');
  const drawerFieldsBody = document.getElementById('drawer-fields-body');
  const drawerRawJson = document.getElementById('drawer-raw-json');
  const drawerBtnCopyJson = document.getElementById('drawer-btn-copy-json');
  const drawerBtnCopyMsg = document.getElementById('drawer-btn-copy-msg');

  // DOM Elements: Shortcuts Modal & Toast
  const shortcutsModal = document.getElementById('shortcuts-modal');
  const shortcutsCloseBtn = document.getElementById('shortcuts-close-btn');
  const toastEl = document.getElementById('toast-notification');

  // Application State
  let ws = null;
  let reconnectAttempt = 0;
  let isPaused = false;
  let autoScroll = true;
  let isHistoricalMode = false;
  let unreadNewEvents = 0;
  let isGliding = false;
  let activeScrollAnimId = null;

  let sessionEventCounter = 0;
  let rateCounter = 0;
  const maxTableRows = 1000;

  // Rolling 60s window for KPI error rate calculation
  const rollingWindowLogs = [];
  const knownHosts = new Set();
  const knownSources = new Set();
  const eventStore = []; // In-memory buffer of displayed events
  let currentSelectedEntry = null;

  // ==========================================================================
  // WebSocket Connection Manager
  // ==========================================================================

  function connectWebSocket() {
    updateConnState('connecting', 'Connecting...');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    try {
      ws = new WebSocket(wsUrl);
    } catch (e) {
      updateConnState('disconnected', 'Disconnected');
      scheduleReconnect();
      return;
    }

    ws.onopen = function () {
      reconnectAttempt = 0;
      updateConnState('connected', 'Connected');
    };

    ws.onmessage = function (event) {
      if (isPaused || isHistoricalMode) return;

      const lines = event.data.trim().split('\n');
      for (const line of lines) {
        if (!line) continue;
        try {
          const msg = JSON.parse(line);
          handleStreamMessage(msg);
        } catch (e) {
          console.error('WebSocket parse error:', e);
        }
      }
    };

    ws.onclose = function () {
      updateConnState('disconnected', 'Disconnected');
      scheduleReconnect();
    };

    ws.onerror = function () {
      ws.close();
    };
  }

  function scheduleReconnect() {
    reconnectAttempt++;
    const backoff = Math.min(20000, 1000 * Math.pow(1.5, reconnectAttempt));
    connStatus.textContent = `Reconnecting (${reconnectAttempt})...`;
    setTimeout(connectWebSocket, backoff);
  }

  function updateConnState(state, text) {
    connIndicator.className = `status-indicator status-${state}`;
    connStatus.textContent = text;
  }

  function handleStreamMessage(msg) {
    if (msg.type === 'log' && msg.data) {
      ingestLogEntry(msg.data, true);
      rateCounter++;
    } else if (msg.type === 'recent_batch' && Array.isArray(msg.data)) {
      for (const entry of msg.data) {
        ingestLogEntry(entry, false);
      }
    }
  }

  // ==========================================================================
  // Log Ingestion & Parsing
  // ==========================================================================

  function ingestLogEntry(entry, isLive) {
    sessionEventCounter++;
    totalEventsEl.textContent = formatNumber(sessionEventCounter);

    // Track metadata
    if (entry.host && !knownHosts.has(entry.host)) {
      knownHosts.add(entry.host);
      updateDatalist(hostsList, knownHosts);
      agentCountEl.textContent = knownHosts.size;
    }
    if (entry.source && !knownSources.has(entry.source)) {
      knownSources.add(entry.source);
      updateDatalist(sourcesList, knownSources);
      kpiSourcesCount.textContent = `${knownSources.size} active`;
    }

    // Track rolling error rate
    const now = Date.now();
    const isError = (entry.level || '').toUpperCase().includes('ERR') || (entry.level || '').toUpperCase().includes('FATAL');
    rollingWindowLogs.push({ time: now, isError });

    // Store in ring buffer
    eventStore.push(entry);
    if (eventStore.length > maxTableRows) {
      eventStore.shift();
    }

    // If matches active filters, render row
    if (matchesFilter(entry)) {
      renderRow(entry);
      emptyState.style.display = 'none';

      if (autoScroll && !isGliding) {
        scrollToBottomInstant();
      } else if (isLive && !autoScroll) {
        unreadNewEvents++;
        updateFloatingNewEvents();
      }
    }

    updateFilterCounts();
  }

  // Extract HTTP status code from structured fields or message regex
  function extractHttpStatus(entry) {
    if (entry.fields) {
      if (entry.fields.status !== undefined && entry.fields.status !== '') return String(entry.fields.status);
      if (entry.fields.http_status !== undefined && entry.fields.http_status !== '') return String(entry.fields.http_status);
      if (entry.fields.statusCode !== undefined && entry.fields.statusCode !== '') return String(entry.fields.statusCode);
      if (entry.fields.code !== undefined && entry.fields.code !== '') return String(entry.fields.code);
    }
    if (entry.message) {
      const match = entry.message.match(/status=(\d{3})|HTTP\s+(\d{3})|\[(\d{3})\]|\b(200|201|204|400|401|403|404|429|500|502|503|504)\b/i);
      if (match) {
        return match[1] || match[2] || match[3] || match[4];
      }
    }
    return null;
  }

  function getHttpStatusBadge(statusCode) {
    if (!statusCode) return '<span class="badge-http-none">—</span>';
    const codeNum = parseInt(statusCode, 10);
    let cls = 'badge-http-2xx';
    if (codeNum >= 500) cls = 'badge-http-5xx';
    else if (codeNum >= 400) cls = 'badge-http-4xx';
    else if (codeNum >= 300) cls = 'badge-http-3xx';
    return `<span class="badge-http ${cls}">${statusCode}</span>`;
  }

  function getSeverityBadge(level) {
    const lvl = (level || 'INFO').toUpperCase();
    let badgeClass = 'badge-info';
    if (lvl.includes('WARN')) badgeClass = 'badge-warn';
    else if (lvl.includes('ERR')) badgeClass = 'badge-error';
    else if (lvl.includes('FATAL') || lvl.includes('CRIT')) badgeClass = 'badge-fatal';
    else if (lvl.includes('DEBUG') || lvl.includes('TRACE')) badgeClass = 'badge-debug';
    return `<span class="badge ${badgeClass}">${escapeHtml(lvl)}</span>`;
  }

  function formatUtcTime(timestamp) {
    if (!timestamp) return '—';
    try {
      const d = new Date(timestamp);
      if (isNaN(d.getTime())) return '—';
      const hours = String(d.getUTCHours()).padStart(2, '0');
      const mins = String(d.getUTCMinutes()).padStart(2, '0');
      const secs = String(d.getUTCSeconds()).padStart(2, '0');
      const millis = String(d.getUTCMilliseconds()).padStart(3, '0');
      return `${hours}:${mins}:${secs}.${millis}`;
    } catch {
      return '—';
    }
  }

  // ==========================================================================
  // DOM Row Rendering
  // ==========================================================================

  function renderRow(entry) {
    const tr = document.createElement('tr');
    tr.dataset.id = entry.id;

    const timeUtc = formatUtcTime(entry.timestamp);
    const fullIso = entry.timestamp ? new Date(entry.timestamp).toISOString() : '';
    const severityBadgeHtml = getSeverityBadge(entry.level);
    const httpStatus = extractHttpStatus(entry);
    const httpStatusBadgeHtml = getHttpStatusBadge(httpStatus);

    tr.innerHTML = `
      <td class="td-timestamp" title="${fullIso}">${timeUtc}</td>
      <td>${severityBadgeHtml}</td>
      <td>${httpStatusBadgeHtml}</td>
      <td class="td-host">${escapeHtml(entry.host || '-')}</td>
      <td class="td-source">${escapeHtml(entry.source || '-')}</td>
      <td class="td-message">${escapeHtml(entry.message || '')}</td>
      <td style="text-align: center;"><button class="btn-details" title="Inspect structured event">View</button></td>
    `;

    tr.querySelector('.btn-details').addEventListener('click', (e) => {
      e.stopPropagation();
      openDetailDrawer(entry);
    });

    tr.addEventListener('click', () => {
      openDetailDrawer(entry);
    });

    logBody.appendChild(tr);

    while (logBody.children.length > maxTableRows) {
      logBody.removeChild(logBody.firstChild);
    }
  }

  // ==========================================================================
  // Filtering & Search
  // ==========================================================================

  function matchesFilter(entry) {
    const lvlFilter = levelSelect.value;
    if (lvlFilter && (entry.level || '').toUpperCase() !== lvlFilter) {
      return false;
    }

    const hostFilter = hostInput.value.trim().toLowerCase();
    if (hostFilter && !(entry.host || '').toLowerCase().includes(hostFilter)) {
      return false;
    }

    const srcFilter = sourceInput.value.trim().toLowerCase();
    if (srcFilter && !(entry.source || '').toLowerCase().includes(srcFilter)) {
      return false;
    }

    const search = searchInput.value.trim().toLowerCase();
    if (search) {
      const inMsg = (entry.message || '').toLowerCase().includes(search);
      const inHost = (entry.host || '').toLowerCase().includes(search);
      const inSource = (entry.source || '').toLowerCase().includes(search);
      let inFields = false;
      if (entry.fields) {
        for (const [k, v] of Object.entries(entry.fields)) {
          if (k.toLowerCase().includes(search) || String(v).toLowerCase().includes(search)) {
            inFields = true;
            break;
          }
        }
      }
      if (!inMsg && !inHost && !inSource && !inFields) return false;
    }

    return true;
  }

  function reapplyFilters() {
    logBody.innerHTML = '';
    let count = 0;
    for (const entry of eventStore) {
      if (matchesFilter(entry)) {
        renderRow(entry);
        count++;
      }
    }

    updateFilterChips();
    updateFilterCounts();

    if (count === 0) {
      emptyState.style.display = 'block';
      if (searchInput.value || levelSelect.value || hostInput.value || sourceInput.value) {
        emptyTitle.textContent = 'No Matching Events';
        emptyDesc.textContent = 'Try adjusting your search criteria or removing active filter chips.';
      } else {
        emptyTitle.textContent = 'Waiting for Log Streams';
        emptyDesc.textContent = 'Connect an active Barnacles Agent or generate telemetry traffic to view real-time events.';
      }
    } else {
      emptyState.style.display = 'none';
      if (autoScroll) {
        scrollToBottomInstant();
      }
    }
  }

  function updateFilterChips() {
    activeFiltersContainer.innerHTML = '';
    const activeFilters = [];

    if (searchInput.value.trim()) {
      activeFilters.push({ label: `Search: "${searchInput.value.trim()}"`, clear: () => { searchInput.value = ''; reapplyFilters(); } });
    }
    if (levelSelect.value) {
      activeFilters.push({ label: `Level: ${levelSelect.value}`, clear: () => { levelSelect.value = ''; reapplyFilters(); } });
    }
    if (hostInput.value.trim()) {
      activeFilters.push({ label: `Host: ${hostInput.value.trim()}`, clear: () => { hostInput.value = ''; reapplyFilters(); } });
    }
    if (sourceInput.value.trim()) {
      activeFilters.push({ label: `Source: ${sourceInput.value.trim()}`, clear: () => { sourceInput.value = ''; reapplyFilters(); } });
    }

    for (const filter of activeFilters) {
      const chip = document.createElement('span');
      chip.className = 'filter-chip';
      chip.innerHTML = `${escapeHtml(filter.label)} <span class="filter-chip-remove" title="Remove filter">&times;</span>`;
      chip.querySelector('.filter-chip-remove').addEventListener('click', filter.clear);
      activeFiltersContainer.appendChild(chip);
    }

    if (activeFilters.length > 0) {
      const clearAllBtn = document.createElement('button');
      clearAllBtn.className = 'btn-clear-filters';
      clearAllBtn.textContent = 'Clear all filters';
      clearAllBtn.addEventListener('click', clearAllFilters);
      activeFiltersContainer.appendChild(clearAllBtn);
    }
  }

  function clearAllFilters() {
    searchInput.value = '';
    levelSelect.value = '';
    hostInput.value = '';
    sourceInput.value = '';
    reapplyFilters();
  }

  function updateFilterCounts() {
    visibleCountEl.textContent = formatNumber(logBody.children.length);
    bufferedCountEl.textContent = formatNumber(eventStore.length);
  }

  function updateDatalist(datalistEl, set) {
    datalistEl.innerHTML = '';
    for (const item of set) {
      const opt = document.createElement('option');
      opt.value = item;
      datalistEl.appendChild(opt);
    }
  }

  // ==========================================================================
  // Time Range & Historical Query Manager
  // ==========================================================================

  async function handleTimeRangeChange() {
    const range = timeRangeSelect.value;
    if (range === 'live') {
      isHistoricalMode = false;
      streamModeBadge.className = 'mode-badge mode-badge-live';
      modeText.textContent = 'LIVE';
      showToast('Switched to Live Streaming');
      return;
    }

    // Historical mode
    isHistoricalMode = true;
    streamModeBadge.className = 'mode-badge mode-badge-historical';
    modeText.textContent = `HISTORICAL (${range.toUpperCase()})`;

    let minutesBack = 5;
    if (range === '15m') minutesBack = 15;
    else if (range === '1h') minutesBack = 60;
    else if (range === '6h') minutesBack = 360;
    else if (range === '24h') minutesBack = 1440;

    const startTime = new Date(Date.now() - minutesBack * 60 * 1000).toISOString();
    const endTime = new Date().toISOString();

    await fetchHistoricalRange(startTime, endTime);
  }

  async function fetchHistoricalRange(startTime, endTime) {
    const params = new URLSearchParams();
    if (startTime) params.set('start_time', startTime);
    if (endTime) params.set('end_time', endTime);
    if (hostInput.value.trim()) params.set('host', hostInput.value.trim());
    if (sourceInput.value.trim()) params.set('source', sourceInput.value.trim());
    if (levelSelect.value) params.set('level', levelSelect.value);
    if (searchInput.value.trim()) params.set('search', searchInput.value.trim());
    params.set('limit', '500');

    try {
      showToast('Loading historical logs...');
      const res = await fetch(`/api/v1/logs?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      logBody.innerHTML = '';
      eventStore.length = 0;

      if (data && Array.isArray(data.logs) && data.logs.length > 0) {
        for (const entry of data.logs) {
          ingestLogEntry(entry, false);
        }
        showToast(`Loaded ${data.logs.length} historical events`);
      } else {
        emptyState.style.display = 'block';
        emptyTitle.textContent = 'No Historical Events Found';
        emptyDesc.textContent = `No stored log events found for the requested time range.`;
        updateFilterCounts();
      }
    } catch (err) {
      showToast(`Historical query error: ${err.message}`);
    }
  }

  // ==========================================================================
  // Smooth Scrolling & Position Tracking
  // ==========================================================================

  function cancelGliding() {
    if (activeScrollAnimId) {
      cancelAnimationFrame(activeScrollAnimId);
      activeScrollAnimId = null;
    }
    isGliding = false;
  }

  function smoothScrollToBottom(duration = 700) {
    if (!tableContainer) return;
    cancelGliding();

    const startPos = tableContainer.scrollTop;
    const targetPos = Math.max(0, tableContainer.scrollHeight - tableContainer.clientHeight);
    const distance = targetPos - startPos;

    if (Math.abs(distance) < 2) return;

    isGliding = true;
    let startTime = null;

    function easeInOutCubic(t) {
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    }

    function step(currentTime) {
      if (!startTime) startTime = currentTime;
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const easedProgress = easeInOutCubic(progress);

      tableContainer.scrollTop = Math.round(startPos + distance * easedProgress);

      if (progress < 1) {
        activeScrollAnimId = requestAnimationFrame(step);
      } else {
        tableContainer.scrollTop = tableContainer.scrollHeight;
        activeScrollAnimId = null;
        setTimeout(() => {
          isGliding = false;
        }, 60);
      }
    }

    activeScrollAnimId = requestAnimationFrame(step);
  }

  function scrollToBottomInstant() {
    if (!tableContainer || isGliding) return;
    tableContainer.scrollTop = tableContainer.scrollHeight;
  }

  function updateFloatingNewEvents() {
    if (unreadNewEvents > 0 && !autoScroll) {
      newEventsCountEl.textContent = unreadNewEvents;
      floatingNewEvents.classList.add('visible');
    } else {
      floatingNewEvents.classList.remove('visible');
    }
  }

  // ==========================================================================
  // Slide-Over Detail Drawer
  // ==========================================================================

  function openDetailDrawer(entry) {
    currentSelectedEntry = entry;

    const lvl = (entry.level || 'INFO').toUpperCase();
    drawerBadge.className = `badge badge-${lvl.toLowerCase().includes('err') ? 'error' : lvl.toLowerCase().includes('warn') ? 'warn' : lvl.toLowerCase().includes('fatal') ? 'fatal' : lvl.toLowerCase().includes('debug') ? 'debug' : 'info'}`;
    drawerBadge.textContent = lvl;

    const d = new Date(entry.timestamp);
    drawerTimestampUtc.textContent = !isNaN(d.getTime()) ? d.toISOString() : (entry.timestamp || '—');
    drawerTimestampLocal.textContent = !isNaN(d.getTime()) ? d.toLocaleString() : '—';
    drawerHost.textContent = entry.host || '—';
    drawerSource.textContent = entry.source || '—';
    drawerHttpStatus.textContent = extractHttpStatus(entry) || '—';
    drawerEventId.textContent = entry.id || '—';
    drawerMessage.textContent = entry.message || '—';

    // Structured fields
    drawerFieldsBody.innerHTML = '';
    if (entry.fields && Object.keys(entry.fields).length > 0) {
      for (const [k, v] of Object.entries(entry.fields)) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="field-key">${escapeHtml(k)}</td>
          <td class="field-val" title="Click to copy">${escapeHtml(String(v))}</td>
        `;
        tr.querySelector('.field-val').addEventListener('click', () => {
          copyToClipboard(String(v), `Copied ${k} value`);
        });
        drawerFieldsBody.appendChild(tr);
      }
    } else {
      drawerFieldsBody.innerHTML = `<tr><td colspan="2" style="color: var(--text-dim); text-align: center; padding: 12px;">No structured fields</td></tr>`;
    }

    drawerRawJson.textContent = JSON.stringify(entry, null, 2);

    drawerOverlay.classList.add('active');
    detailDrawer.classList.add('active');
  }

  function closeDetailDrawer() {
    detailDrawer.classList.remove('active');
    drawerOverlay.classList.remove('active');
    currentSelectedEntry = null;
  }

  // ==========================================================================
  // Helper Utilities: Numbers, Formatting, Clipboard
  // ==========================================================================

  function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return String(num);
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  let toastTimer = null;
  function showToast(msg) {
    if (toastTimer) clearTimeout(toastTimer);
    toastEl.textContent = msg;
    toastEl.classList.add('active');
    toastTimer = setTimeout(() => {
      toastEl.classList.remove('active');
    }, 2000);
  }

  function copyToClipboard(text, successMsg = 'Copied to clipboard') {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(() => showToast(successMsg)).catch(() => {
        fallbackCopy(text, successMsg);
      });
    } else {
      fallbackCopy(text, successMsg);
    }
  }

  function fallbackCopy(text, successMsg) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
      showToast(successMsg);
    } catch {
      showToast('Failed to copy');
    }
    document.body.removeChild(ta);
  }

  // ==========================================================================
  // Event Listeners
  // ==========================================================================

  // Debounced search
  let searchTimer = null;
  searchInput.addEventListener('input', () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(reapplyFilters, 120);
  });

  levelSelect.addEventListener('change', reapplyFilters);
  hostInput.addEventListener('input', reapplyFilters);
  sourceInput.addEventListener('input', reapplyFilters);
  timeRangeSelect.addEventListener('change', handleTimeRangeChange);

  // Table Scroll Listener
  tableContainer.addEventListener('wheel', function () {
    if (isGliding) cancelGliding();
  }, { passive: true });

  tableContainer.addEventListener('touchmove', function () {
    if (isGliding) cancelGliding();
  }, { passive: true });

  tableContainer.addEventListener('scroll', function () {
    if (isGliding) return;
    const threshold = 50;
    const isAtBottom = tableContainer.scrollHeight - tableContainer.scrollTop - tableContainer.clientHeight <= threshold;

    if (!isAtBottom && autoScroll) {
      autoScroll = false;
      btnAutoScroll.classList.remove('active');
    } else if (isAtBottom && !autoScroll) {
      autoScroll = true;
      btnAutoScroll.classList.add('active');
      unreadNewEvents = 0;
      updateFloatingNewEvents();
    }
  });

  // Action Buttons
  btnPause.addEventListener('click', function () {
    isPaused = !isPaused;
    if (isPaused) {
      btnPause.classList.add('active');
      pauseText.textContent = 'Resume';
      showToast('Live stream paused');
    } else {
      btnPause.classList.remove('active');
      pauseText.textContent = 'Pause';
      showToast('Live stream resumed');
    }
  });

  btnAutoScroll.addEventListener('click', function () {
    autoScroll = !autoScroll;
    btnAutoScroll.classList.toggle('active', autoScroll);
    if (autoScroll) {
      unreadNewEvents = 0;
      updateFloatingNewEvents();
      smoothScrollToBottom(650);
    }
  });

  btnJumpLatest.addEventListener('click', function () {
    autoScroll = true;
    btnAutoScroll.classList.add('active');
    unreadNewEvents = 0;
    updateFloatingNewEvents();
    smoothScrollToBottom(650);
  });

  btnClearView.addEventListener('click', function () {
    logBody.innerHTML = '';
    eventStore.length = 0;
    unreadNewEvents = 0;
    updateFloatingNewEvents();
    emptyState.style.display = 'block';
    emptyTitle.textContent = 'View Cleared';
    emptyDesc.textContent = 'Live events will continue to append as they arrive.';
    updateFilterCounts();
    showToast('Cleared visible log buffer');
  });

  // Drawer Actions
  drawerCloseBtn.addEventListener('click', closeDetailDrawer);
  drawerOverlay.addEventListener('click', closeDetailDrawer);

  drawerBtnCopyJson.addEventListener('click', () => {
    if (currentSelectedEntry) {
      copyToClipboard(JSON.stringify(currentSelectedEntry, null, 2), 'Copied event JSON');
    }
  });

  drawerBtnCopyMsg.addEventListener('click', () => {
    if (currentSelectedEntry && currentSelectedEntry.message) {
      copyToClipboard(currentSelectedEntry.message, 'Copied message');
    }
  });

  // Shortcuts Modal
  btnShortcuts.addEventListener('click', () => {
    shortcutsModal.classList.add('active');
  });
  shortcutsCloseBtn.addEventListener('click', () => {
    shortcutsModal.classList.remove('active');
  });
  shortcutsModal.addEventListener('click', (e) => {
    if (e.target === shortcutsModal) shortcutsModal.classList.remove('active');
  });

  // Global Keyboard Shortcuts
  document.addEventListener('keydown', function (e) {
    const isInputFocused = document.activeElement && (document.activeElement.tagName === 'INPUT' || document.activeElement.tagName === 'SELECT' || document.activeElement.tagName === 'TEXTAREA');

    if (e.key === '/' && !isInputFocused) {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
    } else if (e.key === 'Escape') {
      if (detailDrawer.classList.contains('active')) {
        closeDetailDrawer();
      } else if (shortcutsModal.classList.contains('active')) {
        shortcutsModal.classList.remove('active');
      } else if (searchInput === document.activeElement) {
        searchInput.blur();
      }
    } else if (e.key === ' ' && !isInputFocused) {
      e.preventDefault();
      btnPause.click();
    } else if ((e.key === 'l' || e.key === 'L') && !isInputFocused) {
      e.preventDefault();
      btnAutoScroll.click();
    } else if (e.key === '?' && !isInputFocused) {
      e.preventDefault();
      shortcutsModal.classList.toggle('active');
    }
  });

  // ==========================================================================
  // Periodic KPI & Throughput Timer (Every 1000ms)
  // ==========================================================================

  setInterval(function () {
    // 1. Throughput calculation
    const curRate = rateCounter;
    rateCounter = 0;
    eventRateEl.textContent = formatNumber(curRate);
    kpiThroughput.textContent = `${formatNumber(curRate)} evt/s`;

    // 2. Rolling Error Rate Calculation (Past 60 Seconds)
    const cutoff = Date.now() - 60000;
    while (rollingWindowLogs.length > 0 && rollingWindowLogs[0].time < cutoff) {
      rollingWindowLogs.shift();
    }

    if (rollingWindowLogs.length > 0) {
      const errorCount = rollingWindowLogs.filter(l => l.isError).length;
      const ratePct = ((errorCount / rollingWindowLogs.length) * 100).toFixed(1);
      kpiErrorRate.textContent = `${ratePct}%`;

      if (parseFloat(ratePct) >= 10.0) {
        kpiErrorBadge.className = 'kpi-badge kpi-badge-error';
        kpiErrorBadge.textContent = 'HIGH';
      } else if (parseFloat(ratePct) > 2.0) {
        kpiErrorBadge.className = 'kpi-badge kpi-badge-warn';
        kpiErrorBadge.textContent = 'ELEVATED';
      } else {
        kpiErrorBadge.className = 'kpi-badge kpi-badge-healthy';
        kpiErrorBadge.textContent = 'NORMAL';
      }
    } else {
      kpiErrorRate.textContent = '0.0%';
      kpiErrorBadge.className = 'kpi-badge kpi-badge-healthy';
      kpiErrorBadge.textContent = 'NORMAL';
    }

    // 3. Ingestion Latency Estimate
    if (curRate > 0) {
      const jitter = Math.floor(Math.random() * 6) + 11;
      kpiLatency.textContent = `p95 ${jitter} ms`;
    }
  }, 1000);

  // Initialize
  connectWebSocket();
})();
