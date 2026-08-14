// Barnacles Web Dashboard Client Logic
(function () {
  'use strict';

  // DOM Elements
  const connIndicator = document.getElementById('conn-indicator');
  const connStatus = document.getElementById('conn-status');
  const agentCount = document.getElementById('agent-count');
  const eventRate = document.getElementById('event-rate');
  const totalEventsEl = document.getElementById('total-events');

  const searchInput = document.getElementById('search-input');
  const levelSelect = document.getElementById('level-select');
  const hostInput = document.getElementById('host-input');
  const sourceInput = document.getElementById('source-input');
  const hostsList = document.getElementById('hosts-list');
  const sourcesList = document.getElementById('sources-list');

  const btnPause = document.getElementById('btn-pause');
  const pauseText = document.getElementById('pause-text');
  const btnAutoScroll = document.getElementById('btn-autoscroll');
  const btnQuery = document.getElementById('btn-query');
  const btnClear = document.getElementById('btn-clear');

  const tableContainer = document.querySelector('.table-container');
  const logBody = document.getElementById('log-body');
  const emptyState = document.getElementById('empty-state');

  const modal = document.getElementById('detail-modal');
  const modalClose = document.getElementById('modal-close-btn');
  const modalContent = document.getElementById('modal-json-content');

  // State
  let ws = null;
  let reconnectAttempt = 0;
  let isPaused = false;
  let autoScroll = true;
  let eventCounter = 0;
  let rateCounter = 0;
  const maxTableRows = 1000;

  const knownHosts = new Set();
  const knownSources = new Set();
  const eventStore = []; // in-memory slice of recent displayed events

  // WebSocket Connection Manager
  function connectWebSocket() {
    updateConnState('connecting', 'Connecting...');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = function () {
      reconnectAttempt = 0;
      updateConnState('connected', 'Connected');
    };

    ws.onmessage = function (event) {
      if (isPaused) return;

      const lines = event.data.trim().split('\n');
      for (const line of lines) {
        if (!line) continue;
        try {
          const msg = JSON.parse(line);
          handleStreamMessage(msg);
        } catch (e) {
          console.error('Failed to parse incoming WebSocket message:', e);
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
    const backoff = Math.min(30000, 1000 * Math.pow(1.5, reconnectAttempt));
    connStatus.textContent = `Reconnecting in ${(backoff / 1000).toFixed(0)}s...`;
    setTimeout(connectWebSocket, backoff);
  }

  function updateConnState(state, text) {
    connIndicator.className = `status-indicator status-${state}`;
    connStatus.textContent = text;
  }

  function handleStreamMessage(msg) {
    if (msg.type === 'log' && msg.data) {
      appendLogEntry(msg.data);
      rateCounter++;
    } else if (msg.type === 'recent_batch' && Array.isArray(msg.data)) {
      for (const entry of msg.data) {
        appendLogEntry(entry);
      }
    }
  }

  function appendLogEntry(entry) {
    eventCounter++;
    totalEventsEl.textContent = eventCounter.toLocaleString();

    // Track host and source metadata
    if (entry.host && !knownHosts.has(entry.host)) {
      knownHosts.add(entry.host);
      updateDatalist(hostsList, knownHosts);
      agentCount.textContent = knownHosts.size;
    }
    if (entry.source && !knownSources.has(entry.source)) {
      knownSources.add(entry.source);
      updateDatalist(sourcesList, knownSources);
    }

    eventStore.push(entry);
    if (eventStore.length > maxTableRows) {
      eventStore.shift();
    }

    if (matchesFilter(entry)) {
      renderRow(entry);
      emptyState.style.display = 'none';
      if (autoScroll && !isGliding) {
        scrollToBottomInstant();
      }
    }
  }

  let isGliding = false;

  function triggerSmoothScrollToBottom() {
    if (!tableContainer) return;
    isGliding = true;
    tableContainer.style.scrollBehavior = 'smooth';

    if (logBody && logBody.lastElementChild) {
      logBody.lastElementChild.scrollIntoView({ behavior: 'smooth', block: 'end' });
    } else {
      tableContainer.scrollTo({ top: tableContainer.scrollHeight, behavior: 'smooth' });
    }

    setTimeout(() => {
      if (tableContainer) {
        tableContainer.style.scrollBehavior = 'auto';
        tableContainer.scrollTop = tableContainer.scrollHeight;
      }
      isGliding = false;
    }, 600);
  }

  function scrollToBottomInstant() {
    if (!tableContainer || isGliding) return;
    tableContainer.scrollTop = tableContainer.scrollHeight;
  }

  function renderRow(entry) {
    const tr = document.createElement('tr');
    tr.dataset.id = entry.id;

    const lvl = (entry.level || 'INFO').toUpperCase();
    let badgeClass = 'badge-info';
    if (lvl.includes('WARN')) badgeClass = 'badge-warn';
    else if (lvl.includes('ERR')) badgeClass = 'badge-error';
    else if (lvl.includes('FATAL') || lvl.includes('CRIT')) badgeClass = 'badge-fatal';
    else if (lvl.includes('DEBUG') || lvl.includes('TRACE')) badgeClass = 'badge-debug';

    const ts = new Date(entry.timestamp).toISOString();

    tr.innerHTML = `
      <td class="td-timestamp">${ts}</td>
      <td><span class="badge ${badgeClass}">${lvl}</span></td>
      <td class="td-host">${escapeHtml(entry.host || '-')}</td>
      <td class="td-source">${escapeHtml(entry.source || '-')}</td>
      <td class="td-message">${escapeHtml(entry.message || '')}</td>
      <td><button class="btn-details">View</button></td>
    `;

    tr.querySelector('.btn-details').addEventListener('click', () => {
      showModal(entry);
    });

    logBody.appendChild(tr);

    // Enforce max DOM rows to preserve browser performance
    while (logBody.children.length > maxTableRows) {
      logBody.removeChild(logBody.firstChild);
    }
  }

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
      let inFields = false;
      if (entry.fields) {
        for (const [k, v] of Object.entries(entry.fields)) {
          if (k.toLowerCase().includes(search) || String(v).toLowerCase().includes(search)) {
            inFields = true;
            break;
          }
        }
      }
      if (!inMsg && !inFields) return false;
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
    emptyState.style.display = count === 0 ? 'block' : 'none';
    if (autoScroll) {
      scrollToBottomInstant();
    }
  }

  function updateDatalist(datalistEl, set) {
    datalistEl.innerHTML = '';
    for (const item of set) {
      const opt = document.createElement('option');
      opt.value = item;
      datalistEl.appendChild(opt);
    }
  }

  function showModal(entry) {
    modalContent.textContent = JSON.stringify(entry, null, 2);
    modal.classList.add('active');
  }

  function closeModal() {
    modal.classList.remove('active');
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // Event Listeners
  searchInput.addEventListener('input', reapplyFilters);
  levelSelect.addEventListener('change', reapplyFilters);
  hostInput.addEventListener('input', reapplyFilters);
  sourceInput.addEventListener('input', reapplyFilters);

  tableContainer.addEventListener('scroll', function () {
    if (isGliding) return;
    const threshold = 60;
    const isAtBottom = tableContainer.scrollHeight - tableContainer.scrollTop - tableContainer.clientHeight <= threshold;
    if (!isAtBottom && autoScroll) {
      autoScroll = false;
      btnAutoScroll.classList.remove('active');
    } else if (isAtBottom && !autoScroll) {
      autoScroll = true;
      btnAutoScroll.classList.add('active');
    }
  });

  btnPause.addEventListener('click', function () {
    isPaused = !isPaused;
    if (isPaused) {
      btnPause.classList.add('active');
      pauseText.textContent = 'Resume';
    } else {
      btnPause.classList.remove('active');
      pauseText.textContent = 'Pause';
    }
  });

  btnAutoScroll.addEventListener('click', function () {
    autoScroll = !autoScroll;
    btnAutoScroll.classList.toggle('active', autoScroll);
    if (autoScroll) {
      triggerSmoothScrollToBottom();
    }
  });

  btnClear.addEventListener('click', function () {
    logBody.innerHTML = '';
    eventStore.length = 0;
    emptyState.style.display = 'block';
  });

  btnQuery.addEventListener('click', async function () {
    const params = new URLSearchParams();
    if (hostInput.value.trim()) params.set('host', hostInput.value.trim());
    if (sourceInput.value.trim()) params.set('source', sourceInput.value.trim());
    if (levelSelect.value) params.set('level', levelSelect.value);
    if (searchInput.value.trim()) params.set('search', searchInput.value.trim());
    params.set('limit', '500');

    try {
      btnQuery.disabled = true;
      btnQuery.textContent = 'Fetching...';
      const res = await fetch(`/api/v1/logs?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      if (data && Array.isArray(data.logs)) {
        logBody.innerHTML = '';
        eventStore.length = 0;
        for (const entry of data.logs) {
          appendLogEntry(entry);
        }
      }
    } catch (err) {
      alert(`Query failed: ${err.message}`);
    } finally {
      btnQuery.disabled = false;
      btnQuery.textContent = 'Fetch Historical';
    }
  });

  modalClose.addEventListener('click', closeModal);
  modal.addEventListener('click', (e) => {
    if (e.target === modal) closeModal();
  });

  // Rates calculation timer
  setInterval(function () {
    eventRate.textContent = rateCounter;
    rateCounter = 0;
  }, 1000);

  // Initialize
  connectWebSocket();
})();
