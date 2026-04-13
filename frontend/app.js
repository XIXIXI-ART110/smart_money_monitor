const state = {
  stockPayload: null,
  selectedSearchStock: null,
  watchStocks: [],
  selectedWatchCodes: [],
  pendingWatchMutationCodes: [],
  stockResultOrder: [],
  hiddenStockResultCodes: [],
  selectedIndexes: [],
  availableIndexes: [],
  indexBoardOrder: [],
  hiddenIndexCodes: [],
  activeIndexCode: "",
  activeIndexDetail: null,
  opportunityMode: "stock",
  opportunityScope: "market",
  opportunityBoard: "all",
  opportunityRequestId: 0,
  indexOpportunityItems: [],
  stockOpportunityItems: [],
  lowOpportunityItems: [],
  opportunityScanStats: null,
  opportunityCacheMeta: null,
  recommendedOpportunity: null,
  activeOpportunityCode: "",
  activeOpportunityDetail: null,
  styleFlowPayload: null,
  styleIntentPayload: null,
  stockSearchResults: [],
  stockSearchTimer: null,
  stockSearchRequestId: 0,
  toastTimer: 0,
};

const dom = {
  navButtons: [...document.querySelectorAll(".nav button")],
  homeMarketStatus: document.getElementById("homeMarketStatus"),
  homeAvgChange: document.getElementById("homeAvgChange"),
  homeTotalInflow: document.getElementById("homeTotalInflow"),
  homeEtfCount: document.getElementById("homeEtfCount"),
  homeMarketSummary: document.getElementById("homeMarketSummary"),
  styleChips: document.getElementById("styleChips"),
  strongOpportunityList: document.getElementById("strongOpportunityList"),
  mediumOpportunityList: document.getElementById("mediumOpportunityList"),
  homeEtfSummary: document.getElementById("homeEtfSummary"),
  homeEtfChips: document.getElementById("homeEtfChips"),
  refreshHomeBtn: document.getElementById("refreshHomeBtn"),
  stockSearchInput: document.getElementById("stockSearchInput"),
  stockSearchDropdown: document.getElementById("stockSearchDropdown"),
  addStockBtn: document.getElementById("addStockBtn"),
  stockManageText: document.getElementById("stockManageText"),
  stockWatchList: document.getElementById("stockWatchList"),
  runStockBtn: document.getElementById("runStockBtn"),
  stockRunStatus: document.getElementById("stockRunStatus"),
  stockElapsed: document.getElementById("stockElapsed"),
  stockNotify: document.getElementById("stockNotify"),
  stockResults: document.getElementById("stockResults"),
  refreshIndexBtn: document.getElementById("refreshIndexBtn"),
  indexBoardStatus: document.getElementById("indexBoardStatus"),
  indexQuickChips: document.getElementById("indexQuickChips"),
  indexBoardGrid: document.getElementById("indexBoardGrid"),
  indexSelectionStatus: document.getElementById("indexSelectionStatus"),
  indexDetailPanel: document.getElementById("indexDetailPanel"),
  refreshStyleFlowBtn: document.getElementById("refreshStyleFlowBtn"),
  styleFlowStatus: document.getElementById("styleFlowStatus"),
  styleFlowMeta: document.getElementById("styleFlowMeta"),
  styleFlowDistribution: document.getElementById("styleFlowDistribution"),
  styleIntentPanel: document.getElementById("styleIntentPanel"),
  styleEtfGroups: document.getElementById("styleEtfGroups"),
  opportunityStatus: document.getElementById("opportunityStatus"),
  opportunityScanStats: document.getElementById("opportunityScanStats"),
  opportunityScanModeText: document.getElementById("opportunityScanModeText"),
  opportunityCacheInfoText: document.getElementById("opportunityCacheInfoText"),
  opportunityScanBadges: document.getElementById("opportunityScanBadges"),
  opportunityRefreshBtn: document.getElementById("opportunityRefreshBtn"),
  opportunityScannedTotal: document.getElementById("opportunityScannedTotal"),
  opportunityPrefilteredTotal: document.getElementById("opportunityPrefilteredTotal"),
  opportunityRefinedTotal: document.getElementById("opportunityRefinedTotal"),
  opportunityReturnedTotal: document.getElementById("opportunityReturnedTotal"),
  opportunityTabs: document.getElementById("opportunityTabs"),
  opportunityScopeTabs: document.getElementById("opportunityScopeTabs"),
  opportunityBoardTabs: document.getElementById("opportunityBoardTabs"),
  opportunityRecommendCard: document.getElementById("opportunityRecommendCard"),
  opportunityPoolList: document.getElementById("opportunityPoolList"),
  opportunityDetailPanel: document.getElementById("opportunityDetailPanel"),
  availableIndexCount: document.getElementById("availableIndexCount"),
  availableIndexPool: document.getElementById("availableIndexPool"),
  heatmap: document.getElementById("heatmap")
};

const API_BASE = (() => {
  const configuredBase =
    window.SMART_MONEY_API_BASE ||
    document.querySelector('meta[name="smart-money-api-base"]')?.getAttribute("content") ||
    "";
  const normalizedBase = String(configuredBase).trim().replace(/\/+$/, "");
  if (normalizedBase) return normalizedBase;

  const { protocol, hostname, port } = window.location;
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";

  if ((protocol === "http:" || protocol === "https:") && isLocalHost) {
    // When the frontend is served by FastAPI locally, prefer same-origin API calls.
    return "";
  }
  if (protocol === "file:") {
    return port === "8000" ? "http://127.0.0.1:8000" : "http://127.0.0.1:8001";
  }
  return "";
})();

const IS_LOCAL_DEV = (() => {
  const { protocol, hostname } = window.location;
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";
  return protocol === "file:" || isLocalHost;
})();

function apiPath(path) {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE}${path}`;
}

const api = {
  stocks: apiPath("/api/stocks"),
  runStocks: apiPath("/api/run-once"),
  analyzeStock: (code, name = "") => apiPath(`/api/run-once?code=${encodeURIComponent(code)}&name=${encodeURIComponent(name)}`),
  indexes: apiPath("/api/indexes"),
  indexOptions: apiPath("/api/indexes/options"),
  indexDetail: (code) => apiPath(`/api/indexes/detail?code=${encodeURIComponent(code)}`),
  styleFundFlow: apiPath("/api/style-fund-flow"),
  styleIntent: apiPath("/api/style-intent"),
  opportunities: (scope = "market", board = "all", forceRefresh = false) => apiPath(`/api/opportunities?scope=${encodeURIComponent(scope)}&board=${encodeURIComponent(board)}${forceRefresh ? "&force_refresh=true" : ""}`),
  lowOpportunity: apiPath("/api/opportunity/low"),
  stockLowOpportunity: apiPath("/api/opportunity/stock_low"),
  opportunityRecommend: apiPath("/api/opportunity/recommend"),
  opportunityDetail: (code) => apiPath(`/api/opportunity/detail?code=${encodeURIComponent(code)}`),
  searchStocks: (keyword) => apiPath(`/api/search-stocks?q=${encodeURIComponent(keyword)}`)
};

const OPPORTUNITY_SCOPE_LABELS = {
  market: "全市场",
  watchlist: "自选股"
};

const OPPORTUNITY_BOARD_LABELS = {
  all: "全部",
  gem: "创业板",
  sz_main: "深市主板",
  sh_main: "沪市主板",
  star: "科创板"
};

const NAV_VIEW_STORAGE_KEY = "smart-money-monitor:last-view";
const WATCH_SELECTION_STORAGE_KEY = "smart-money-monitor:selected-watch-codes";
const STOCK_RESULT_ORDER_STORAGE_KEY = "smart-money-monitor:stock-results-order:v1";
const STOCK_RESULT_HIDDEN_STORAGE_KEY = "smart-money-monitor:stock-results-hidden:v1";
const INDEX_BOARD_ORDER_STORAGE_KEY = "smart-money-monitor:index-board-order:v1";
const INDEX_BOARD_HIDDEN_STORAGE_KEY = "smart-money-monitor:index-board-hidden:v1";
const DEFAULT_VIEW_ID = "etfView";
const NAV_VIEW_KEY_TO_ID = {
  home: "homeView",
  stock: "stockView",
  etf: "etfView",
  style_flow: "styleFlowView",
  opportunities: "opportunityView",
  heatmap: "heatView"
};
const NAV_VIEW_ID_TO_KEY = Object.fromEntries(
  Object.entries(NAV_VIEW_KEY_TO_ID).map(([key, id]) => [id, key])
);

const STATIC_LOW_OPPORTUNITY_ITEMS = [
  {
    code: "510300",
    name: "沪深300ETF",
    score: 82,
    signal: "推荐",
    reason: "回撤后企稳，均线拐头，量能改善",
    summary: "适合观察低位布局机会，但不建议追高。",
    metrics: {
      drawdown: "18%",
      trend: "均线拐头",
      risk: "中"
    }
  },
  {
    code: "159915",
    name: "创业板ETF",
    score: 74,
    signal: "观察",
    reason: "低位震荡，短期修复中",
    summary: "当前位置偏低，但修复确认还需要继续观察成交量。",
    metrics: {
      drawdown: "22%",
      trend: "震荡修复",
      risk: "中高"
    }
  },
  {
    code: "588000",
    name: "科创50ETF",
    score: 67,
    signal: "观察",
    reason: "阶段低位附近止跌，短线改善",
    summary: "有修复迹象，但波动较大，适合轻仓跟踪。",
    metrics: {
      drawdown: "25%",
      trend: "低位企稳",
      risk: "中高"
    }
  },
  {
    code: "512100",
    name: "中证1000ETF",
    score: 56,
    signal: "谨慎",
    reason: "波动仍偏大，量能改善不明显",
    summary: "暂时不作为优先配置方向，先等待更清晰信号。",
    metrics: {
      drawdown: "14%",
      trend: "弱势震荡",
      risk: "高"
    }
  }
];

const esc = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#39;");

const num = (value, digits = 2) => {
  const parsed = Number(value);
  if (value === null || value === undefined || value === "" || Number.isNaN(parsed)) return "N/A";
  return parsed.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
};

const pct = (value) => {
  const parsed = Number(value);
  if (value === null || value === undefined || value === "" || Number.isNaN(parsed)) return "N/A";
  return `${parsed.toFixed(2)}%`;
};

const amt = (value) => {
  const parsed = Number(value);
  if (value === null || value === undefined || value === "" || Number.isNaN(parsed)) return "N/A";
  const abs = Math.abs(parsed);
  if (abs >= 100000000) return `${(parsed / 100000000).toFixed(2)} 亿`;
  if (abs >= 10000) return `${(parsed / 10000).toFixed(2)} 万`;
  return parsed.toFixed(2);
};

const hasNumericValue = (value) => value !== null && value !== undefined && value !== "" && !Number.isNaN(Number(value));

const stockPriceText = (value) => {
  if (!hasNumericValue(value) || Number(value) <= 0) return "--";
  return num(value);
};

const stockPercentText = (value) => {
  if (!hasNumericValue(value)) return "--";
  return pct(value);
};

const stockAmountText = (value) => {
  if (!hasNumericValue(value)) return "--";
  return amt(value);
};

const tone = (value) => {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) return "flat";
  if (parsed > 0) return "up";
  if (parsed < 0) return "down";
  return "flat";
};

function firstDefinedValue(...values) {
  return values.find((value) => value !== null && value !== undefined && value !== "");
}

function dedupeIds(values) {
  return [...new Set(
    (Array.isArray(values) ? values : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  )];
}

function readStoredIdArray(key) {
  try {
    return dedupeIds(JSON.parse(localStorage.getItem(key) || "[]"));
  } catch (error) {
    console.warn(`Failed to read localStorage key: ${key}`, error);
    return [];
  }
}

function persistStoredIdArray(key, values) {
  try {
    localStorage.setItem(key, JSON.stringify(dedupeIds(values)));
  } catch (error) {
    console.warn(`Failed to persist localStorage key: ${key}`, error);
  }
}

function replaceStoredIdArray(key, values, assign) {
  const normalized = dedupeIds(values);
  assign(normalized);
  persistStoredIdArray(key, normalized);
}

function removeIds(source, idsToRemove) {
  if (!idsToRemove.length) return dedupeIds(source);
  const removed = new Set(dedupeIds(idsToRemove));
  return dedupeIds(source).filter((id) => !removed.has(id));
}

function appendIds(source, idsToAppend) {
  return dedupeIds([...(Array.isArray(source) ? source : []), ...dedupeIds(idsToAppend)]);
}

function orderItemsByStoredIds(items, getId, orderedIds) {
  const itemMap = new Map();
  const unordered = [];

  (Array.isArray(items) ? items : []).forEach((item) => {
    const id = getId(item);
    if (!id || itemMap.has(id)) {
      unordered.push(item);
      return;
    }
    itemMap.set(id, item);
  });

  const ordered = [];
  dedupeIds(orderedIds).forEach((id) => {
    const item = itemMap.get(id);
    if (!item) return;
    ordered.push(item);
    itemMap.delete(id);
  });

  return [...ordered, ...itemMap.values(), ...unordered];
}

function getStockResultId(item) {
  return String(item?.code || item?.symbol || "").trim();
}

function getIndexCardId(item) {
  return String(item?.code || item?.index_code || item?.symbol || "").trim();
}

function reorderItemsByIds(items, ids, getId) {
  return orderItemsByStoredIds(items, getId, ids);
}

function persistedStockResults(results) {
  const visibleResults = (Array.isArray(results) ? results : []).filter((item) => {
    const id = getStockResultId(item);
    return id && !state.hiddenStockResultCodes.includes(id);
  });
  const ordered = orderItemsByStoredIds(visibleResults, getStockResultId, state.stockResultOrder);
  const nextOrder = ordered.map((item) => getStockResultId(item)).filter(Boolean);
  replaceStoredIdArray(STOCK_RESULT_ORDER_STORAGE_KEY, nextOrder, (value) => {
    state.stockResultOrder = value;
  });
  return ordered;
}

function clearHiddenStockResultsForIncomingItems(results) {
  const incomingIds = dedupeIds((Array.isArray(results) ? results : []).map((item) => getStockResultId(item)));
  if (!incomingIds.length) return;
  replaceStoredIdArray(
    STOCK_RESULT_HIDDEN_STORAGE_KEY,
    removeIds(state.hiddenStockResultCodes, incomingIds),
    (value) => {
      state.hiddenStockResultCodes = value;
    }
  );
}

function persistIndexBoardState() {
  replaceStoredIdArray(
    INDEX_BOARD_ORDER_STORAGE_KEY,
    state.selectedIndexes.map((item) => getIndexCardId(item)),
    (value) => {
      state.indexBoardOrder = value;
    }
  );
  replaceStoredIdArray(
    INDEX_BOARD_HIDDEN_STORAGE_KEY,
    state.hiddenIndexCodes,
    (value) => {
      state.hiddenIndexCodes = value;
    }
  );
}

function buildPersistedIndexSelection(defaultIndexes, allOptions) {
  const defaultItems = Array.isArray(defaultIndexes) ? defaultIndexes : [];
  const pool = Array.isArray(allOptions) && allOptions.length ? allOptions : defaultItems;
  const poolMap = new Map(pool.map((item) => [getIndexCardId(item), item]));
  const hiddenSet = new Set(state.hiddenIndexCodes);
  const ordered = [];
  const seen = new Set();

  dedupeIds(state.indexBoardOrder).forEach((id) => {
    const item = poolMap.get(id);
    if (!item || hiddenSet.has(id) || seen.has(id)) return;
    ordered.push(item);
    seen.add(id);
  });

  defaultItems.forEach((item) => {
    const id = getIndexCardId(item);
    if (!id || hiddenSet.has(id) || seen.has(id)) return;
    ordered.push(poolMap.get(id) || item);
    seen.add(id);
  });

  return ordered;
}

function clearIndexBoardPersistence() {
  state.indexBoardOrder = [];
  state.hiddenIndexCodes = [];
  persistIndexBoardState();
}

function getRenderedIds(container, itemSelector, dataAttribute) {
  return [...container.querySelectorAll(itemSelector)]
    .map((element) => String(element.dataset[dataAttribute] || "").trim())
    .filter(Boolean);
}

function createSortableGrid(container, options) {
  let dragState = null;

  function cleanupDrag(event) {
    if (!dragState) return;
    const current = dragState;
    if (current.handle?.releasePointerCapture && event?.pointerId !== undefined) {
      try {
        current.handle.releasePointerCapture(event.pointerId);
      } catch (_) {
        // Ignore release errors from stale pointer captures.
      }
    }
    window.removeEventListener("pointermove", handlePointerMove);
    window.removeEventListener("pointerup", handlePointerEnd);
    window.removeEventListener("pointercancel", handlePointerEnd);

    if (current.phase === "active") {
      current.card.classList.remove("is-dragging");
      current.container.classList.remove("is-sorting");
      document.body.classList.remove("dragging-cards");
      current.card.style.position = "";
      current.card.style.left = "";
      current.card.style.top = "";
      current.card.style.width = "";
      current.card.style.height = "";
      current.card.style.zIndex = "";
      current.card.style.pointerEvents = "";
      current.card.style.margin = "";
      if (current.placeholder?.parentNode) {
        current.placeholder.parentNode.insertBefore(current.card, current.placeholder);
        current.placeholder.remove();
      }
    }

    dragState = null;
  }

  function closestItem(clientX, clientY) {
    const elements = [...container.querySelectorAll(options.itemSelector)]
      .filter((element) => element !== dragState?.card);
    if (!elements.length) return null;

    return elements.reduce((best, element) => {
      const rect = element.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      const distance = Math.hypot(centerX - clientX, centerY - clientY);
      if (!best || distance < best.distance) {
        return { element, rect, centerX, centerY, distance };
      }
      return best;
    }, null);
  }

  function placePlaceholder(clientX, clientY) {
    if (!dragState?.placeholder) return;
    const target = closestItem(clientX, clientY);
    if (!target) return;
    const shouldInsertAfter = clientY > target.centerY
      || (Math.abs(clientY - target.centerY) < target.rect.height * 0.32 && clientX > target.centerX);
    const referenceNode = shouldInsertAfter ? target.element.nextSibling : target.element;
    if (referenceNode === dragState.placeholder) return;
    container.insertBefore(dragState.placeholder, referenceNode);
  }

  function updateFloatingCard(clientX, clientY) {
    dragState.card.style.left = `${clientX - dragState.offsetX}px`;
    dragState.card.style.top = `${clientY - dragState.offsetY}px`;
  }

  function beginActiveDrag(event) {
    const rect = dragState.card.getBoundingClientRect();
    dragState.phase = "active";
    dragState.offsetX = event.clientX - rect.left;
    dragState.offsetY = event.clientY - rect.top;
    dragState.placeholder = document.createElement("div");
    dragState.placeholder.className = "card-sort-placeholder";
    dragState.placeholder.style.height = `${rect.height}px`;
    dragState.container.insertBefore(dragState.placeholder, dragState.card.nextSibling);
    dragState.card.classList.add("is-dragging");
    dragState.container.classList.add("is-sorting");
    document.body.classList.add("dragging-cards");
    dragState.card.style.width = `${rect.width}px`;
    dragState.card.style.height = `${rect.height}px`;
    dragState.card.style.position = "fixed";
    dragState.card.style.left = `${rect.left}px`;
    dragState.card.style.top = `${rect.top}px`;
    dragState.card.style.zIndex = "80";
    dragState.card.style.pointerEvents = "none";
    dragState.card.style.margin = "0";
    updateFloatingCard(event.clientX, event.clientY);
    placePlaceholder(event.clientX, event.clientY);
  }

  function handlePointerMove(event) {
    if (!dragState || event.pointerId !== dragState.pointerId) return;
    const distance = Math.hypot(event.clientX - dragState.startX, event.clientY - dragState.startY);
    if (dragState.phase === "pending") {
      if (distance < 8) return;
      beginActiveDrag(event);
    }
    if (dragState.phase !== "active") return;
    event.preventDefault();
    updateFloatingCard(event.clientX, event.clientY);
    placePlaceholder(event.clientX, event.clientY);
  }

  function handlePointerEnd(event) {
    if (!dragState || event.pointerId !== dragState.pointerId) return;
    const shouldCommit = dragState.phase === "active";
    cleanupDrag(event);
    if (!shouldCommit) return;
    const ids = getRenderedIds(container, options.itemSelector, options.dataAttribute);
    options.onCommit(dedupeIds(ids));
  }

  container.addEventListener("pointerdown", (event) => {
    const handle = event.target.closest(options.handleSelector);
    if (!handle) return;
    const card = handle.closest(options.itemSelector);
    if (!card) return;
    if (event.pointerType === "mouse" && event.button !== 0) return;
    if (dragState) cleanupDrag(event);
    event.preventDefault();
    event.stopPropagation();
    dragState = {
      phase: "pending",
      container,
      card,
      handle,
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      offsetX: 0,
      offsetY: 0,
      placeholder: null,
    };
    if (handle.setPointerCapture) {
      try {
        handle.setPointerCapture(event.pointerId);
      } catch (_) {
        // Ignore pointer capture errors on unsupported browsers.
      }
    }
    window.addEventListener("pointermove", handlePointerMove, { passive: false });
    window.addEventListener("pointerup", handlePointerEnd);
    window.addEventListener("pointercancel", handlePointerEnd);
  });
}

function hiddenFundTagByScore(score) {
  if (!hasNumericValue(score)) return "";
  const numeric = Number(score);
  if (numeric >= 80) return "强吸筹";
  if (numeric >= 60) return "吸筹";
  if (numeric >= 40) return "中性";
  if (numeric >= 20) return "出货";
  return "强出货";
}

function hiddenFundToneClass(score, tag) {
  const rawTag = String(tag || "").trim();
  if (rawTag.includes("强吸筹") || rawTag.includes("吸筹")) return "up";
  if (rawTag.includes("强出货") || rawTag.includes("出货")) return "down";
  if (hasNumericValue(score)) {
    const numeric = Number(score);
    if (numeric >= 60) return "up";
    if (numeric < 40) return "down";
  }
  return "flat";
}

const NO_HIDDEN_FUND_MESSAGE = "暂无明显主力吸筹或出货信号";

function resolveHiddenFundData(item) {
  const analysis = item?.analysis && typeof item.analysis === "object" ? item.analysis : {};
  const score = item?.score && typeof item.score === "object" ? item.score : {};
  const rawScore = firstDefinedValue(
    analysis.hidden_fund_score,
    score.hidden_fund_score,
    item?.hidden_fund_score
  );
  const hiddenFundScore = hasNumericValue(rawScore)
    ? Math.max(0, Math.min(100, Number(rawScore)))
    : null;
  const rawTag = firstDefinedValue(
    analysis.hidden_fund_tag,
    score.hidden_fund_tag,
    item?.hidden_fund_tag
  );
  const rawReason = firstDefinedValue(
    analysis.hidden_fund_reason,
    score.hidden_fund_reason,
    item?.hidden_fund_reason
  );
  const rawLabels = firstArrayValue(
    analysis.hidden_fund_labels,
    score.hidden_fund_labels,
    item?.hidden_fund_labels
  );
  const hiddenFundLabels = rawLabels
    .map((label) => String(label ?? "").trim())
    .filter(Boolean);
  const hiddenFundTag = String(rawTag || hiddenFundTagByScore(hiddenFundScore)).trim();
  const hiddenFundReason = String(rawReason || "").trim();
  const hasSignal = hiddenFundScore !== null || Boolean(hiddenFundTag) || Boolean(hiddenFundReason) || hiddenFundLabels.length > 0;
  return {
    hasSignal,
    score: hiddenFundScore,
    scoreText: hiddenFundScore === null ? "--" : `${Math.round(hiddenFundScore)}分`,
    tag: hiddenFundTag || "中性观察",
    reason: hiddenFundReason || NO_HIDDEN_FUND_MESSAGE,
    labels: hiddenFundLabels,
    toneClass: hiddenFundToneClass(hiddenFundScore, hiddenFundTag),
    meterWidth: hiddenFundScore === null ? 0 : Math.max(6, Math.min(100, Number(hiddenFundScore))),
  };
}

function setChip(el, text, toneName = "") {
  el.textContent = text;
  el.className = `chip ${toneName}`.trim();
}

function ensureToastElement() {
  let toast = document.getElementById("appToast");
  if (toast) return toast;
  toast = document.createElement("div");
  toast.id = "appToast";
  toast.className = "app-toast hidden";
  toast.setAttribute("aria-live", "polite");
  document.body.appendChild(toast);
  return toast;
}

function showToast(message, toneName = "blue") {
  const toast = ensureToastElement();
  window.clearTimeout(state.toastTimer);
  toast.textContent = message;
  toast.className = `app-toast ${toneName}`.trim();
  state.toastTimer = window.setTimeout(() => {
    toast.classList.add("hidden");
  }, 2200);
}

function normalizeViewId(view) {
  const rawView = String(view || "").trim();
  const id = NAV_VIEW_KEY_TO_ID[rawView] || rawView;
  const hasNavButton = dom.navButtons.some((button) => button.dataset.view === id);
  return hasNavButton && document.getElementById(id) ? id : "";
}

function persistActiveView(id) {
  const key = NAV_VIEW_ID_TO_KEY[id];
  if (!key) return;
  try {
    localStorage.setItem(NAV_VIEW_STORAGE_KEY, key);
  } catch (error) {
    console.warn("Failed to persist active view", error);
  }
}

function readStoredViewId() {
  try {
    return normalizeViewId(localStorage.getItem(NAV_VIEW_STORAGE_KEY));
  } catch (error) {
    console.warn("Failed to read active view", error);
    return "";
  }
}

function switchView(id, options = {}) {
  const normalizedId = normalizeViewId(id) || DEFAULT_VIEW_ID;
  const persist = options.persist !== false;
  document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === normalizedId));
  dom.navButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.view === normalizedId));
  if (persist) {
    persistActiveView(normalizedId);
  }
}

function restoreActiveView() {
  switchView(readStoredViewId() || DEFAULT_VIEW_ID, { persist: false });
}

function persistWatchSelection(codes) {
  try {
    localStorage.setItem(WATCH_SELECTION_STORAGE_KEY, JSON.stringify(codes));
  } catch (error) {
    console.warn("Failed to persist watch selection", error);
  }
}

function isWatchlisted(code) {
  const normalizedCode = String(code || "").trim();
  return state.watchStocks.some((item) => String(item.code || "").trim() === normalizedCode);
}

function isWatchMutationPending(code) {
  const normalizedCode = String(code || "").trim();
  return state.pendingWatchMutationCodes.includes(normalizedCode);
}

function setWatchMutationPending(code, pending) {
  const normalizedCode = String(code || "").trim();
  if (!normalizedCode) return;
  const nextCodes = new Set(state.pendingWatchMutationCodes);
  if (pending) nextCodes.add(normalizedCode);
  else nextCodes.delete(normalizedCode);
  state.pendingWatchMutationCodes = [...nextCodes];
}

function watchSourceLabel(item) {
  const source = String(item?.source || "").trim();
  if (source === "opportunity_pool") return "来自机会池";
  if (source === "stock_search") return "来自搜索";
  return "手动关注";
}

function readStoredWatchSelection() {
  try {
    const raw = JSON.parse(localStorage.getItem(WATCH_SELECTION_STORAGE_KEY) || "[]");
    return Array.isArray(raw) ? raw.map((item) => String(item || "").trim()).filter(Boolean) : [];
  } catch (error) {
    console.warn("Failed to read watch selection", error);
    return [];
  }
}

function syncWatchSelection(items) {
  const validCodes = new Set(items.map((item) => String(item.code || "").trim()).filter(Boolean));
  state.selectedWatchCodes = state.selectedWatchCodes.filter((code) => validCodes.has(code));
  persistWatchSelection(state.selectedWatchCodes);
}

function updateWatchManageText() {
  const total = state.watchStocks.length;
  const selected = state.selectedWatchCodes.length;
  const suffix = selected ? ` · 已选中 ${selected} 只` : "";
  setChip(dom.stockManageText, `当前自选股：${total}只${suffix}`);
}

function buildWatchPayload(stock, source = "manual") {
  const code = String(stock?.code || "").trim();
  const name = String(stock?.name || "").trim() || code;
  const board = String(stock?.board || "").trim();
  return {
    code,
    name,
    source,
    board,
    added_at: new Date().toISOString(),
  };
}

async function saveWatchStock(payload, options = {}) {
  const code = String(payload?.code || "").trim();
  const name = String(payload?.name || "").trim() || code;
  const remove = options.remove === true;
  if (!code || !name) return false;
  if (isWatchMutationPending(code)) return false;

  setWatchMutationPending(code, true);
  try {
    if (remove) {
      await fetchJson(`${api.stocks}/${encodeURIComponent(code)}`, { method: "DELETE" });
      showToast(`已取消关注 ${name}`);
    } else {
      await fetchJson(api.stocks, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      showToast(`已加入自选：${name}`, "good");
    }
    await loadStocks();
    renderRecommendedOpportunity();
    renderOpportunityPool();
    renderOpportunityDetail();
    return true;
  } catch (error) {
    showToast(remove ? `取消关注失败：${error.message}` : `加入自选失败：${error.message}`, "bad");
    return false;
  } finally {
    setWatchMutationPending(code, false);
    renderRecommendedOpportunity();
    renderOpportunityPool();
    renderOpportunityDetail();
  }
}

function toggleWatchSelection(code) {
  const normalizedCode = String(code || "").trim();
  if (!normalizedCode) return;
  const selectedSet = new Set(state.selectedWatchCodes);
  if (selectedSet.has(normalizedCode)) {
    selectedSet.delete(normalizedCode);
  } else {
    selectedSet.add(normalizedCode);
  }
  state.selectedWatchCodes = state.watchStocks
    .map((item) => String(item.code || "").trim())
    .filter((itemCode) => selectedSet.has(itemCode));
  persistWatchSelection(state.selectedWatchCodes);
  renderWatchList(dom.stockWatchList, state.watchStocks);
  updateWatchManageText();
}

function renderWatchList(target, items) {
  if (!items.length) {
    target.innerHTML = `<div class="empty">当前暂无股票自选项。</div>`;
    return;
  }
  const selectedCodes = new Set(state.selectedWatchCodes);
  target.innerHTML = items.map((item) => `
    <div
      class="watch-item ${selectedCodes.has(item.code) ? "selected" : ""}"
      data-watch-code="${esc(item.code)}"
      role="button"
      tabindex="0"
      aria-pressed="${selectedCodes.has(item.code) ? "true" : "false"}"
    >
      <div class="watch-main">
        <span class="watch-select-indicator" aria-hidden="true">${selectedCodes.has(item.code) ? "✓" : "✓"}</span>
        <div class="watch-main-copy">
          <strong>${esc(item.name)}</strong>
          <span>${esc(item.code)} · ${esc(watchSourceLabel(item))}</span>
        </div>
      </div>
      <div class="watch-item-actions">
        <span class="watch-item-hint">${selectedCodes.has(item.code) ? "已选中" : "点击整行选择"}</span>
        <button class="action-button" type="button" data-watch-analyze="${esc(item.code)}">分析</button>
        <button class="danger-button" type="button" data-watch-delete="${esc(item.code)}" aria-label="删除 ${esc(item.name)}">删除</button>
      </div>
    </div>
  `).join("");
}

function renderOpportunityItems(items, level) {
  if (!items.length) return `<div class="empty">暂无${level === "strong" ? "强机会" : "次机会"}。</div>`;
  return items.map((item, index) => `
    <div class="opp-item ${level}">
      <div class="opp-head">
        <strong>${index + 1}. ${esc(item.name || "未知标的")} (${esc(item.code || "N/A")})</strong>
        <span class="opp-score">${"⭐".repeat(Math.max(1, Math.min(Number(item.score || 0), 5)))}</span>
      </div>
      <div class="summary">${esc(item.ai_advice || "暂无建议")}</div>
      <div class="sub">${esc((item.signals || []).join("、") || "暂无信号")}</div>
    </div>
  `).join("");
}

function renderHeatmap(results) {
  if (!results.length) {
    dom.heatmap.innerHTML = `<div class="empty">先运行一次个股分析，热力图会自动渲染。</div>`;
    return;
  }

  dom.heatmap.innerHTML = results.map((item) => {
    const market = item.market_data || {};
    const change = Number(market.pct_change || 0);
    const toneName = change > 0.8 ? "green" : change < -0.8 ? "red" : "neutral";
    return `
      <div class="heat-item ${toneName}">
        <div>
          <h4>${esc(item.name || "未知股票")}</h4>
          <small>${esc(item.code || "")}</small>
        </div>
        <div>
          <strong>${pct(market.pct_change)}</strong>
          <small>成交额 ${amt(market.turnover)} · 评分 ${esc(item.score ?? 0)}</small>
        </div>
      </div>
    `;
  }).join("");
}

function firstArrayValue(...values) {
  return values.find((value) => Array.isArray(value)) || [];
}

function normalizeRunOncePayload(payload) {
  const rawData = payload?.data && typeof payload.data === "object" ? payload.data : {};
  const normalizedData = {
    ...rawData,
    results: firstArrayValue(rawData.results, payload?.results, rawData.items, payload?.items),
    failed_symbols: firstArrayValue(rawData.failed_symbols, payload?.failed_symbols),
    failed_details: rawData.failed_details && typeof rawData.failed_details === "object"
      ? rawData.failed_details
      : (payload?.failed_details && typeof payload.failed_details === "object" ? payload.failed_details : {}),
    opportunity_rank: firstArrayValue(rawData.opportunity_rank, payload?.opportunity_rank),
    style_distribution: firstArrayValue(rawData.style_distribution, payload?.style_distribution),
    market_sentiment: rawData.market_sentiment || payload?.market_sentiment || {},
    notification: rawData.notification || payload?.notification || {},
    elapsed_seconds: rawData.elapsed_seconds ?? payload?.elapsed_seconds ?? 0,
  };
  return {
    ...payload,
    data: normalizedData,
  };
}

function renderStockResults(results, failedDetails = {}) {
  const visibleResults = persistedStockResults(results);
  if (!results.length) {
    dom.stockResults.innerHTML = `<div class="empty">本次没有返回个股结果。</div>`;
    dom.heatmap.innerHTML = `<div class="empty">本次没有热力图数据。</div>`;
    return;
  }

  const failedEntries = Object.entries(failedDetails || {});
  const failureSummaryCard = failedEntries.length ? `
    <article class="card stock-card error">
      <div class="stock-card-head">
        <div class="stock-title">
          <h3>失败摘要</h3>
          <span>${esc(`${results.length} 只成功 / ${failedEntries.length} 只失败`)}</span>
        </div>
        <div class="stock-score">
          <span>失败数</span>
          <strong>${esc(failedEntries.length)}</strong>
        </div>
      </div>
      <div class="stock-conclusion">本次已返回成功结果；失败股票保留在失败明细里，便于继续排查。</div>
      <div class="stock-tag-row">
        ${failedEntries.map(([code, detail]) => {
          const stage = String(detail?.stage || "unknown").trim();
          const reason = String(detail?.reason || "未知原因").trim();
          return `<span class="stock-tag risk">${esc(`${code} · ${stage} · ${reason}`)}</span>`;
        }).join("")}
      </div>
    </article>
  ` : "";

  if (!visibleResults.length) {
    dom.stockResults.innerHTML = `${failureSummaryCard}<div class="empty">当前分析结果卡片已全部从看板隐藏。重新运行分析或再次打开个股分析后会恢复。</div>`;
    renderHeatmap(results);
    return;
  }

  dom.stockResults.innerHTML = `${failureSummaryCard}${visibleResults.map((item) => {
    const analysis = item.analysis || {};
    const market = item.market_data || {};
    const fund = item.fund_flow || {};
    const cardId = getStockResultId(item);
    const hiddenFund = resolveHiddenFundData(item);
    const isError = item.status !== "ok";
    const changeTone = tone(market.pct_change);
    const fundTone = tone(fund.main_net_inflow);
    const signalTags = Array.isArray(analysis.signal) ? analysis.signal : [];
    const riskTags = Array.isArray(analysis.risk) ? analysis.risk : [];
    const marketNotice = String(market.data_notice || "").trim();
    const isDataIncomplete = Boolean(analysis.data_incomplete || market.is_data_incomplete || isError);
    const dimensionScores = analysis.dimension_scores || {};
    const dimensionItems = [
      ["低位", dimensionScores.low_position],
      ["量能", dimensionScores.volume_change],
      ["趋势", dimensionScores.trend_strength],
      ["资金", dimensionScores.fund_support]
    ];
    const conclusion = analysis.conclusion || item.ai_summary || item.error || "暂无结论";
    const tags = [
      ...(isError ? [`<span class="stock-tag risk">${esc(item.error || item.status || "异常")}</span>`] : []),
      ...(isDataIncomplete ? [`<span class="stock-tag warning">数据不完整</span>`] : []),
      ...signalTags.map((tag) => `<span class="stock-tag signal">${esc(tag)}</span>`),
      ...riskTags.map((tag) => `<span class="stock-tag risk">${esc(tag)}</span>`)
    ].join("") || `<span class="stock-tag neutral">暂无信号</span>`;

    return `
      <article class="card stock-card stock-result-card ${isError ? "error" : ""}" data-stock-result-id="${esc(cardId)}">
        <div class="stock-card-head">
          <div class="stock-title">
            <h3>${esc(item.name || "未知股票")}</h3>
            <span>${esc(item.code || "N/A")}</span>
          </div>
          <div class="stock-card-actions">
            <div class="stock-score">
              <span>评分</span>
              <strong>${esc(item.score ?? 0)}</strong>
            </div>
            ${cardId ? `
              <div class="card-inline-actions" aria-label="卡片操作">
                <button class="card-handle-button" type="button" data-stock-drag-handle aria-label="拖动排序 ${esc(item.name || cardId)}" title="拖动排序">
                  <span aria-hidden="true">⋮⋮</span>
                </button>
                <button class="card-remove-button" type="button" data-stock-hide="${esc(cardId)}" aria-label="删除 ${esc(item.name || cardId)}" title="从当前看板隐藏">
                  <span aria-hidden="true">✕</span>
                </button>
              </div>
            ` : ""}
          </div>
        </div>
        <div class="stock-quote-row">
          <strong class="stock-price ${changeTone}">${stockPriceText(market.latest_price)}</strong>
          <span class="stock-change ${changeTone}">${stockPercentText(market.pct_change)}</span>
        </div>
        ${marketNotice ? `<div class="stock-note">${esc(marketNotice)}</div>` : ""}
        <div class="stock-data-line">
          <span>成交额</span>
          <strong>${stockAmountText(market.turnover)}</strong>
        </div>
        <div class="stock-data-line">
          <span>主力资金</span>
          <strong class="${fundTone}">${stockAmountText(fund.main_net_inflow)}</strong>
        </div>
        <div class="stock-conclusion">${esc(conclusion)}</div>
        <section class="hidden-fund-card" aria-label="暗盘资金">
          <div class="hidden-fund-head">
            <div>
              <strong>📊 暗盘资金</strong>
              <span class="sub">${hiddenFund.hasSignal ? "基于当前分析结果提取的暗盘资金信号" : esc(NO_HIDDEN_FUND_MESSAGE)}</span>
            </div>
            <div class="hidden-fund-score ${hiddenFund.toneClass}">
              <span>${esc(hiddenFund.scoreText)}</span>
              <em>${esc(hiddenFund.tag)}</em>
            </div>
          </div>
          <div class="hidden-fund-meter" aria-hidden="true">
            <span class="hidden-fund-meter-bar ${hiddenFund.toneClass}" style="width:${hiddenFund.meterWidth}%"></span>
          </div>
          <div class="hidden-fund-reason">${esc(hiddenFund.reason)}</div>
          <div class="stock-tag-row">
            ${(hiddenFund.labels.length
              ? hiddenFund.labels.map((label) => `<span class="stock-tag signal">${esc(label)}</span>`).join("")
              : `<span class="stock-tag neutral">${esc(NO_HIDDEN_FUND_MESSAGE)}</span>`)}
          </div>
        </section>
        <div class="stock-dimension-row">
          ${dimensionItems.map(([label, value]) => `
            <span class="stock-dimension">
              <b>${esc(label)}</b>
              <em>${hasNumericValue(value) ? esc(value) : "--"}</em>
            </span>
          `).join("")}
        </div>
        <div class="stock-tag-row">${tags}</div>
      </article>
    `;
  }).join("")}`;

  renderHeatmap(results);
}

function renderHome() {
  const stockData = state.stockPayload?.data || {};
  const sentiment = stockData.market_sentiment || {};
  const detail = sentiment.detail || {};
  const styleDistribution = stockData.style_distribution || [];
  const opportunityRank = stockData.opportunity_rank || [];
  const strong = opportunityRank.filter((item) => item.level === "strong");
  const medium = opportunityRank.filter((item) => item.level === "medium");
  const cards = state.selectedIndexes || [];

  dom.homeMarketStatus.textContent = sentiment.market_status || "--";
  dom.homeAvgChange.textContent = detail.avg_change === undefined ? "--" : pct(detail.avg_change);
  dom.homeTotalInflow.textContent = detail.total_inflow === undefined ? "--" : amt(detail.total_inflow);
  dom.homeEtfCount.textContent = String(cards.length || 0);
  dom.homeMarketSummary.textContent = sentiment.summary || "运行一次个股分析后，这里会显示市场情绪判断。";
  dom.strongOpportunityList.innerHTML = renderOpportunityItems(strong, "strong");
  dom.mediumOpportunityList.innerHTML = renderOpportunityItems(medium, "medium");
  dom.styleChips.innerHTML = styleDistribution.length
    ? styleDistribution.map((item) => `<span class="chip">${esc(item.label)} · ${esc(item.stock_count)}只 · ${amt(item.net_inflow)}</span>`).join("")
    : `<span class="chip">暂无风格分布</span>`;
  dom.homeEtfSummary.textContent = cards.length
    ? `当前指数卡片看板已加载，主面板跟踪：${cards.slice(0, 4).map((item) => item.name).join("、")}`
    : "正在准备指数卡片看板。";
  dom.homeEtfChips.innerHTML = cards.length
    ? cards.slice(0, 4).map((item) => `<span class="chip ${Number(item.change_pct) > 0 ? "good" : Number(item.change_pct) < 0 ? "bad" : "blue"}">${esc(item.name)} · ${pct(item.change_pct)}</span>`).join("")
    : `<span class="chip">暂无指数快照</span>`;
}

function signalTone(signal) {
  if (signal === "推荐") return "good";
  if (signal === "谨慎") return "bad";
  return "blue";
}

function opportunitySymbol(item) {
  return item?.symbol || item?.code || "";
}

function opportunityScoreObject(item) {
  return item?.score && typeof item.score === "object" ? item.score : {};
}

function opportunityScoreValue(item) {
  const score = opportunityScoreObject(item);
  return Number(score.board_total ?? item?.board_total ?? item?.score_value ?? score.total_score ?? item?.score ?? 0);
}

function opportunitySignal(item) {
  const signal = item?.signal || item?.tag;
  if (signal) return signal;
  const level = opportunityScoreObject(item).level || "";
  if (level === "A") return "推荐";
  if (level === "B" || level === "C") return "观察";
  return "谨慎";
}

function opportunityReason(item) {
  const score = opportunityScoreObject(item);
  return item?.reason || score.conclusion || item?.summary || "暂无理由";
}

function opportunitySummary(item) {
  const score = opportunityScoreObject(item);
  return item?.summary || score.conclusion || "暂无总结";
}

function opportunityBoardLabel(item) {
  return item?.board_name || (item?.board ? OPPORTUNITY_BOARD_LABELS[item.board] : "") || "";
}

function opportunityScopeLabel(item) {
  if (state.opportunityMode !== "stock") return "";
  return item?.scope_name || (item?.scope ? OPPORTUNITY_SCOPE_LABELS[item.scope] : "") || OPPORTUNITY_SCOPE_LABELS[state.opportunityScope] || "";
}

function firstSentence(text, fallback = "暂无摘要") {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) return fallback;
  const [sentence] = normalized.split(/[。；;！!？?]/).filter(Boolean);
  const brief = sentence || normalized;
  return brief.length > 46 ? `${brief.slice(0, 46)}...` : brief;
}

function opportunityBrief(item) {
  const summary = opportunitySummary(item);
  const reason = opportunityReason(item);
  const text = summary && summary !== "暂无总结" ? summary : reason;
  return firstSentence(text, "暂无摘要");
}

function opportunityQuoteValue(item, ...keys) {
  const market = item?.market_data && typeof item.market_data === "object" ? item.market_data : {};
  const quote = item?.quote && typeof item.quote === "object" ? item.quote : {};
  for (const key of keys) {
    const value = firstDefinedValue(item?.[key], market?.[key], quote?.[key]);
    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }
  return null;
}

function opportunityPriceText(item) {
  return stockPriceText(opportunityQuoteValue(item, "latest_price", "price", "close"));
}

function opportunityChangeText(item) {
  return stockPercentText(opportunityQuoteValue(item, "pct_change", "change_pct"));
}

function opportunityChangeTone(item) {
  return tone(opportunityQuoteValue(item, "pct_change", "change_pct"));
}

function normalizeOpportunityTag(tag) {
  const text = firstSentence(tag, "");
  if (!text) return "";
  if (text.includes("明显放量") || text.includes("量能放大")) return "明显放量";
  if (text.includes("换手过热")) return "换手过热";
  if (text.includes("站上20日线")) return "站上20日线";
  if (text.includes("接近60日线")) return "接近60日线";
  if (text.includes("60日")) return "接近60日线";
  if (text.includes("低位")) return "低位区";
  return text;
}

function opportunityDecisionTags(item, limit = 4) {
  const score = opportunityScoreObject(item);
  const rawTags = [
    ...(Array.isArray(score.tags) ? score.tags : []),
    ...(Array.isArray(item?.tags) ? item.tags : []),
    ...(Array.isArray(item?.signals) ? item.signals : []),
  ];
  const tags = [];
  rawTags.forEach((tag) => {
    const label = normalizeOpportunityTag(tag);
    if (label && !tags.includes(label)) tags.push(label);
  });

  if (!tags.length) {
    const features = item?.features || {};
    if (features.volume_spike) tags.push("明显放量");
    if (features.turnover_hot) tags.push("换手过热");
    if (features.trend_turn) tags.push("站上20日线");
    if (features.near_ma60) tags.push("接近60日线");
    if (features.stop_falling) tags.push("止跌确认");
    if (features.low_zone) tags.push("低位区");
  }

  const board = String(item?.board || "").trim();
  if ((board === "sz_main" || board === "sh_main") && !tags.includes("主板正式机会")) {
    tags.push("主板正式机会");
  }

  const priority = ["低位区", "明显放量", "换手过热", "站上20日线", "接近60日线", "主板正式机会"];
  return [...tags].sort((a, b) => {
    const aIndex = priority.indexOf(a);
    const bIndex = priority.indexOf(b);
    const normalizedA = aIndex === -1 ? 999 : aIndex;
    const normalizedB = bIndex === -1 ? 999 : bIndex;
    return normalizedA - normalizedB;
  }).slice(0, limit);
}

function opportunityDetailParagraphs(item) {
  const brief = opportunityBrief(item);
  const rows = [
    opportunityReason(item),
    opportunitySummary(item),
    item?.suggestion,
  ].map((text) => String(text || "").trim()).filter((text) => {
    if (!text || ["暂无理由", "暂无总结"].includes(text)) return false;
    return firstSentence(text, "") !== brief;
  });
  return [...new Set(rows)];
}

function opportunityWatchButton(item, compact = false) {
  const symbol = opportunitySymbol(item);
  if (!symbol) return "";
  const active = isWatchlisted(symbol);
  const pending = isWatchMutationPending(symbol);
  const label = pending ? "处理中..." : active ? "已加入" : compact ? "+关注" : "加入自选";
  return `<button class="watch-toggle-button ${active ? "active" : ""}" type="button" data-watch-toggle="${esc(symbol)}" data-watch-name="${esc(item.name || symbol)}" data-watch-board="${esc(item.board || "")}" data-watch-source="opportunity_pool" ${pending ? "disabled" : ""}>${esc(label)}</button>`;
}

function renderOpportunityTabs() {
  if (!dom.opportunityTabs) return;
  dom.opportunityTabs.querySelectorAll("[data-opportunity-tab]").forEach((button) => {
    button.classList.toggle("active", button.dataset.opportunityTab === state.opportunityMode);
  });
  dom.opportunityScopeTabs?.querySelectorAll("[data-opportunity-scope]").forEach((button) => {
    button.classList.toggle("active", button.dataset.opportunityScope === state.opportunityScope);
  });
  dom.opportunityBoardTabs?.querySelectorAll("[data-opportunity-board]").forEach((button) => {
    button.classList.toggle("active", button.dataset.opportunityBoard === state.opportunityBoard);
  });
}

function setOpportunitySectionVisible(element, isVisible) {
  if (!element) return;
  element.hidden = !isVisible;
  element.style.display = isVisible ? "" : "none";
}

function formatOpportunityStatNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) return "--";
  return numeric.toLocaleString("zh-CN");
}

function renderOpportunityScanStats() {
  const container = dom.opportunityScanStats;
  if (!container) return;

  if (state.opportunityMode !== "stock") {
    container.classList.add("hidden");
    return;
  }

  const stats = state.opportunityScanStats || {};
  const meta = state.opportunityCacheMeta || {};
  const hasVisibleStats = ["scanned_total", "prefiltered_total", "history_refined_total", "returned_total"].some((key) => {
    const numeric = Number(stats[key]);
    return Number.isFinite(numeric) && numeric >= 0;
  });

  if (!hasVisibleStats) {
    container.classList.add("hidden");
    return;
  }

  container.classList.remove("hidden");
  const modeLabel = OPPORTUNITY_SCOPE_LABELS[state.opportunityScope] || "全市场";
  if (dom.opportunityScanModeText) {
    dom.opportunityScanModeText.textContent = `当前模式：${modeLabel}`;
  }
  if (dom.opportunityCacheInfoText) {
    const cacheDate = String(meta.cache_date || "").trim();
    const generatedAt = String(meta.generated_at || "").trim();
    const cacheHint = meta.is_cached ? "来自缓存" : "最新生成";
    const parts = ["今日机会池"];
    if (cacheDate) parts.push(cacheDate);
    if (generatedAt) parts.push(`更新时间 ${generatedAt}`);
    parts.push(cacheHint);
    dom.opportunityCacheInfoText.textContent = parts.join(" · ");
  }
  if (dom.opportunityScannedTotal) {
    dom.opportunityScannedTotal.textContent = formatOpportunityStatNumber(stats.scanned_total);
  }
  if (dom.opportunityPrefilteredTotal) {
    dom.opportunityPrefilteredTotal.textContent = formatOpportunityStatNumber(stats.prefiltered_total);
  }
  if (dom.opportunityRefinedTotal) {
    dom.opportunityRefinedTotal.textContent = formatOpportunityStatNumber(stats.history_refined_total);
  }
  if (dom.opportunityReturnedTotal) {
    dom.opportunityReturnedTotal.textContent = formatOpportunityStatNumber(stats.returned_total);
  }
  if (dom.opportunityScanBadges) {
    const badges = [];
    if (meta.used_full_market_scan === true || stats.used_full_market_scan === true) {
      badges.push(`<span class="chip good">真实全市场扫描</span>`);
    }
    if (stats.used_fallback === true) {
      badges.push(`<span class="chip bad">已启用保底结果</span>`);
    }
    const boardLabel = OPPORTUNITY_BOARD_LABELS[stats.board] || OPPORTUNITY_BOARD_LABELS[state.opportunityBoard] || "";
    if (boardLabel) {
      badges.push(`<span class="chip blue">${esc(boardLabel)}</span>`);
    }
    dom.opportunityScanBadges.innerHTML = badges.join("");
  }
  if (dom.opportunityRefreshBtn) {
    const shouldShowRefresh = IS_LOCAL_DEV && state.opportunityMode === "stock" && state.opportunityScope === "market";
    dom.opportunityRefreshBtn.hidden = !shouldShowRefresh;
  }
}

function syncOpportunityModeSections() {
  const isStockMode = state.opportunityMode === "stock";
  setOpportunitySectionVisible(dom.opportunityScopeTabs, isStockMode);
  setOpportunitySectionVisible(dom.opportunityBoardTabs, isStockMode);
  setOpportunitySectionVisible(dom.opportunityDetailPanel, isStockMode);
  renderOpportunityScanStats();
}

function fetchJson(url, options) {
  return fetch(url, options).then(async (response) => {
    const contentType = String(response.headers.get("content-type") || "").toLowerCase();
    const rawText = await response.text();
    const trimmedText = rawText.trim();

    if (!trimmedText) {
      const error = new Error(`接口返回空响应 (${response.status})`);
      error.status = response.status;
      error.responseText = rawText;
      throw error;
    }

    let payload;
    try {
      payload = JSON.parse(trimmedText);
    } catch (_) {
      const compactText = trimmedText.replace(/\s+/g, " ").slice(0, 160);
      const hint = compactText ? `：${compactText}` : "";
      const error = new Error(`接口返回非 JSON 内容 (${response.status}${contentType ? `, ${contentType}` : ""})${hint}`);
      error.status = response.status;
      error.responseText = rawText;
      throw error;
    }

    if (!response.ok || payload?.ok === false) {
      const detail = payload?.error_detail && typeof payload.error_detail === "object"
        ? payload.error_detail
        : {};
      const reason = detail.error || detail.reason || "";
      const suffix = reason ? ` (${reason})` : "";
      const error = new Error(`${payload?.message || "请求失败"}${suffix}`);
      error.status = response.status;
      error.payload = payload;
      error.responseText = rawText;
      throw error;
    }

    return payload;
  }).catch((error) => {
    if (error instanceof Error) throw error;
    throw new Error("请求失败");
  });
}

function styleDirectionText(item) {
  const flow = Number(item?.net_inflow || 0);
  if (flow > 0) return "净流入";
  if (flow < 0) return "净流出";
  return "均衡";
}

function renderStyleDistribution(items = [], summary = {}) {
  if (!dom.styleFlowDistribution) return;
  if (!items.length) {
    dom.styleFlowDistribution.innerHTML = `<div class="empty">暂无风格资金分布数据。</div>`;
    return;
  }

  dom.styleFlowDistribution.innerHTML = items.map((item) => {
    const relatedEtfs = Array.isArray(item.related_etfs) ? item.related_etfs : [];
    const direction = item.direction || "neutral";
    return `
      <article class="style-flow-card ${esc(direction)}">
        <div class="style-flow-card-head">
          <div>
            <h3>${esc(item.style_name || "未命名风格")}</h3>
            <p>${esc(item.description || "暂无说明")}</p>
          </div>
          <span class="chip ${direction === "up" ? "good" : direction === "down" ? "bad" : "blue"}">${esc(styleDirectionText(item))}</span>
        </div>
        <div class="style-flow-main ${direction}">${amt(item.net_inflow)}</div>
        <div class="style-flow-sub">${esc(item.summary || "暂无总结")}</div>
        <div class="style-flow-metrics">
          <div class="style-flow-metric"><small>平均涨跌幅</small><strong class="${tone(item.avg_change_pct)}">${pct(item.avg_change_pct)}</strong></div>
          <div class="style-flow-metric"><small>主力资金流</small><strong class="${tone(item.main_flow)}">${amt(item.main_flow)}</strong></div>
          <div class="style-flow-metric"><small>成交额</small><strong>${amt(item.turnover)}</strong></div>
          <div class="style-flow-metric"><small>关联ETF</small><strong>${esc(relatedEtfs.length)}</strong></div>
          <div class="style-flow-metric"><small>博弈强度</small><strong>${esc(item.strength_score ?? 0)}</strong></div>
        </div>
      </article>
    `;
  }).join("");

  const updatedAt = summary?.updated_at || "未知时间";
  setChip(dom.styleFlowMeta, `更新于 ${updatedAt} · 总净流 ${amt(summary?.total_net_inflow ?? 0)}`, "blue");
}

function renderStyleIntent(intent = {}, summary = {}) {
  if (!dom.styleIntentPanel) return;
  if (!intent || !Object.keys(intent).length) {
    dom.styleIntentPanel.innerHTML = `<div class="empty">暂无风格博弈意图数据。</div>`;
    return;
  }

  const labels = Array.isArray(intent.signal_labels) ? intent.signal_labels : [];
  const modeTone = Number(intent.core_value) > 0 ? "good" : Number(intent.core_value) < 0 ? "bad" : "blue";
  dom.styleIntentPanel.innerHTML = `
    <div class="style-intent-shell">
      <div class="style-intent-head">
        <div>
          <div class="style-intent-title-row">
            <h3>${esc(intent.mode || "风格意图待判断")}</h3>
            <span class="chip ${modeTone}">${esc(intent.mode_key || "--")}</span>
          </div>
          <p>${esc(intent.summary || "暂无总结")}</p>
        </div>
      </div>
      <div class="style-intent-stats">
        <div class="style-intent-stat">
          <small>攻防差值</small>
          <strong class="${tone(intent.core_value)}">${num(intent.core_value)}</strong>
        </div>
        <div class="style-intent-stat">
          <small>进攻分数</small>
          <strong>${esc(intent.attack_score ?? "--")}</strong>
        </div>
        <div class="style-intent-stat">
          <small>防御分数</small>
          <strong>${esc(intent.defense_score ?? "--")}</strong>
        </div>
      </div>
      <div class="style-intent-indicators">
        <article class="style-intent-indicator">
          <small>风险等级</small>
          <strong>${esc(intent.risk_level || "--")}</strong>
          <p>结合攻防强弱后的 V1 风险口径。</p>
        </article>
        <article class="style-intent-indicator">
          <small>最强流入风格</small>
          <strong>${esc(intent.top_inflow_style || summary?.top_inflow_style || "--")}</strong>
          <p>当前净流入最强的风格方向。</p>
        </article>
        <article class="style-intent-indicator">
          <small>相对流出风格</small>
          <strong>${esc(intent.top_outflow_style || summary?.top_outflow_style || "--")}</strong>
          <p>当前净流出或承压更明显的方向。</p>
        </article>
      </div>
      <div class="chips">
        ${(labels.length ? labels : ["暂无信号标签"]).map((text) => `<span class="chip">${esc(text)}</span>`).join("")}
      </div>
      <div class="style-flow-sub">更新时间：${esc(intent.update_time || summary?.updated_at || "--")}</div>
    </div>
  `;
}

function renderStyleEtfGroups(styles = []) {
  if (!dom.styleEtfGroups) return;
  if (!styles.length) {
    dom.styleEtfGroups.innerHTML = `<div class="empty">暂无关联 ETF 数据。</div>`;
    return;
  }

  dom.styleEtfGroups.innerHTML = styles.map((style) => {
    const etfs = Array.isArray(style.related_etfs) ? style.related_etfs : [];
    const direction = style.direction || "neutral";
    return `
      <section class="style-etf-group">
        <div class="style-etf-group-head">
          <div>
            <h3>${esc(style.style_name || "未命名风格")}</h3>
            <p>${esc(style.summary || style.description || "暂无说明")}</p>
          </div>
          <span class="chip ${direction === "up" ? "good" : direction === "down" ? "bad" : "blue"}">${esc(styleDirectionText(style))}</span>
        </div>
        <div class="style-etf-list">
          ${etfs.map((etf) => `
            <button
              class="style-etf-chip"
              type="button"
              data-style-etf-code="${esc(etf.code || "")}"
              data-style-etf-name="${esc(etf.name || "")}"
              data-style-index-code="${esc(etf.index_code || "")}"
            >
              <span class="style-etf-chip-title">${esc(etf.name || etf.code || "ETF")}</span>
              <span class="style-etf-chip-sub ${tone(etf.pct_change)}">${esc(etf.code || "")} · ${pct(etf.pct_change)} · ${amt(etf.main_net_inflow)}</span>
            </button>
          `).join("")}
        </div>
      </section>
    `;
  }).join("");
}

async function openStyleEtfLink(code, name, indexCode = "") {
  const normalizedCode = String(code || "").trim();
  if (!normalizedCode) return;
  if (indexCode) {
    switchView("etfView");
    await loadIndexDetail(indexCode, true);
    showToast(`${name || normalizedCode} 已联动到指数ETF页`, "good");
    return;
  }
  window.open(apiPath(`/api/etf/analyze?code=${encodeURIComponent(normalizedCode)}`), "_blank", "noopener");
}

async function loadStyleFlowWidgets(forceRefresh = false) {
  if (!dom.styleFlowStatus) return;
  try {
    setChip(dom.styleFlowStatus, forceRefresh ? "正在刷新风格数据..." : "正在加载风格数据...", "blue");
    if (dom.styleFlowDistribution) {
      dom.styleFlowDistribution.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在计算风格资金分布...</div></div>`;
    }
    if (dom.styleIntentPanel) {
      dom.styleIntentPanel.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在推导风格博弈意图...</div></div>`;
    }

    const [flowPayload, intentPayload] = await Promise.all([
      fetchJson(api.styleFundFlow),
      fetchJson(api.styleIntent),
    ]);

    state.styleFlowPayload = flowPayload;
    state.styleIntentPayload = intentPayload;

    const styles = flowPayload.data?.styles || flowPayload.data?.items || [];
    const intent = intentPayload.data?.item || {};
    const summary = {
      ...(flowPayload.data?.summary || {}),
      ...(intentPayload.data?.summary || {}),
    };

    renderStyleDistribution(styles, summary);
    renderStyleIntent(intent, summary);
    renderStyleEtfGroups(styles);

    setChip(
      dom.styleFlowStatus,
      `已加载 ${styles.length} 个风格 · ${summary?.updated_at || "刚刚"}`,
      "good",
    );
  } catch (error) {
    setChip(dom.styleFlowStatus, `加载失败：${error.message}`, "bad");
    if (dom.styleFlowMeta) {
      setChip(dom.styleFlowMeta, `加载失败：${error.message}`, "bad");
    }
    if (dom.styleFlowDistribution) {
      dom.styleFlowDistribution.innerHTML = `<div class="empty">风格资金分布加载失败：${esc(error.message)}</div>`;
    }
    if (dom.styleIntentPanel) {
      dom.styleIntentPanel.innerHTML = `<div class="empty">风格博弈意图加载失败：${esc(error.message)}</div>`;
    }
    if (dom.styleEtfGroups) {
      dom.styleEtfGroups.innerHTML = `<div class="empty">关联 ETF 列表加载失败：${esc(error.message)}</div>`;
    }
  }
}

function buildSparkline(points, width = 220, height = 40) {
  if (!Array.isArray(points) || points.length < 2) return "";
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(max - min, 1e-6);
  const stepX = width / (points.length - 1);
  return points.map((point, index) => {
    const x = (index * stepX).toFixed(2);
    const y = (height - ((point - min) / range) * (height - 6) - 3).toFixed(2);
    return `${index === 0 ? "M" : "L"}${x} ${y}`;
  }).join(" ");
}

function buildAreaChart(points, width = 760, height = 220) {
  if (!Array.isArray(points) || points.length < 2) return { line: "", area: "" };
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(max - min, 1e-6);
  const stepX = width / (points.length - 1);
  const coords = points.map((point, index) => {
    const x = index * stepX;
    const y = height - ((point - min) / range) * (height - 26) - 13;
    return [x, y];
  });
  const line = coords.map(([x, y], index) => `${index === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`).join(" ");
  return { line, area: `${line} L ${width} ${height} L 0 ${height} Z` };
}

function renderRecommendedOpportunity() {
  const item = state.recommendedOpportunity;
  if (!item) {
    dom.opportunityRecommendCard.innerHTML = `<div class="empty">当前筛选条件下暂未发现明确机会，可切换范围或板块继续查看。</div>`;
    return;
  }

  const symbol = opportunitySymbol(item);
  const scoreValue = opportunityScoreValue(item);
  const signal = opportunitySignal(item);
  const scopeLabel = opportunityScopeLabel(item);
  const boardLabel = opportunityBoardLabel(item);
  const decisionTags = opportunityDecisionTags(item, 4);
  const priceText = opportunityPriceText(item);
  const changeText = opportunityChangeText(item);
  const changeTone = opportunityChangeTone(item);
  dom.opportunityRecommendCard.innerHTML = `
    <div class="recommend-main">
      <div class="opportunity-card-top">
        <div>
          <div class="chips">
            <span class="chip ${signalTone(signal)}">${esc(signal)}</span>
            <span class="chip">今日先看</span>
            ${boardLabel ? `<span class="chip blue">${esc(boardLabel)}</span>` : ""}
            ${scopeLabel ? `<span class="chip">${esc(scopeLabel)}</span>` : ""}
          </div>
          <h3>${esc(item.name)}</h3>
          <div class="opportunity-code">${esc(symbol)}</div>
        </div>
        <div class="opportunity-card-actions">
          ${opportunityWatchButton(item)}
          <div class="recommend-score">
            <div>
              <small class="sub">综合评分</small>
              <strong>${esc(scoreValue)}</strong>
            </div>
          </div>
        </div>
      </div>
      <div class="opportunity-quote-row">
        <strong class="opportunity-price">${esc(priceText)}</strong>
        <span class="opportunity-change ${changeTone}">${esc(changeText)}</span>
      </div>
      <div class="recommend-text">${esc(opportunityBrief(item))}</div>
      ${decisionTags.length ? `
        <div class="chips">
          ${decisionTags.map((tag) => `<span class="chip blue">${esc(tag)}</span>`).join("")}
        </div>
      ` : ""}
    </div>
  `;
}

function renderOpportunityPool() {
  const items = state.lowOpportunityItems || [];
  if (!items.length) {
    dom.opportunityPoolList.innerHTML = `<div class="empty">当前筛选条件下暂无低位机会，可切换范围或板块查看更多候选。</div>`;
    return;
  }

  if (state.opportunityMode === "stock" && items.length === 1) {
    const item = items[0] || {};
    const scopeLabel = opportunityScopeLabel(item);
    const boardLabel = opportunityBoardLabel(item);
    dom.opportunityPoolList.innerHTML = `
      <div class="empty">
        <div class="chips" style="justify-content: center; margin-bottom: 10px;">
          ${scopeLabel ? `<span class="chip">${esc(scopeLabel)}</span>` : ""}
          ${boardLabel ? `<span class="chip blue">${esc(boardLabel)}</span>` : ""}
          ${item.badge ? `<span class="chip">${esc(item.badge)}</span>` : ""}
        </div>
        <strong>当前板块仅筛出 1 条机会</strong>
        <p style="margin: 8px 0 0;">已在左侧推荐和下方详情区展示，可切换其他板块查看更多候选。</p>
      </div>
    `;
    return;
  }

  const boardNote = state.opportunityMode === "stock" && state.opportunityBoard === "all"
    ? `
      <div class="empty" style="padding: 14px; text-align: left;">
        <strong>全部板块已按板块策略汇总</strong>
        <p style="margin: 6px 0 0;">卡片上的板块标签用于区分来源，可切换单一板块进一步聚焦。</p>
      </div>
    `
    : "";

  const cards = items.map((item, index) => {
    const symbol = opportunitySymbol(item);
    const scoreValue = opportunityScoreValue(item);
    const boardLabel = opportunityBoardLabel(item);
    const tags = opportunityDecisionTags(item, 4);
    const priceText = opportunityPriceText(item);
    const changeText = opportunityChangeText(item);
    const changeTone = opportunityChangeTone(item);
    return `
    <article class="opportunity-pool-item ${state.activeOpportunityCode === symbol ? "active" : ""}" data-opportunity-code="${esc(symbol)}">
      <div class="opportunity-card-top">
        <div>
          <h3>${esc(item.name)}</h3>
          <div class="opportunity-code">${esc(symbol)}${boardLabel ? ` · ${esc(boardLabel)}` : ""}</div>
        </div>
        <div class="opportunity-card-actions">
          ${opportunityWatchButton(item, true)}
          <div class="opportunity-score">${esc(scoreValue)}</div>
        </div>
      </div>
      <div class="opportunity-quote-row">
        <strong class="opportunity-price">${esc(priceText)}</strong>
        <span class="opportunity-change ${changeTone}">${esc(changeText)}</span>
      </div>
      <div class="opportunity-reason">${esc(opportunityBrief(item))}</div>
      <div class="chips">
        <span class="chip">第 ${esc(index + 1)}</span>
        ${tags.map((tag) => `<span class="chip blue">${esc(tag)}</span>`).join("")}
      </div>
    </article>
  `;
  }).join("");
  dom.opportunityPoolList.innerHTML = `${boardNote}${cards}`;
}

function renderOpportunityDetail() {
  if (state.opportunityMode !== "stock") {
    if (dom.opportunityDetailPanel) {
      dom.opportunityDetailPanel.innerHTML = "";
      setOpportunitySectionVisible(dom.opportunityDetailPanel, false);
    }
    return;
  }

  setOpportunitySectionVisible(dom.opportunityDetailPanel, true);

  const item = state.activeOpportunityDetail;
  if (!item) {
    dom.opportunityDetailPanel.innerHTML = `<div class="empty">点击某张机会卡片后，这里显示详细信息。</div>`;
    return;
  }

  const metrics = item.metrics || {};
  const features = item.features || {};
  const drawdown = metrics.drawdown || features.drawdown || "--";
  const trend = metrics.trend || (features.trend_turn ? "均线拐头" : "仍待确认");
  const risk = metrics.risk || features.risk || "--";
  const symbol = opportunitySymbol(item);
  const scoreValue = opportunityScoreValue(item);
  const signal = opportunitySignal(item);
  const score = opportunityScoreObject(item);
  const subScores = score.sub_scores || {};
  const tags = score.tags || item.tags || item.signals || [];
  const scopeLabel = opportunityScopeLabel(item);
  const boardLabel = opportunityBoardLabel(item);
  const detailParagraphs = opportunityDetailParagraphs(item);
  const decisionTags = opportunityDecisionTags(item, 4);
  const priceText = opportunityPriceText(item);
  const changeText = opportunityChangeText(item);
  const changeTone = opportunityChangeTone(item);
  dom.opportunityDetailPanel.innerHTML = `
    <div class="chips">
      <span class="chip ${signalTone(signal)}">${esc(signal)}</span>
      <span class="chip blue">综合评分 ${esc(scoreValue)}</span>
      ${scopeLabel ? `<span class="chip">${esc(scopeLabel)}</span>` : ""}
      ${boardLabel ? `<span class="chip blue">${esc(boardLabel)}</span>` : ""}
      ${item.badge ? `<span class="chip">${esc(item.badge)}</span>` : ""}
      ${score.level ? `<span class="chip blue">等级 ${esc(score.level)}</span>` : ""}
    </div>
    <div style="margin-top:14px;">
      <h3>${esc(item.name)}</h3>
      <div class="sub">${esc(symbol)}</div>
    </div>
    <div class="opportunity-detail-head">
      <div class="opportunity-quote-row">
        <strong class="opportunity-price">${esc(priceText)}</strong>
        <span class="opportunity-change ${changeTone}">${esc(changeText)}</span>
      </div>
      <div class="opportunity-detail-actions">
        ${opportunityWatchButton(item)}
        <button class="action-button" type="button" data-opportunity-open-stock="${esc(symbol)}" data-opportunity-stock-name="${esc(item.name || symbol)}">打开个股分析</button>
      </div>
    </div>
    <div style="margin-top:12px;">
      <div class="sub">详细分析</div>
      ${detailParagraphs.length
        ? detailParagraphs.map((text) => `<div class="recommend-text" style="margin-top:8px;">${esc(text)}</div>`).join("")
        : `<div class="recommend-text" style="margin-top:8px;">暂无更多展开说明，可重点参考下方低位、量能、趋势和资金细项。</div>`}
    </div>
    <div class="opportunity-metrics">
      <div class="opportunity-metric">
        <small>回撤幅度</small>
        <strong>${esc(drawdown)}</strong>
      </div>
      <div class="opportunity-metric">
        <small>趋势判断</small>
        <strong>${esc(trend)}</strong>
      </div>
      <div class="opportunity-metric">
        <small>风险等级</small>
        <strong>${esc(risk)}</strong>
      </div>
    </div>
    <div class="chips" style="margin-top:14px;">
      <span class="chip">低位 ${esc(subScores.low ?? "--")}</span>
      <span class="chip">量能 ${esc(subScores.volume ?? "--")}</span>
      <span class="chip">趋势 ${esc(subScores.trend ?? "--")}</span>
      <span class="chip">资金 ${esc(subScores.capital ?? "--")}</span>
    </div>
    ${decisionTags.length ? `
      <div class="chips" style="margin-top:14px;">
        ${decisionTags.map((tag) => `<span class="chip blue">${esc(tag)}</span>`).join("")}
      </div>
    ` : ""}
    ${tags.length ? `
      <div class="chips" style="margin-top:14px;">
        ${tags.map((tag) => `<span class="chip blue">${esc(tag)}</span>`).join("")}
      </div>
    ` : ""}
    ${Object.keys(features).length ? `
      <div class="chips" style="margin-top:14px;">
        <span class="chip ${features.volume_spike ? "good" : "blue"}">量能放大 ${features.volume_spike ? "是" : "否"}</span>
        <span class="chip ${features.stop_falling ? "good" : "blue"}">止跌 ${features.stop_falling ? "是" : "否"}</span>
        <span class="chip ${features.bullish_break ? "good" : "blue"}">放量阳线 ${features.bullish_break ? "是" : "否"}</span>
      </div>
    ` : ""}
  `;
}

function getIndexByCode(code) {
  return state.selectedIndexes.find((item) => item.code === code)
    || state.availableIndexes.find((item) => item.code === code)
    || null;
}

function renderIndexBoard() {
  const cards = state.selectedIndexes;
  if (!cards.length) {
    dom.indexBoardGrid.innerHTML = `<div class="empty">当前没有已添加的指数卡片，请在下方直接添加指数。</div>`;
    dom.indexQuickChips.innerHTML = `<span class="chip">0 项</span>`;
    return;
  }

  const positiveCount = cards.filter((item) => Number(item.change_pct) > 0).length;
  const negativeCount = cards.filter((item) => Number(item.change_pct) < 0).length;
  dom.indexQuickChips.innerHTML = `
    <span class="chip blue">已显示 ${cards.length} 项</span>
    <span class="chip ${positiveCount ? "good" : "blue"}">上涨 ${positiveCount}</span>
    <span class="chip ${negativeCount ? "bad" : "blue"}">下跌 ${negativeCount}</span>
  `;

  dom.indexBoardGrid.innerHTML = cards.map((item) => {
    const line = buildSparkline(item.sparkline || []);
    return `
      <article class="index-mini-card index-board-item ${state.activeIndexCode === item.code ? "active" : ""}" data-index-code="${esc(item.code)}" data-index-card-id="${esc(getIndexCardId(item))}">
        <div class="index-card-top">
          <div>
            <h3 class="index-card-name">${esc(item.name)}</h3>
            <div class="index-card-category">${esc(item.category || "宽基")}</div>
          </div>
          <div class="index-card-actions">
            <span class="chip blue">${esc(item.category || "宽基")}</span>
            <div class="card-inline-actions" aria-label="卡片操作">
              <button class="card-handle-button" type="button" data-index-drag-handle aria-label="拖动排序 ${esc(item.name || item.code)}" title="拖动排序">
                <span aria-hidden="true">⋮⋮</span>
              </button>
              <button class="card-remove-button" type="button" data-index-hide="${esc(getIndexCardId(item))}" aria-label="删除 ${esc(item.name || item.code)}" title="从当前看板隐藏">
                <span aria-hidden="true">✕</span>
              </button>
            </div>
          </div>
        </div>
        <strong class="index-card-value ${tone(item.change_pct)}">${num(item.value)}</strong>
        <div class="index-card-change ${tone(item.change_pct)}">
          <span>${num(item.change)}</span>
          <span>${pct(item.change_pct)}</span>
        </div>
        <div class="sparkline-wrap">
          ${line
            ? `<svg class="sparkline" viewBox="0 0 220 40" preserveAspectRatio="none"><path d="${line}" fill="none" stroke="${Number(item.change_pct) >= 0 ? "#ff7a84" : "#39d39e"}" stroke-width="2.2" stroke-linecap="round"/></svg>`
            : `<div class="sparkline-placeholder">走势数据准备中</div>`}
        </div>
      </article>
    `;
  }).join("");
}

function renderIndexAddPanel() {
  const selectedCodes = new Set(state.selectedIndexes.map((item) => item.code));
  const addableIndexes = state.availableIndexes.filter((item) => !selectedCodes.has(item.code));
  dom.availableIndexCount.textContent = `${addableIndexes.length} 项可添加`;

  const options = addableIndexes.map((item) => {
    return `
      <button class="available-pill" type="button" data-index-code="${esc(item.code)}" aria-label="添加 ${esc(item.name)}">
        ${esc(item.name)}
      </button>
    `;
  }).join("");
  dom.availableIndexPool.innerHTML = options || `<div class="empty">当前可选指数已全部加入看板。</div>`;
}

function ensureActiveIndexAfterSelectionChange() {
  if (!state.selectedIndexes.length) {
    state.activeIndexCode = "";
    state.activeIndexDetail = null;
    return;
  }
  if (!state.activeIndexCode || !state.selectedIndexes.some((item) => item.code === state.activeIndexCode)) {
    state.activeIndexCode = state.selectedIndexes[0].code;
    state.activeIndexDetail = null;
  }
}

async function syncSelectedIndexes(nextSelected, shouldLoadDetail = true) {
  state.selectedIndexes = nextSelected;
  persistIndexBoardState();
  ensureActiveIndexAfterSelectionChange();
  renderIndexBoard();
  renderIndexAddPanel();
  renderHome();

  if (!state.selectedIndexes.length) {
    detailError("当前没有已添加的指数，请在下方直接添加指数。");
    setChip(dom.indexSelectionStatus, "当前没有选中指数", "bad");
    return;
  }

  if (shouldLoadDetail) {
    await loadIndexDetail(state.activeIndexCode, true);
  }
}

function detailLoading() {
  dom.indexDetailPanel.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在加载指数详情...</div></div>`;
}

function detailError(message) {
  dom.indexDetailPanel.innerHTML = `<div class="empty">${esc(message || "指数详情加载失败，请稍后重试。")}</div>`;
}

function renderIndexDetail(detail) {
  if (!detail) {
    detailError("请选择一个指数查看今日信息。");
    return;
  }

  const chart = buildAreaChart(detail.sparkline || []);
  const etf = detail.etf || {};

  dom.indexDetailPanel.innerHTML = `
    <div class="detail-surface">
      <div class="detail-top">
        <div>
          <h2 class="detail-title">${esc(detail.name || "未知指数")}</h2>
          <div class="detail-sub">${esc(detail.code || "")} · ${esc(detail.style || "风格待更新")}</div>
        </div>
        <div class="detail-status">
          <span class="chip ${Number(detail.change_pct) > 0 ? "good" : Number(detail.change_pct) < 0 ? "bad" : "blue"}">${esc(detail.signal || "震荡")}</span>
          <span class="chip blue">振幅 ${esc(detail.amplitude || "--")}</span>
        </div>
      </div>

      <div class="detail-layout">
        <div class="detail-chart-block">
          <h3>今日波动情况</h3>
          <p>这里展示当前选中指数的点位变化和今日结构，帮助快速判断今天是修复、震荡还是承压。</p>
          <div class="detail-chart-shell">
            ${chart.line
              ? `
                <svg class="chart-svg" viewBox="0 0 760 220" preserveAspectRatio="none">
                  <defs>
                    <linearGradient id="indexChartFill" x1="0" x2="0" y1="0" y2="1">
                      <stop offset="0%" stop-color="${Number(detail.change_pct) >= 0 ? "rgba(255,122,132,0.36)" : "rgba(57,211,158,0.32)"}"></stop>
                      <stop offset="100%" stop-color="rgba(0,0,0,0)"></stop>
                    </linearGradient>
                  </defs>
                  <path d="${chart.area}" fill="url(#indexChartFill)"></path>
                  <path d="${chart.line}" fill="none" stroke="${Number(detail.change_pct) >= 0 ? "#ff7a84" : "#39d39e"}" stroke-width="3" stroke-linecap="round"></path>
                </svg>
              `
              : `<div class="empty">暂无走势图</div>`}
            <div class="detail-axis">
              <span>开盘</span>
              <span>盘中</span>
              <span>收盘前</span>
            </div>
          </div>

          <div class="detail-stats">
            <div class="detail-stat"><small>当前点位</small><strong>${num(detail.value)}</strong></div>
            <div class="detail-stat"><small>涨跌额 / 涨跌幅</small><strong class="${tone(detail.change_pct)}">${num(detail.change)} / ${pct(detail.change_pct)}</strong></div>
            <div class="detail-stat"><small>市场风格</small><strong>${esc(detail.style || "待更新")}</strong></div>
            <div class="detail-stat"><small>信号判断</small><strong>${esc(detail.signal || "震荡")}</strong></div>
          </div>
        </div>

        <div class="detail-side-stack">
          <div class="side-card">
            <h3>市场风格或信号判断</h3>
            <p>${esc(detail.summary || "暂无总结。")}</p>
          </div>
          <div class="side-card">
            <h3>对应 ETF</h3>
            <p><strong>${esc(etf.name || "暂无关联 ETF")}</strong> ${etf.code ? `(${esc(etf.code)})` : ""}</p>
          </div>
          <div class="side-card">
            <h3>对应 ETF 简短建议</h3>
            <p>${esc(etf.suggestion || "适合继续观察，不宜追高。")}</p>
          </div>
        </div>
      </div>
    </div>
  `;
}

function mergeSelectedWithLatest(currentSelected, latestPool) {
  const poolMap = new Map(latestPool.map((item) => [item.code, item]));
  return currentSelected.map((item) => poolMap.get(item.code) || item).filter(Boolean);
}

async function initializeIndexBoard(forceDefaults = false) {
  setChip(dom.indexBoardStatus, "正在加载指数卡片...", "blue");
  detailLoading();

  const [indexesPayload, optionsPayload] = await Promise.all([
    fetchJson(api.indexes),
    fetchJson(api.indexOptions)
  ]);

  const defaultIndexes = indexesPayload.data?.indexes || [];
  const allOptions = optionsPayload.data?.options || defaultIndexes;

  state.availableIndexes = allOptions;
  if (forceDefaults) {
    clearIndexBoardPersistence();
    state.selectedIndexes = defaultIndexes;
  } else {
    state.selectedIndexes = buildPersistedIndexSelection(defaultIndexes, allOptions);
  }

  persistIndexBoardState();
  ensureActiveIndexAfterSelectionChange();
  renderIndexBoard();
  renderIndexAddPanel();
  renderHome();

  if (state.selectedIndexes.length) {
    setChip(dom.indexBoardStatus, `已加载 ${state.selectedIndexes.length} 张指数卡片`, "good");
    await loadIndexDetail(state.activeIndexCode || state.selectedIndexes[0].code, true);
  } else {
    setChip(dom.indexBoardStatus, "当前没有可展示的指数卡片", "bad");
    detailError("指数卡片为空，请在下方添加指数。");
  }
}

async function loadIndexDetail(code, force = false) {
  if (!code) return;
  if (!force && state.activeIndexCode === code && state.activeIndexDetail) return;

  state.activeIndexCode = code;
  renderIndexBoard();
  setChip(dom.indexSelectionStatus, `当前选中：${code}`, "blue");
  detailLoading();

  try {
    const payload = await fetchJson(api.indexDetail(code));
    state.activeIndexDetail = payload.data?.index || null;
    renderIndexDetail(state.activeIndexDetail);
  } catch (error) {
    state.activeIndexDetail = null;
    detailError(`指数详情加载失败：${error.message}`);
    setChip(dom.indexSelectionStatus, `详情加载失败：${error.message}`, "bad");
  }
}

async function refreshIndexBoardData(forceDefaults = false) {
  try {
    await initializeIndexBoard(forceDefaults);
  } catch (error) {
    setChip(dom.indexBoardStatus, `加载失败：${error.message}`, "bad");
    detailError(`指数数据加载失败：${error.message}`);
    dom.indexBoardGrid.innerHTML = `<div class="empty">指数卡片加载失败：${esc(error.message)}</div>`;
  }
}

async function loadOpportunityDetail(code, force = false) {
  if (state.opportunityMode !== "stock") return;
  if (!code) return;
  if (!force && state.activeOpportunityCode === code && state.activeOpportunityDetail) return;

  state.activeOpportunityCode = code;
  renderOpportunityPool();
  dom.opportunityDetailPanel.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在加载机会详情...</div></div>`;
  const target = state.lowOpportunityItems.find((item) => opportunitySymbol(item) === code) || null;
  state.activeOpportunityDetail = target;
  if (target) {
    state.recommendedOpportunity = target;
    renderRecommendedOpportunity();
  }
  renderOpportunityDetail();
}

function syncOpportunityView(items, mode) {
  state.opportunityMode = mode;
  state.lowOpportunityItems = [...items];
  state.activeOpportunityCode = "";
  state.activeOpportunityDetail = null;
  state.recommendedOpportunity = [...items]
    .sort((left, right) => opportunityScoreValue(right) - opportunityScoreValue(left))[0] || null;
  renderOpportunityTabs();
  syncOpportunityModeSections();
  renderRecommendedOpportunity();
  renderOpportunityPool();
  renderOpportunityDetail();
  renderOpportunityScanStats();
}

async function applyOpportunityMode(mode, board = state.opportunityBoard, scope = state.opportunityScope, forceRefresh = false) {
  const requestId = ++state.opportunityRequestId;
  if (mode === "stock") {
    state.opportunityScope = scope;
    state.opportunityBoard = board;
    state.opportunityMode = "stock";
    state.opportunityScanStats = null;
    state.opportunityCacheMeta = null;
    renderOpportunityTabs();
    syncOpportunityModeSections();
    const scopeLabel = OPPORTUNITY_SCOPE_LABELS[scope] || "全市场";
    const boardLabel = OPPORTUNITY_BOARD_LABELS[board] || "全部";
    setChip(dom.opportunityStatus, forceRefresh ? `正在刷新${scopeLabel} · ${boardLabel}今日机会池...` : `正在加载${scopeLabel} · ${boardLabel}个股机会...`, "blue");
    dom.opportunityRecommendCard.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在准备个股推荐...</div></div>`;
    dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载中...</div>`;
    dom.opportunityDetailPanel.innerHTML = `<div class="empty">点击某张个股机会卡片后，这里显示详细信息。</div>`;

    try {
      const payload = await fetchJson(api.opportunities(scope, board, forceRefresh));
      if (requestId !== state.opportunityRequestId || state.opportunityMode !== "stock") return;
      state.opportunityScope = payload.data?.scope || scope;
      state.opportunityBoard = payload.data?.board || board;
      state.opportunityScanStats = payload.data?.scan_stats || null;
      state.opportunityCacheMeta = {
        cache_date: payload.data?.cache_date || "",
        generated_at: payload.data?.generated_at || "",
        is_cached: Boolean(payload.data?.is_cached),
        used_full_market_scan: Boolean(payload.data?.used_full_market_scan),
      };
      state.stockOpportunityItems = payload.data?.items || [];
      syncOpportunityView(state.stockOpportunityItems, "stock");
      const nextCode = opportunitySymbol(state.recommendedOpportunity) || opportunitySymbol(state.lowOpportunityItems[0]) || "";
      if (nextCode) {
        await loadOpportunityDetail(nextCode, true);
      }
      const loadedScopeLabel = payload.data?.scope_name || OPPORTUNITY_SCOPE_LABELS[state.opportunityScope] || scopeLabel;
      const loadedBoardLabel = OPPORTUNITY_BOARD_LABELS[state.opportunityBoard] || boardLabel;
      const scanStats = payload.data?.scan_stats || {};
      const scannedTotal = Number(scanStats.scanned_total || 0);
      const refinedTotal = Number(scanStats.history_refined_total || 0);
      const statusParts = [`已加载 ${state.lowOpportunityItems.length} 个${loadedScopeLabel} · ${loadedBoardLabel}个股机会`];
      if (scannedTotal > 0) {
        statusParts.push(`扫描 ${scannedTotal} 只`);
      }
      if (refinedTotal > 0 && refinedTotal !== scannedTotal) {
        statusParts.push(`精算 ${refinedTotal} 只`);
      }
      setChip(dom.opportunityStatus, statusParts.join("，"), "good");
      return;
    } catch (error) {
      if (requestId !== state.opportunityRequestId || state.opportunityMode !== "stock") return;
      state.opportunityScanStats = null;
      state.opportunityCacheMeta = null;
      renderOpportunityScanStats();
      setChip(dom.opportunityStatus, `个股机会加载失败：${error.message}`, "bad");
      dom.opportunityRecommendCard.innerHTML = `<div class="empty">个股推荐加载失败：${esc(error.message)}</div>`;
      dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载失败：${esc(error.message)}</div>`;
      dom.opportunityDetailPanel.innerHTML = `<div class="empty">个股机会详情暂不可用。</div>`;
      return;
    }
  }

  state.opportunityMode = "index";
  state.opportunityScanStats = null;
  state.opportunityCacheMeta = null;
  syncOpportunityView(state.indexOpportunityItems, "index");
  setChip(dom.opportunityStatus, `静态演示已加载 ${state.lowOpportunityItems.length} 个指数机会`, "good");
}

async function loadOpportunityWidgets() {
  setChip(dom.opportunityStatus, "正在准备低位机会池...", "blue");
  dom.opportunityRecommendCard.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在准备自动推荐...</div></div>`;
  dom.opportunityPoolList.innerHTML = `<div class="empty">低位机会列表加载中...</div>`;
  dom.opportunityDetailPanel.innerHTML = `<div class="empty">点击某张机会卡片后，这里显示详细信息。</div>`;

  state.indexOpportunityItems = [...STATIC_LOW_OPPORTUNITY_ITEMS];
  await applyOpportunityMode("stock");
}

async function addIndexToBoard(code) {
  if (state.selectedIndexes.some((item) => item.code === code)) return;
  const target = getIndexByCode(code);
  if (!target) return;
  state.hiddenIndexCodes = removeIds(state.hiddenIndexCodes, [code]);
  await syncSelectedIndexes([...state.selectedIndexes, target], false);
  if (!state.activeIndexCode) {
    await loadIndexDetail(target.code, true);
  }
}

async function removeIndexFromBoard(code) {
  state.hiddenIndexCodes = appendIds(state.hiddenIndexCodes, [code]);
  const nextSelected = state.selectedIndexes.filter((item) => item.code !== code);
  const removedActive = state.activeIndexCode === code;
  await syncSelectedIndexes(nextSelected, removedActive);
}

async function moveIndex(code, direction) {
  const currentIndex = state.selectedIndexes.findIndex((item) => item.code === code);
  if (currentIndex < 0) return;
  const targetIndex = direction === "up" ? currentIndex - 1 : currentIndex + 1;
  if (targetIndex < 0 || targetIndex >= state.selectedIndexes.length) return;
  const nextSelected = [...state.selectedIndexes];
  const [moved] = nextSelected.splice(currentIndex, 1);
  nextSelected.splice(targetIndex, 0, moved);
  await syncSelectedIndexes(nextSelected, false);
}

function hideStockResultCard(code) {
  const targetCode = String(code || "").trim();
  if (!targetCode) return;
  state.hiddenStockResultCodes = appendIds(state.hiddenStockResultCodes, [targetCode]);
  persistStoredIdArray(STOCK_RESULT_HIDDEN_STORAGE_KEY, state.hiddenStockResultCodes);
  renderStockResults(
    state.stockPayload?.data?.results || [],
    state.stockPayload?.data?.failed_details || {}
  );
  showToast(`已从当前看板隐藏 ${targetCode}`);
}

async function applyIndexBoardOrder(ids) {
  const nextSelected = reorderItemsByIds(state.selectedIndexes, ids, getIndexCardId);
  await syncSelectedIndexes(nextSelected, false);
}

function hideStockSearchDropdown() {
  state.stockSearchResults = [];
  dom.stockSearchDropdown.classList.add("hidden");
  dom.stockSearchDropdown.innerHTML = "";
}

function renderStockSearchDropdown(items, message = "") {
  state.stockSearchResults = items || [];
  if (message) {
    dom.stockSearchDropdown.innerHTML = `<div class="stock-search-empty">${esc(message)}</div>`;
    dom.stockSearchDropdown.classList.remove("hidden");
    return;
  }

  if (!state.stockSearchResults.length) {
    hideStockSearchDropdown();
    return;
  }

  dom.stockSearchDropdown.innerHTML = state.stockSearchResults.map((item, index) => `
    <button class="stock-search-option" type="button" data-stock-result-index="${index}">
      <span>
        <strong>${esc(item.name)}</strong>
        <small>${esc(item.code)}</small>
      </span>
      <em>点击分析</em>
    </button>
  `).join("");
  dom.stockSearchDropdown.classList.remove("hidden");
}

async function searchStockOptions() {
  const keyword = dom.stockSearchInput.value.trim();
  if (!keyword) {
    hideStockSearchDropdown();
    return;
  }

  const requestId = state.stockSearchRequestId + 1;
  state.stockSearchRequestId = requestId;
  renderStockSearchDropdown([], "搜索中...");

  try {
    const payload = await fetchJson(api.searchStocks(keyword));
    if (requestId !== state.stockSearchRequestId) return;
    const items = payload.data?.items || [];
    renderStockSearchDropdown(items, items.length ? "" : "没有找到匹配股票");
  } catch (error) {
    if (requestId !== state.stockSearchRequestId) return;
    renderStockSearchDropdown([], `搜索失败：${error.message}`);
  }
}

function scheduleStockSearch() {
  window.clearTimeout(state.stockSearchTimer);
  const keyword = dom.stockSearchInput.value.trim();
  state.selectedSearchStock = null;
  if (!keyword) {
    hideStockSearchDropdown();
    return;
  }
  state.stockSearchTimer = window.setTimeout(searchStockOptions, 240);
}

async function loadStocks() {
  try {
    const payload = await fetchJson(api.stocks);
    const stocks = payload.data?.stocks || [];
    state.watchStocks = stocks;
    syncWatchSelection(stocks);
    renderWatchList(dom.stockWatchList, stocks);
    updateWatchManageText();
  } catch (error) {
    setChip(dom.stockManageText, `加载失败：${error.message}`, "bad");
    dom.stockWatchList.innerHTML = `<div class="empty">股票自选池加载失败。</div>`;
  }
}

async function addStock(stock = null) {
  const target = stock || state.selectedSearchStock || state.stockSearchResults[0];
  if (!target?.code || !target?.name) {
    alert("请先搜索并选择一只股票");
    return;
  }
  dom.addStockBtn.disabled = true;
  try {
    const saved = await saveWatchStock(buildWatchPayload(target, "stock_search"));
    if (!saved) return;
    dom.stockSearchInput.value = "";
    hideStockSearchDropdown();
  } catch (error) {
    alert(`添加股票失败：${error.message}`);
  } finally {
    dom.addStockBtn.disabled = false;
  }
}

async function openSearchedStock(stock = null) {
  const target = stock || state.selectedSearchStock || state.stockSearchResults[0];
  if (!target?.code || !target?.name) {
    alert("请先搜索并选择一只股票");
    return;
  }

  state.selectedSearchStock = target;
  dom.stockSearchInput.value = `${target.code} ${target.name}`;
  hideStockSearchDropdown();
  switchView("stockView");
  dom.runStockBtn.disabled = true;
  setChip(dom.stockRunStatus, `正在分析：${target.name}`, "blue");
  setChip(dom.stockElapsed, "耗时：--");
  setChip(dom.stockNotify, "飞书：未执行", "bad");

  try {
    const payload = normalizeRunOncePayload(
      await fetchJson(api.analyzeStock(target.code, target.name), {
        method: "POST",
        headers: { "Content-Type": "application/json" }
      })
    );
    state.stockPayload = payload;
    clearHiddenStockResultsForIncomingItems(payload.data.results);
    renderStockResults(payload.data.results, payload.data.failed_details);
    setChip(dom.stockRunStatus, `已打开：${target.name}`, "good");
    setChip(dom.stockElapsed, `耗时：${num(payload.data?.elapsed_seconds ?? 0, 3)}秒`);
  } catch (error) {
    setChip(dom.stockRunStatus, `打开失败：${error.message}`, "bad");
    dom.stockResults.innerHTML = `<div class="empty">个股打开失败：${esc(error.message)}</div>`;
  } finally {
    dom.runStockBtn.disabled = false;
  }
}

async function deleteStock(code) {
  try {
    const entry = state.watchStocks.find((item) => item.code === code) || { code, name: code };
    const removed = await saveWatchStock(buildWatchPayload(entry, entry.source || "manual"), { remove: true });
    if (!removed) {
      alert("删除股票失败，请稍后重试");
    }
  } catch (error) {
    alert(`删除股票失败：${error.message}`);
  }
}

async function runStockAnalysis() {
  dom.runStockBtn.disabled = true;
  setChip(dom.stockRunStatus, "分析中...");
  setChip(dom.stockElapsed, "耗时：--");
  setChip(dom.stockNotify, "飞书：--");
  try {
    const payload = normalizeRunOncePayload(await fetchJson(api.runStocks, { method: "POST", headers: { "Content-Type": "application/json" } }));
    state.stockPayload = payload;
    clearHiddenStockResultsForIncomingItems(payload.data.results);
    renderStockResults(payload.data.results, payload.data.failed_details);
    renderHome();
    const failedCount = Object.keys(payload.data?.failed_details || {}).length;
    setChip(dom.stockRunStatus, failedCount ? `部分成功：${payload.data.results.length} 成功 / ${failedCount} 失败` : "分析完成", failedCount ? "blue" : "good");
    setChip(dom.stockElapsed, `耗时：${num(payload.data?.elapsed_seconds ?? 0, 3)}秒`);
    const notification = payload.data?.notification || {};
    setChip(dom.stockNotify, notification.sent ? "飞书：已推送" : `飞书：${notification.reason || "未推送"}`, notification.sent ? "good" : "bad");
  } catch (error) {
    setChip(dom.stockRunStatus, `失败：${error.message}`, "bad");
    setChip(dom.stockNotify, "飞书：未执行", "bad");
    dom.stockResults.innerHTML = `<div class="empty">个股分析失败：${esc(error.message)}</div>`;
    dom.heatmap.innerHTML = `<div class="empty">本次未生成热力图：${esc(error.message)}</div>`;
  } finally {
    dom.runStockBtn.disabled = false;
  }
}

async function refreshHomeData() {
  await Promise.allSettled([runStockAnalysis(), refreshIndexBoardData(false)]);
}

state.selectedWatchCodes = readStoredWatchSelection();
state.stockResultOrder = readStoredIdArray(STOCK_RESULT_ORDER_STORAGE_KEY);
state.hiddenStockResultCodes = readStoredIdArray(STOCK_RESULT_HIDDEN_STORAGE_KEY);
state.indexBoardOrder = readStoredIdArray(INDEX_BOARD_ORDER_STORAGE_KEY);
state.hiddenIndexCodes = readStoredIdArray(INDEX_BOARD_HIDDEN_STORAGE_KEY);

createSortableGrid(dom.stockResults, {
  itemSelector: ".stock-result-card",
  handleSelector: "[data-stock-drag-handle]",
  dataAttribute: "stockResultId",
  onCommit(ids) {
    replaceStoredIdArray(STOCK_RESULT_ORDER_STORAGE_KEY, ids, (value) => {
      state.stockResultOrder = value;
    });
    renderStockResults(
      state.stockPayload?.data?.results || [],
      state.stockPayload?.data?.failed_details || {}
    );
  }
});

createSortableGrid(dom.indexBoardGrid, {
  itemSelector: ".index-board-item",
  handleSelector: "[data-index-drag-handle]",
  dataAttribute: "indexCardId",
  onCommit(ids) {
    applyIndexBoardOrder(ids);
  }
});

dom.navButtons.forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
dom.refreshHomeBtn.addEventListener("click", refreshHomeData);
dom.addStockBtn.addEventListener("click", () => addStock());
dom.stockSearchInput.addEventListener("input", scheduleStockSearch);
dom.stockSearchInput.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") return;
  event.preventDefault();
  if (state.stockSearchResults.length) {
    openSearchedStock(state.stockSearchResults[0]);
  } else {
    searchStockOptions();
  }
});
dom.stockSearchDropdown.addEventListener("click", (event) => {
  const option = event.target.closest("[data-stock-result-index]");
  if (!option) return;
  const target = state.stockSearchResults[Number(option.dataset.stockResultIndex)];
  openSearchedStock(target);
});
document.addEventListener("click", (event) => {
  if (event.target.closest(".stock-search")) return;
  hideStockSearchDropdown();
});
dom.stockWatchList.addEventListener("click", (event) => {
  const analyzeButton = event.target.closest("[data-watch-analyze]");
  if (analyzeButton) {
    event.stopPropagation();
    const code = analyzeButton.dataset.watchAnalyze || "";
    const entry = state.watchStocks.find((item) => item.code === code);
    if (entry) openSearchedStock(entry);
    return;
  }
  const deleteButton = event.target.closest("[data-watch-delete]");
  if (deleteButton) {
    event.stopPropagation();
    deleteStock(deleteButton.dataset.watchDelete || "");
    return;
  }
  const item = event.target.closest("[data-watch-code]");
  if (!item) return;
  toggleWatchSelection(item.dataset.watchCode || "");
});
dom.stockWatchList.addEventListener("keydown", (event) => {
  const item = event.target.closest("[data-watch-code]");
  if (!item) return;
  if (event.key !== "Enter" && event.key !== " ") return;
  event.preventDefault();
  toggleWatchSelection(item.dataset.watchCode || "");
});
dom.runStockBtn.addEventListener("click", runStockAnalysis);
dom.refreshIndexBtn.addEventListener("click", () => refreshIndexBoardData(false));
dom.refreshStyleFlowBtn?.addEventListener("click", () => loadStyleFlowWidgets(true));
dom.opportunityRefreshBtn?.addEventListener("click", () => applyOpportunityMode("stock", state.opportunityBoard, state.opportunityScope, true));

dom.stockResults.addEventListener("click", (event) => {
  const deleteButton = event.target.closest("[data-stock-hide]");
  if (!deleteButton) return;
  event.stopPropagation();
  hideStockResultCard(deleteButton.dataset.stockHide || "");
});

dom.indexBoardGrid.addEventListener("click", (event) => {
  const deleteButton = event.target.closest("[data-index-hide]");
  if (deleteButton) {
    event.stopPropagation();
    removeIndexFromBoard(deleteButton.dataset.indexHide || "");
    return;
  }
  if (event.target.closest("[data-index-drag-handle]")) return;
  const card = event.target.closest(".index-mini-card");
  if (!card) return;
  loadIndexDetail(card.dataset.indexCode || "", false);
});

dom.availableIndexPool.addEventListener("click", async (event) => {
  const button = event.target.closest(".available-pill");
  if (!button || button.disabled) return;
  await addIndexToBoard(button.dataset.indexCode || "");
});

dom.styleEtfGroups?.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-style-etf-code]");
  if (!button) return;
  await openStyleEtfLink(
    button.dataset.styleEtfCode || "",
    button.dataset.styleEtfName || "",
    button.dataset.styleIndexCode || "",
  );
});

dom.opportunityTabs?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-opportunity-tab]");
  if (!button) return;
  applyOpportunityMode(button.dataset.opportunityTab || "index");
});

dom.opportunityScopeTabs?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-opportunity-scope]");
  if (!button) return;
  applyOpportunityMode("stock", state.opportunityBoard, button.dataset.opportunityScope || "market");
});

dom.opportunityBoardTabs?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-opportunity-board]");
  if (!button) return;
  applyOpportunityMode("stock", button.dataset.opportunityBoard || "all", state.opportunityScope);
});

dom.opportunityPoolList.addEventListener("click", (event) => {
  const watchToggle = event.target.closest("[data-watch-toggle]");
  if (watchToggle) {
    event.stopPropagation();
    const payload = buildWatchPayload({
      code: watchToggle.dataset.watchToggle || "",
      name: watchToggle.dataset.watchName || "",
      board: watchToggle.dataset.watchBoard || "",
    }, watchToggle.dataset.watchSource || "opportunity_pool");
    saveWatchStock(payload, { remove: isWatchlisted(payload.code) });
    return;
  }
  const card = event.target.closest(".opportunity-pool-item");
  if (!card) return;
  loadOpportunityDetail(card.dataset.opportunityCode || "", false);
});
dom.opportunityRecommendCard?.addEventListener("click", (event) => {
  const watchToggle = event.target.closest("[data-watch-toggle]");
  if (watchToggle) {
    event.stopPropagation();
    const payload = buildWatchPayload({
      code: watchToggle.dataset.watchToggle || "",
      name: watchToggle.dataset.watchName || "",
      board: watchToggle.dataset.watchBoard || "",
    }, watchToggle.dataset.watchSource || "opportunity_pool");
    saveWatchStock(payload, { remove: isWatchlisted(payload.code) });
    return;
  }
  const current = state.recommendedOpportunity;
  if (!current) return;
  loadOpportunityDetail(opportunitySymbol(current), false);
});
dom.opportunityDetailPanel?.addEventListener("click", (event) => {
  const watchToggle = event.target.closest("[data-watch-toggle]");
  if (watchToggle) {
    const payload = buildWatchPayload({
      code: watchToggle.dataset.watchToggle || "",
      name: watchToggle.dataset.watchName || "",
      board: watchToggle.dataset.watchBoard || "",
    }, watchToggle.dataset.watchSource || "opportunity_pool");
    saveWatchStock(payload, { remove: isWatchlisted(payload.code) });
    return;
  }
  const openStock = event.target.closest("[data-opportunity-open-stock]");
  if (openStock) {
    openSearchedStock({
      code: openStock.dataset.opportunityOpenStock || "",
      name: openStock.dataset.opportunityStockName || "",
    });
  }
});
restoreActiveView();
Promise.allSettled([loadStocks(), refreshIndexBoardData(false), loadStyleFlowWidgets(false), loadOpportunityWidgets()]).then(renderHome);
