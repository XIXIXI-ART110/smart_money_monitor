const state = {
  stockPayload: null,
  selectedIndexes: [],
  availableIndexes: [],
  activeIndexCode: "",
  activeIndexDetail: null,
  isIndexModalOpen: false,
  opportunityMode: "stock",
  opportunityScope: "market",
  opportunityBoard: "all",
  opportunityRequestId: 0,
  indexOpportunityItems: [],
  stockOpportunityItems: [],
  lowOpportunityItems: [],
  recommendedOpportunity: null,
  activeOpportunityCode: "",
  activeOpportunityDetail: null,
  stockSearchResults: [],
  stockSearchTimer: null,
  stockSearchRequestId: 0
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
  openIndexSettingsBtn: document.getElementById("openIndexSettingsBtn"),
  openIndexSettingsInlineBtn: document.getElementById("openIndexSettingsInlineBtn"),
  closeIndexSettingsBtn: document.getElementById("closeIndexSettingsBtn"),
  indexSettingsModal: document.getElementById("indexSettingsModal"),
  indexSettingsBackdrop: document.getElementById("indexSettingsBackdrop"),
  indexBoardStatus: document.getElementById("indexBoardStatus"),
  indexQuickChips: document.getElementById("indexQuickChips"),
  indexBoardGrid: document.getElementById("indexBoardGrid"),
  indexSelectionStatus: document.getElementById("indexSelectionStatus"),
  indexDetailPanel: document.getElementById("indexDetailPanel"),
  opportunityStatus: document.getElementById("opportunityStatus"),
  opportunityTabs: document.getElementById("opportunityTabs"),
  opportunityScopeTabs: document.getElementById("opportunityScopeTabs"),
  opportunityBoardTabs: document.getElementById("opportunityBoardTabs"),
  opportunityRecommendCard: document.getElementById("opportunityRecommendCard"),
  opportunityPoolList: document.getElementById("opportunityPoolList"),
  opportunityDetailPanel: document.getElementById("opportunityDetailPanel"),
  selectedIndexCount: document.getElementById("selectedIndexCount"),
  availableIndexCount: document.getElementById("availableIndexCount"),
  selectedIndexList: document.getElementById("selectedIndexList"),
  availableIndexPool: document.getElementById("availableIndexPool"),
  resetIndexBoardBtn: document.getElementById("resetIndexBoardBtn"),
  refreshIndexInlineBtn: document.getElementById("refreshIndexInlineBtn"),
  heatmap: document.getElementById("heatmap")
};

const API_BASE = (() => {
  const configuredBase =
    window.SMART_MONEY_API_BASE ||
    document.querySelector('meta[name="smart-money-api-base"]')?.getAttribute("content") ||
    "";
  const normalizedBase = String(configuredBase).trim().replace(/\/+$/, "");
  if (normalizedBase) return normalizedBase;

  const localApiBase = "http://127.0.0.1:8000";
  const { protocol, hostname, port } = window.location;
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";
  if ((protocol === "http:" || protocol === "https:") && isLocalHost) {
    return port === "8000" ? "" : localApiBase;
  }
  if (protocol === "file:") return localApiBase;
  return "";
})();

function apiPath(path) {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE}${path}`;
}

const api = {
  stocks: apiPath("/api/stocks"),
  runStocks: apiPath("/api/run-once"),
  indexes: apiPath("/api/indexes"),
  indexOptions: apiPath("/api/indexes/options"),
  indexDetail: (code) => apiPath(`/api/indexes/detail?code=${encodeURIComponent(code)}`),
  opportunities: (scope = "market", board = "all") => apiPath(`/api/opportunities?scope=${encodeURIComponent(scope)}&board=${encodeURIComponent(board)}`),
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
const DEFAULT_VIEW_ID = "etfView";
const NAV_VIEW_KEY_TO_ID = {
  home: "homeView",
  stock: "stockView",
  etf: "etfView",
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

function setChip(el, text, toneName = "") {
  el.textContent = text;
  el.className = `chip ${toneName}`.trim();
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

function renderWatchList(target, items) {
  if (!items.length) {
    target.innerHTML = `<div class="empty">当前暂无股票自选项。</div>`;
    return;
  }
  target.innerHTML = items.map((item) => `
    <div class="watch-item">
      <div class="watch-main">
        <strong>${esc(item.name)}</strong>
        <span>${esc(item.code)}</span>
      </div>
      <button class="danger-button" type="button" onclick="deleteStock('${esc(item.code)}')">删除</button>
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

function renderStockResults(results) {
  if (!results.length) {
    dom.stockResults.innerHTML = `<div class="empty">本次没有返回个股结果。</div>`;
    dom.heatmap.innerHTML = `<div class="empty">本次没有热力图数据。</div>`;
    return;
  }

  dom.stockResults.innerHTML = results.map((item) => {
    const analysis = item.analysis || {};
    const market = item.market_data || {};
    const fund = item.fund_flow || {};
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
      <article class="card stock-card ${isError ? "error" : ""}">
        <div class="stock-card-head">
          <div class="stock-title">
            <h3>${esc(item.name || "未知股票")}</h3>
            <span>${esc(item.code || "N/A")}</span>
          </div>
          <div class="stock-score">
            <span>评分</span>
            <strong>${esc(item.score ?? 0)}</strong>
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
  }).join("");

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

function opportunityReasonTags(item, limit = 3) {
  const score = opportunityScoreObject(item);
  const rawTags = [
    ...(Array.isArray(score.tags) ? score.tags : []),
    ...(Array.isArray(item?.tags) ? item.tags : []),
    ...(Array.isArray(item?.signals) ? item.signals : []),
  ];
  const tags = [];
  rawTags.forEach((tag) => {
    const label = firstSentence(tag, "");
    if (label && !tags.includes(label)) tags.push(label);
  });

  if (!tags.length) {
    const features = item?.features || {};
    if (features.trend_turn) tags.push("趋势转强");
    if (features.volume_spike) tags.push("量能放大");
    if (features.stop_falling) tags.push("止跌确认");
    if (features.bullish_break) tags.push("阳线突破");
  }

  return tags.slice(0, limit);
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

function syncOpportunityModeSections() {
  const isStockMode = state.opportunityMode === "stock";
  setOpportunitySectionVisible(dom.opportunityScopeTabs, isStockMode);
  setOpportunitySectionVisible(dom.opportunityBoardTabs, isStockMode);
  setOpportunitySectionVisible(dom.opportunityDetailPanel, isStockMode);
}

function fetchJson(url, options) {
  return fetch(url, options).then(async (response) => {
    const payload = await response.json();
    if (!response.ok || payload.ok === false) throw new Error(payload.message || "请求失败");
    return payload;
  });
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
  const reasonTags = opportunityReasonTags(item);
  dom.opportunityRecommendCard.innerHTML = `
    <div class="recommend-main">
      <div class="chips">
        <span class="chip ${signalTone(signal)}">${esc(signal)}</span>
        <span class="chip">今日先看</span>
        ${scopeLabel ? `<span class="chip">${esc(scopeLabel)}</span>` : ""}
        ${boardLabel ? `<span class="chip blue">${esc(boardLabel)}</span>` : ""}
        ${item.badge ? `<span class="chip">${esc(item.badge)}</span>` : ""}
      </div>
      <div>
        <h3>${esc(item.name)}</h3>
        <div class="sub">${esc(symbol)}</div>
      </div>
      <div class="recommend-score">
        <div>
          <small class="sub">综合评分</small>
          <strong>${esc(scoreValue)}</strong>
        </div>
      </div>
      <div class="recommend-text">${esc(opportunityBrief(item))}</div>
      ${reasonTags.length ? `
        <div class="chips">
          ${reasonTags.map((tag) => `<span class="chip blue">${esc(tag)}</span>`).join("")}
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
    const signal = opportunitySignal(item);
    const boardLabel = opportunityBoardLabel(item);
    return `
    <article class="opportunity-pool-item ${state.activeOpportunityCode === symbol ? "active" : ""}" data-opportunity-code="${esc(symbol)}">
      <div class="opportunity-head">
        <div>
          <h3>${esc(item.name)}</h3>
          <div class="opportunity-code">${esc(symbol)}</div>
        </div>
        <div class="opportunity-score">${esc(scoreValue)}</div>
      </div>
      <div class="chips">
        <span class="chip ${signalTone(signal)}">${esc(signal)}</span>
        <span class="chip">第 ${esc(index + 1)}</span>
        ${boardLabel ? `<span class="chip blue">${esc(boardLabel)}</span>` : ""}
        ${item.badge ? `<span class="chip">${esc(item.badge)}</span>` : ""}
      </div>
      <div class="opportunity-reason">${esc(opportunityBrief(item))}</div>
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
    dom.indexBoardGrid.innerHTML = `<div class="empty">当前没有已添加的指数卡片，请先在设置弹层里添加。</div>`;
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
      <article class="index-mini-card ${state.activeIndexCode === item.code ? "active" : ""}" data-index-code="${esc(item.code)}">
        <div class="index-card-top">
          <div>
            <h3 class="index-card-name">${esc(item.name)}</h3>
            <div class="index-card-category">${esc(item.category || "宽基")}</div>
          </div>
          <span class="chip blue">${esc(item.category || "宽基")}</span>
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

function renderIndexSettingsModal() {
  const selectedCodes = new Set(state.selectedIndexes.map((item) => item.code));
  dom.selectedIndexCount.textContent = `${state.selectedIndexes.length} 项`;
  dom.availableIndexCount.textContent = `${state.availableIndexes.length} 项`;

  if (!state.selectedIndexes.length) {
    dom.selectedIndexList.innerHTML = `<div class="empty">当前还没有已添加指数。</div>`;
  } else {
    dom.selectedIndexList.innerHTML = state.selectedIndexes.map((item, index) => `
      <div class="selected-item-card" data-index-code="${esc(item.code)}">
        <div class="selected-item-top">
          <div>
            <div class="selected-item-name">${esc(item.name)}</div>
            <div class="selected-item-meta">${esc(item.code)} · ${esc(item.category || "宽基")} · ${pct(item.change_pct)}</div>
          </div>
          <span class="chip ${Number(item.change_pct) > 0 ? "good" : Number(item.change_pct) < 0 ? "bad" : "blue"}">${num(item.value)}</span>
        </div>
        <div class="selected-item-actions">
          <button class="action-button" type="button" data-action="move-up" ${index === 0 ? "disabled" : ""}>上移</button>
          <button class="action-button" type="button" data-action="move-down" ${index === state.selectedIndexes.length - 1 ? "disabled" : ""}>下移</button>
          <button class="action-button" type="button" data-action="activate">查看详情</button>
          <button class="action-button" type="button" data-action="remove">删除</button>
        </div>
      </div>
    `).join("");
  }

  const options = state.availableIndexes.map((item) => {
    const alreadySelected = selectedCodes.has(item.code);
    return `
      <button class="available-pill ${alreadySelected ? "disabled" : ""}" type="button" data-index-code="${esc(item.code)}" ${alreadySelected ? "disabled" : ""}>
        ${esc(item.name)}
      </button>
    `;
  }).join("");
  dom.availableIndexPool.innerHTML = options || `<div class="empty">暂无可选指数。</div>`;
}

function openIndexSettingsModal() {
  state.isIndexModalOpen = true;
  dom.indexSettingsModal.classList.remove("hidden");
  dom.indexSettingsModal.setAttribute("aria-hidden", "false");
  renderIndexSettingsModal();
}

function closeIndexSettingsModal() {
  state.isIndexModalOpen = false;
  dom.indexSettingsModal.classList.add("hidden");
  dom.indexSettingsModal.setAttribute("aria-hidden", "true");
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
  ensureActiveIndexAfterSelectionChange();
  renderIndexBoard();
  renderIndexSettingsModal();
  renderHome();

  if (!state.selectedIndexes.length) {
    detailError("当前没有已添加的指数，请先在设置弹层中选择指数。");
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
  if (forceDefaults || !state.selectedIndexes.length) {
    state.selectedIndexes = defaultIndexes;
  } else {
    state.selectedIndexes = mergeSelectedWithLatest(state.selectedIndexes, allOptions);
  }

  ensureActiveIndexAfterSelectionChange();
  renderIndexBoard();
  renderIndexSettingsModal();
  renderHome();

  if (state.selectedIndexes.length) {
    setChip(dom.indexBoardStatus, `已加载 ${state.selectedIndexes.length} 张指数卡片`, "good");
    await loadIndexDetail(state.activeIndexCode || state.selectedIndexes[0].code, true);
  } else {
    setChip(dom.indexBoardStatus, "当前没有可展示的指数卡片", "bad");
    detailError("指数卡片为空，请打开设置弹层添加指数。");
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
}

async function applyOpportunityMode(mode, board = state.opportunityBoard, scope = state.opportunityScope) {
  const requestId = ++state.opportunityRequestId;
  if (mode === "stock") {
    state.opportunityScope = scope;
    state.opportunityBoard = board;
    state.opportunityMode = "stock";
    renderOpportunityTabs();
    syncOpportunityModeSections();
    const scopeLabel = OPPORTUNITY_SCOPE_LABELS[scope] || "全市场";
    const boardLabel = OPPORTUNITY_BOARD_LABELS[board] || "全部";
    setChip(dom.opportunityStatus, `正在加载${scopeLabel} · ${boardLabel}个股机会...`, "blue");
    dom.opportunityRecommendCard.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在准备个股推荐...</div></div>`;
    dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载中...</div>`;
    dom.opportunityDetailPanel.innerHTML = `<div class="empty">点击某张个股机会卡片后，这里显示详细信息。</div>`;

    try {
      const payload = await fetchJson(api.opportunities(scope, board));
      if (requestId !== state.opportunityRequestId || state.opportunityMode !== "stock") return;
      state.opportunityScope = payload.data?.scope || scope;
      state.opportunityBoard = payload.data?.board || board;
      state.stockOpportunityItems = payload.data?.items || [];
      syncOpportunityView(state.stockOpportunityItems, "stock");
      const nextCode = opportunitySymbol(state.recommendedOpportunity) || opportunitySymbol(state.lowOpportunityItems[0]) || "";
      if (nextCode) {
        await loadOpportunityDetail(nextCode, true);
      }
      const loadedScopeLabel = payload.data?.scope_name || OPPORTUNITY_SCOPE_LABELS[state.opportunityScope] || scopeLabel;
      const loadedBoardLabel = OPPORTUNITY_BOARD_LABELS[state.opportunityBoard] || boardLabel;
      setChip(dom.opportunityStatus, `已加载 ${state.lowOpportunityItems.length} 个${loadedScopeLabel} · ${loadedBoardLabel}个股机会`, "good");
      return;
    } catch (error) {
      if (requestId !== state.opportunityRequestId || state.opportunityMode !== "stock") return;
      setChip(dom.opportunityStatus, `个股机会加载失败：${error.message}`, "bad");
      dom.opportunityRecommendCard.innerHTML = `<div class="empty">个股推荐加载失败：${esc(error.message)}</div>`;
      dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载失败：${esc(error.message)}</div>`;
      dom.opportunityDetailPanel.innerHTML = `<div class="empty">个股机会详情暂不可用。</div>`;
      return;
    }
  }

  state.opportunityMode = "index";
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
  await syncSelectedIndexes([...state.selectedIndexes, target], false);
  if (!state.activeIndexCode) {
    await loadIndexDetail(target.code, true);
  }
}

async function removeIndexFromBoard(code) {
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
      <em>点击加入</em>
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
    renderWatchList(dom.stockWatchList, stocks);
    setChip(dom.stockManageText, `当前自选股：${stocks.length}只`);
  } catch (error) {
    setChip(dom.stockManageText, `加载失败：${error.message}`, "bad");
    dom.stockWatchList.innerHTML = `<div class="empty">股票自选池加载失败。</div>`;
  }
}

async function addStock(stock = null) {
  const target = stock || state.stockSearchResults[0];
  if (!target?.code || !target?.name) {
    alert("请先搜索并选择一只股票");
    return;
  }
  dom.addStockBtn.disabled = true;
  try {
    await fetchJson(api.stocks, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: target.code, name: target.name })
    });
    dom.stockSearchInput.value = "";
    hideStockSearchDropdown();
    await loadStocks();
  } catch (error) {
    alert(`添加股票失败：${error.message}`);
  } finally {
    dom.addStockBtn.disabled = false;
  }
}

async function deleteStock(code) {
  try {
    await fetchJson(`${api.stocks}/${encodeURIComponent(code)}`, { method: "DELETE" });
    await loadStocks();
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
    renderStockResults(payload.data.results);
    renderHome();
    setChip(dom.stockRunStatus, "分析完成", "good");
    setChip(dom.stockElapsed, `耗时：${num(payload.data?.elapsed_seconds ?? 0, 3)}秒`);
    const notification = payload.data?.notification || {};
    setChip(dom.stockNotify, notification.sent ? "飞书：已推送" : `飞书：${notification.reason || "未推送"}`, notification.sent ? "good" : "bad");
  } catch (error) {
    setChip(dom.stockRunStatus, `失败：${error.message}`, "bad");
    dom.stockResults.innerHTML = `<div class="empty">个股分析失败：${esc(error.message)}</div>`;
  } finally {
    dom.runStockBtn.disabled = false;
  }
}

async function refreshHomeData() {
  await Promise.allSettled([runStockAnalysis(), refreshIndexBoardData(false)]);
}

dom.navButtons.forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
dom.refreshHomeBtn.addEventListener("click", refreshHomeData);
dom.addStockBtn.addEventListener("click", () => addStock());
dom.stockSearchInput.addEventListener("input", scheduleStockSearch);
dom.stockSearchInput.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") return;
  event.preventDefault();
  if (state.stockSearchResults.length) {
    addStock(state.stockSearchResults[0]);
  } else {
    searchStockOptions();
  }
});
dom.stockSearchDropdown.addEventListener("click", (event) => {
  const option = event.target.closest("[data-stock-result-index]");
  if (!option) return;
  const target = state.stockSearchResults[Number(option.dataset.stockResultIndex)];
  addStock(target);
});
document.addEventListener("click", (event) => {
  if (event.target.closest(".stock-search")) return;
  hideStockSearchDropdown();
});
dom.runStockBtn.addEventListener("click", runStockAnalysis);
dom.refreshIndexBtn.addEventListener("click", () => refreshIndexBoardData(false));
dom.openIndexSettingsBtn.addEventListener("click", openIndexSettingsModal);
dom.openIndexSettingsInlineBtn.addEventListener("click", openIndexSettingsModal);
dom.closeIndexSettingsBtn.addEventListener("click", closeIndexSettingsModal);
dom.indexSettingsBackdrop.addEventListener("click", closeIndexSettingsModal);
dom.resetIndexBoardBtn.addEventListener("click", () => refreshIndexBoardData(true));
dom.refreshIndexInlineBtn.addEventListener("click", () => refreshIndexBoardData(false));

dom.indexBoardGrid.addEventListener("click", (event) => {
  const card = event.target.closest(".index-mini-card");
  if (!card) return;
  loadIndexDetail(card.dataset.indexCode || "", false);
});

dom.selectedIndexList.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  const card = event.target.closest("[data-index-code]");
  if (!button || !card) return;
  const code = card.dataset.indexCode || "";
  const action = button.dataset.action;
  if (action === "move-up") await moveIndex(code, "up");
  if (action === "move-down") await moveIndex(code, "down");
  if (action === "remove") await removeIndexFromBoard(code);
  if (action === "activate") {
    state.activeIndexCode = code;
    closeIndexSettingsModal();
    await loadIndexDetail(code, true);
  }
});

dom.availableIndexPool.addEventListener("click", async (event) => {
  const button = event.target.closest(".available-pill");
  if (!button || button.disabled) return;
  await addIndexToBoard(button.dataset.indexCode || "");
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
  const card = event.target.closest(".opportunity-pool-item");
  if (!card) return;
  loadOpportunityDetail(card.dataset.opportunityCode || "", false);
});

window.deleteStock = deleteStock;

restoreActiveView();
Promise.allSettled([loadStocks(), refreshIndexBoardData(true), loadOpportunityWidgets()]).then(renderHome);
