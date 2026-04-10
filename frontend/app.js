const state = {
  stockPayload: null,
  selectedIndexes: [],
  availableIndexes: [],
  activeIndexCode: "",
  activeIndexDetail: null,
  isIndexModalOpen: false,
  opportunityMode: "stock",
  indexOpportunityItems: [],
  stockOpportunityItems: [],
  lowOpportunityItems: [],
  recommendedOpportunity: null,
  activeOpportunityCode: "",
  activeOpportunityDetail: null
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
  stockCodeInput: document.getElementById("stockCodeInput"),
  stockNameInput: document.getElementById("stockNameInput"),
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

const api = {
  stocks: "/api/stocks",
  runStocks: "/api/run-once",
  indexes: "/api/indexes",
  indexOptions: "/api/indexes/options",
  indexDetail: (code) => `/api/indexes/detail?code=${encodeURIComponent(code)}`,
  lowOpportunity: "/api/opportunity/low",
  stockLowOpportunity: "/api/opportunity/stock_low",
  opportunityRecommend: "/api/opportunity/recommend",
  opportunityDetail: (code) => `/api/opportunity/detail?code=${encodeURIComponent(code)}`
};

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

function switchView(id) {
  document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === id));
  dom.navButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.view === id));
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
    return `
      <article class="card ${isError ? "error" : ""}">
        <div class="card-head">
          <div>
            <h3>${esc(item.name || "未知股票")}</h3>
            <div class="sub">${esc(item.code || "N/A")} · 评分 ${esc(item.score ?? 0)}</div>
          </div>
          <span class="badge ${isError ? "error" : ""}">${esc(item.status || "unknown")}</span>
        </div>
        <div class="kv">
          <div class="cell"><small>最新价</small><strong>${num(market.latest_price)}</strong></div>
          <div class="cell"><small>涨跌幅</small><strong>${pct(market.pct_change)}</strong></div>
          <div class="cell"><small>成交额</small><strong>${amt(market.turnover)}</strong></div>
          <div class="cell"><small>主力净流入</small><strong>${amt(fund.main_net_inflow)}</strong></div>
        </div>
        <div class="tags">${(analysis.signal || []).map((tag) => `<span class="tag">${esc(tag)}</span>`).join("") || `<span class="tag">暂无信号</span>`}</div>
        <div class="tags">${(analysis.risk || []).map((tag) => `<span class="chip bad">${esc(tag)}</span>`).join("") || `<span class="tag">暂无风险</span>`}</div>
        <div class="summary">${esc(item.ai_summary || item.error || "暂无摘要")}</div>
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

function renderOpportunityTabs() {
  if (!dom.opportunityTabs) return;
  dom.opportunityTabs.querySelectorAll("[data-opportunity-tab]").forEach((button) => {
    button.classList.toggle("active", button.dataset.opportunityTab === state.opportunityMode);
  });
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
    dom.opportunityRecommendCard.innerHTML = `<div class="empty">今日自动推荐暂不可用。</div>`;
    return;
  }

  dom.opportunityRecommendCard.innerHTML = `
    <div class="recommend-main">
      <div class="chips">
        <span class="chip ${signalTone(item.signal)}">今日自动推荐</span>
      </div>
      <div>
        <h3>${esc(item.name)}</h3>
        <div class="sub">${esc(item.code)}</div>
      </div>
      <div class="recommend-score">
        <div>
          <small class="sub">综合评分</small>
          <strong>${esc(item.score ?? 0)}</strong>
        </div>
        <span class="chip ${signalTone(item.signal)}">${esc(item.signal || "观察")}</span>
      </div>
      <div class="recommend-text">${esc(item.reason || "暂无推荐理由")}</div>
      <div class="recommend-text">${esc(item.summary || "暂无总结")}</div>
    </div>
  `;
}

function renderOpportunityPool() {
  const items = state.lowOpportunityItems || [];
  if (!items.length) {
    dom.opportunityPoolList.innerHTML = `<div class="empty">当前暂无低位机会池数据。</div>`;
    return;
  }

  dom.opportunityPoolList.innerHTML = items.map((item) => `
    <article class="opportunity-pool-item ${state.activeOpportunityCode === item.code ? "active" : ""}" data-opportunity-code="${esc(item.code)}">
      <div class="opportunity-head">
        <div>
          <h3>${esc(item.name)}</h3>
          <div class="opportunity-code">${esc(item.code)}</div>
        </div>
        <div class="opportunity-score">${esc(item.score ?? 0)}</div>
      </div>
      <div class="chips">
        <span class="chip ${signalTone(item.signal)}">${esc(item.signal || "观察")}</span>
      </div>
      <div class="opportunity-reason">${esc(item.reason || "暂无理由")}</div>
    </article>
  `).join("");
}

function renderOpportunityDetail() {
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
  dom.opportunityDetailPanel.innerHTML = `
    <div class="chips">
      <span class="chip ${signalTone(item.signal)}">${esc(item.signal || "观察")}</span>
      <span class="chip blue">综合评分 ${esc(item.score ?? 0)}</span>
    </div>
    <div style="margin-top:14px;">
      <h3>${esc(item.name)}</h3>
      <div class="sub">${esc(item.code)}</div>
    </div>
    <div class="recommend-text" style="margin-top:12px;">${esc(item.reason || "暂无理由")}</div>
    <div class="recommend-text" style="margin-top:8px;">${esc(item.summary || "暂无总结")}</div>
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
  if (!code) return;
  if (!force && state.activeOpportunityCode === code && state.activeOpportunityDetail) return;

  state.activeOpportunityCode = code;
  renderOpportunityPool();
  dom.opportunityDetailPanel.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在加载机会详情...</div></div>`;
  const target = state.lowOpportunityItems.find((item) => item.code === code) || null;
  state.activeOpportunityDetail = target;
  renderOpportunityDetail();
}

function syncOpportunityView(items, mode) {
  state.opportunityMode = mode;
  state.lowOpportunityItems = [...items];
  state.recommendedOpportunity = [...items]
    .sort((left, right) => Number(right.score || 0) - Number(left.score || 0))[0] || null;
  renderOpportunityTabs();
  renderRecommendedOpportunity();
  renderOpportunityPool();
}

async function applyOpportunityMode(mode) {
  if (mode === "stock") {
    setChip(dom.opportunityStatus, "正在加载个股机会...", "blue");
    dom.opportunityRecommendCard.innerHTML = `<div class="loading"><div><div class="spinner"></div>正在准备个股推荐...</div></div>`;
    dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载中...</div>`;
    dom.opportunityDetailPanel.innerHTML = `<div class="empty">点击某张个股机会卡片后，这里显示详细信息。</div>`;

    try {
      const payload = await fetchJson(api.stockLowOpportunity);
      state.stockOpportunityItems = payload.data?.items || [];
      syncOpportunityView(state.stockOpportunityItems, "stock");
      const nextCode = state.recommendedOpportunity?.code || state.lowOpportunityItems[0]?.code || "";
      if (nextCode) {
        await loadOpportunityDetail(nextCode, true);
      }
      setChip(dom.opportunityStatus, `已加载 ${state.lowOpportunityItems.length} 个个股机会`, "good");
      return;
    } catch (error) {
      setChip(dom.opportunityStatus, `个股机会加载失败：${error.message}`, "bad");
      dom.opportunityRecommendCard.innerHTML = `<div class="empty">个股推荐加载失败：${esc(error.message)}</div>`;
      dom.opportunityPoolList.innerHTML = `<div class="empty">个股机会列表加载失败：${esc(error.message)}</div>`;
      dom.opportunityDetailPanel.innerHTML = `<div class="empty">个股机会详情暂不可用。</div>`;
      return;
    }
  }

  syncOpportunityView(state.indexOpportunityItems, "index");
  const nextCode = state.recommendedOpportunity?.code || state.lowOpportunityItems[0]?.code || "";
  if (nextCode) {
    await loadOpportunityDetail(nextCode, true);
  }
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

async function addStock() {
  const code = dom.stockCodeInput.value.trim();
  const name = dom.stockNameInput.value.trim();
  if (!code || !name) {
    alert("请输入股票代码和股票名称");
    return;
  }
  dom.addStockBtn.disabled = true;
  try {
    await fetchJson(api.stocks, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code, name })
    });
    dom.stockCodeInput.value = "";
    dom.stockNameInput.value = "";
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
    const payload = await fetchJson(api.runStocks, { method: "POST", headers: { "Content-Type": "application/json" } });
    state.stockPayload = payload;
    renderStockResults(payload.data?.results || []);
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
dom.addStockBtn.addEventListener("click", addStock);
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

dom.opportunityPoolList.addEventListener("click", (event) => {
  const card = event.target.closest(".opportunity-pool-item");
  if (!card) return;
  loadOpportunityDetail(card.dataset.opportunityCode || "", false);
});

window.deleteStock = deleteStock;

Promise.allSettled([loadStocks(), refreshIndexBoardData(true), loadOpportunityWidgets()]).then(renderHome);
