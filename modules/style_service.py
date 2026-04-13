from __future__ import annotations

from datetime import datetime
from time import perf_counter
from typing import Any
import re

try:
    import akshare as ak
except Exception:  # pragma: no cover - import fallback
    ak = None


STYLE_ORDER = [
    "大盘宽基",
    "红利防御",
    "大金融",
    "中小成长",
    "高波题材",
    "大宗商品",
]

ATTACK_STYLES = {"中小成长", "高波题材", "大宗商品"}
DEFENSE_STYLES = {"大盘宽基", "红利防御", "大金融"}


ETF_STYLE_MAP = {
    # 大盘宽基
    "510300": "大盘宽基",
    "510050": "大盘宽基",
    "159919": "大盘宽基",
    "159901": "大盘宽基",
    "563300": "大盘宽基",
    # 红利防御
    "515180": "红利防御",
    "515080": "红利防御",
    "560880": "红利防御",
    "159545": "红利防御",
    "512890": "红利防御",
    # 大金融
    "512800": "大金融",
    "512000": "大金融",
    "159928": "大金融",
    "515650": "大金融",
    "515290": "大金融",
    # 中小成长
    "159915": "中小成长",
    "512500": "中小成长",
    "159845": "中小成长",
    "588000": "中小成长",
    "159949": "中小成长",
    "560010": "中小成长",
    # 高波题材
    "159819": "高波题材",
    "512480": "高波题材",
    "515000": "高波题材",
    "159995": "高波题材",
    "561360": "高波题材",
    "562500": "高波题材",
    "516160": "高波题材",
    # 大宗商品
    "159980": "大宗商品",
    "159985": "大宗商品",
    "518880": "大宗商品",
    "159930": "大宗商品",
    "515220": "大宗商品",
    "516910": "大宗商品",
}


STYLE_EMOJI = {
    "大盘宽基": "🔵",
    "红利防御": "🟡",
    "大金融": "💎",
    "中小成长": "🟢",
    "高波题材": "🔴",
    "大宗商品": "🟤",
}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "" or value == "-":
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except Exception:
        return default


def _style_key(style_name: str) -> str:
    mapping = {
        "大盘宽基": "broad_market",
        "红利防御": "dividend_defense",
        "大金融": "big_finance",
        "中小成长": "mid_small_growth",
        "高波题材": "high_beta_theme",
        "大宗商品": "commodities",
    }
    return mapping.get(style_name, style_name)


def _normalize_code(code: Any) -> str:
    raw = str(code or "").strip()
    if not raw:
        return ""

    normalized = raw.upper().replace("-", "").replace("_", "").replace(" ", "")
    normalized = normalized.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    normalized = normalized.removeprefix("SH").removeprefix("SZ").removeprefix("BJ")

    digit_matches = re.findall(r"\d+", normalized)
    if digit_matches:
        digits = "".join(digit_matches)
        if len(digits) >= 6:
            return digits[-6:]
        return digits.zfill(6)
    return normalized.zfill(6)


def _normalize_col_name(name: Any) -> str:
    return (
        str(name or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .replace("%", "")
        .replace("（", "(")
        .replace("）", ")")
    )


def _find_col(df, candidates: list[str]) -> str | None:
    cols = list(df.columns)
    direct_map = {str(column): column for column in cols}
    normalized_map = {_normalize_col_name(column): column for column in cols}
    for column in candidates:
        if column in direct_map:
            return direct_map[column]
        normalized = _normalize_col_name(column)
        if normalized in normalized_map:
            return normalized_map[normalized]
    return None


def _build_empty_flow_response(message: str = "暂无可用数据") -> dict[str, Any]:
    items = []
    for style_name in STYLE_ORDER:
        items.append(
            {
                "style_key": _style_key(style_name),
                "style_name": style_name,
                "emoji": STYLE_EMOJI.get(style_name, "📊"),
                "net_inflow": 0.0,
                "main_flow": 0.0,
                "etf_count": 0,
                "avg_change_pct": 0.0,
                "direction": "neutral",
                "strength_score": 0.0,
                "related_etfs": [],
            }
        )
    return {
        "ok": True,
        "message": message,
        "data": {
            "items": items,
            "summary": {
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_net_inflow": 0.0,
                "style_count": len(items),
            },
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }


def _build_empty_intent_response(message: str = "暂无可用数据") -> dict[str, Any]:
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    item = {
        "mode": "均衡震荡",
        "mode_key": "balanced",
        "summary": "当前暂无足够数据，暂按均衡震荡处理。",
        "core_value": 0.0,
        "attack_score": 0.0,
        "defense_score": 0.0,
        "risk_level": "中",
        "top_inflow_style": "",
        "top_outflow_style": "",
        "signal_labels": ["数据不足"],
        "update_time": update_time,
    }
    return {
        "ok": True,
        "message": message,
        "data": {
            "item": item,
            "summary": {
                "updated_at": update_time,
                "mode": item["mode"],
                "mode_key": item["mode_key"],
                "top_inflow_style": item["top_inflow_style"],
                "top_outflow_style": item["top_outflow_style"],
            },
        },
    }


def _fetch_etf_spot_df():
    if ak is None:
        raise RuntimeError("AKShare 未安装，无法获取 ETF 数据")

    dataframe = ak.fund_etf_spot_em()
    if dataframe is None or dataframe.empty:
        raise RuntimeError("ETF 实时行情为空")
    return dataframe


def _parse_etf_rows_from_df(dataframe) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_row_count = len(dataframe)

    code_col = _find_col(dataframe, ["代码", "基金代码", "code"])
    name_col = _find_col(dataframe, ["名称", "基金简称", "name"])
    pct_col = _find_col(dataframe, ["涨跌幅", "涨跌幅%", "pct_chg", "change_percent"])
    amount_col = _find_col(dataframe, ["成交额", "amount", "成交金额"])
    price_col = _find_col(dataframe, ["最新价", "最新", "price", "最新净值"])

    if not code_col or not name_col:
        raise RuntimeError(f"ETF 数据字段异常，当前字段: {list(dataframe.columns)}")

    rows: list[dict[str, Any]] = []
    unmatched_codes: list[str] = []
    for _, row in dataframe.iterrows():
        raw_code = row.get(code_col)
        code = _normalize_code(raw_code)
        if code not in ETF_STYLE_MAP:
            if code and len(unmatched_codes) < 10:
                unmatched_codes.append(str(raw_code))
            continue

        name = str(row.get(name_col, "")).strip() or code
        change_pct = _safe_float(row.get(pct_col), 0.0) if pct_col else 0.0
        amount = _safe_float(row.get(amount_col), 0.0) if amount_col else 0.0
        latest_price = _safe_float(row.get(price_col), 0.0) if price_col else 0.0
        style_name = ETF_STYLE_MAP[code]

        # V1 代理资金流：成交额 * 涨跌幅 / 100。
        flow = amount * (change_pct / 100.0)

        rows.append(
            {
                "code": code,
                "name": name,
                "style_name": style_name,
                "latest_price": round(latest_price, 4) if latest_price else 0.0,
                "change_pct": round(change_pct, 2),
                "amount": round(amount, 2),
                "flow": round(flow, 2),
            }
        )

    if not rows:
        raise RuntimeError(
            "未命中任何已映射 ETF，"
            f"raw_rows={raw_row_count}, code_col={code_col}, name_col={name_col}, "
            f"pct_col={pct_col}, amount_col={amount_col}, "
            f"unmatched_sample={unmatched_codes}"
        )

    debug_info = {
        "raw_etf_rows": raw_row_count,
        "mapped_etf_rows": len(rows),
        "unmatched_codes_sample": unmatched_codes,
        "code_col": code_col,
        "name_col": name_col,
        "pct_col": pct_col or "",
        "amount_col": amount_col or "",
        "price_col": price_col or "",
    }
    return rows, debug_info


def _parse_etf_rows() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    dataframe = _fetch_etf_spot_df()
    return _parse_etf_rows_from_df(dataframe)


def _calc_style_strength(net_inflow: float, avg_change_pct: float, main_flow: float) -> float:
    net_inflow_yi = net_inflow / 100000000 if net_inflow else 0.0
    main_flow_yi = main_flow / 100000000 if main_flow else 0.0

    net_score = _clamp(50 + net_inflow_yi * 2.0, 0, 100)
    pct_score = _clamp(50 + avg_change_pct * 10.0, 0, 100)
    flow_score = _clamp(50 + main_flow_yi * 3.0, 0, 100)

    score = net_score * 0.45 + pct_score * 0.30 + flow_score * 0.25
    return round(_clamp(score, 0, 100), 2)


def get_style_fund_flow() -> dict[str, Any]:
    started_at = perf_counter()
    try:
        etf_rows, debug_info = _parse_etf_rows()
    except Exception as exc:
        return _build_empty_flow_response(f"ETF 数据获取失败，已回退空结构：{exc}")

    grouped: dict[str, dict[str, Any]] = {}
    for style_name in STYLE_ORDER:
        grouped[style_name] = {
            "style_key": _style_key(style_name),
            "style_name": style_name,
            "emoji": STYLE_EMOJI.get(style_name, "📊"),
            "net_inflow": 0.0,
            "main_flow": 0.0,
            "etf_count": 0,
            "avg_change_pct": 0.0,
            "direction": "neutral",
            "strength_score": 0.0,
            "related_etfs": [],
            "_pct_sum": 0.0,
        }

    for row in etf_rows:
        style_name = row["style_name"]
        item = grouped[style_name]
        item["net_inflow"] += row["flow"]
        item["main_flow"] += row["flow"]
        item["etf_count"] += 1
        item["_pct_sum"] += row["change_pct"]
        item["related_etfs"].append(
            {
                "code": row["code"],
                "name": row["name"],
                "latest_price": row["latest_price"],
                "pct_change": row["change_pct"],
                "turnover": row["amount"],
                "main_net_inflow": row["flow"],
            }
        )

    items: list[dict[str, Any]] = []
    total_net_inflow = 0.0
    for style_name in STYLE_ORDER:
        item = grouped[style_name]
        count = item["etf_count"]
        avg_change_pct = item["_pct_sum"] / count if count > 0 else 0.0
        item["avg_change_pct"] = round(avg_change_pct, 2)

        net_inflow = round(item["net_inflow"], 2)
        main_flow = round(item["main_flow"], 2)

        if net_inflow > 0:
            direction = "up"
        elif net_inflow < 0:
            direction = "down"
        else:
            direction = "neutral"

        item["direction"] = direction
        item["net_inflow"] = net_inflow
        item["main_flow"] = main_flow
        item["strength_score"] = _calc_style_strength(net_inflow, avg_change_pct, main_flow)
        item["related_etfs"] = sorted(
            item["related_etfs"],
            key=lambda row: (row["main_net_inflow"], row["pct_change"], row["turnover"]),
            reverse=True,
        )[:5]
        item.pop("_pct_sum", None)
        total_net_inflow += net_inflow
        items.append(item)

    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "ok": True,
        "message": (
            "success | "
            f"raw_etf_rows={debug_info.get('raw_etf_rows', 0)} | "
            f"mapped_etf_rows={debug_info.get('mapped_etf_rows', 0)} | "
            f"unmatched_codes_sample={debug_info.get('unmatched_codes_sample', [])}"
        ),
        "data": {
            "items": items,
            "summary": {
                "updated_at": update_time,
                "total_net_inflow": round(total_net_inflow, 2),
                "style_count": len(items),
                "elapsed_seconds": round(perf_counter() - started_at, 3),
                "raw_etf_rows": debug_info.get("raw_etf_rows", 0),
                "mapped_etf_rows": debug_info.get("mapped_etf_rows", 0),
                "unmatched_codes_sample": debug_info.get("unmatched_codes_sample", []),
                "code_col": debug_info.get("code_col", ""),
                "name_col": debug_info.get("name_col", ""),
                "pct_col": debug_info.get("pct_col", ""),
                "amount_col": debug_info.get("amount_col", ""),
                "price_col": debug_info.get("price_col", ""),
            },
            "update_time": update_time,
        },
    }


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 2) if values else 0.0


def _build_intent_summary(mode: str, top_inflow_style: str, top_outflow_style: str) -> str:
    if mode == "题材进攻增强":
        return f"{top_inflow_style} 方向更强，资金风险偏好回升，市场偏向进攻。"
    if mode == "成长修复":
        return f"{top_inflow_style} 出现修复迹象，但整体进攻力度仍需继续观察。"
    if mode == "均衡震荡":
        return "进攻与防御风格强弱接近，市场暂时缺少明确主线，短期偏均衡震荡。"
    if mode == "防御偏好增强":
        return f"{top_inflow_style} 相对占优，{top_outflow_style} 承压，市场更偏向稳健配置。"
    if mode == "全面防守":
        return f"{top_outflow_style} 方向承压明显，资金风险偏好下降，市场进入防守状态。"
    return "当前风格信号中性。"


def get_style_intent() -> dict[str, Any]:
    flow_response = get_style_fund_flow()
    try:
        items = list(((flow_response.get("data") or {}).get("items")) or [])
        if not items:
            return _build_empty_intent_response("风格数据为空")
    except Exception:
        return _build_empty_intent_response("风格数据结构异常")

    item_map = {item["style_name"]: item for item in items}

    attack_scores = [item_map[name]["strength_score"] for name in STYLE_ORDER if name in ATTACK_STYLES and name in item_map]
    defense_scores = [item_map[name]["strength_score"] for name in STYLE_ORDER if name in DEFENSE_STYLES and name in item_map]

    attack_score = _avg(attack_scores)
    defense_score = _avg(defense_scores)
    core_value = round(attack_score - defense_score, 2)

    sorted_by_flow = sorted(items, key=lambda item: item["net_inflow"], reverse=True)
    top_inflow_style = sorted_by_flow[0]["style_name"] if sorted_by_flow else ""
    top_outflow_style = sorted_by_flow[-1]["style_name"] if sorted_by_flow else ""

    high_beta_item = item_map.get("高波题材", {})
    growth_item = item_map.get("中小成长", {})
    dividend_item = item_map.get("红利防御", {})
    finance_item = item_map.get("大金融", {})
    broad_item = item_map.get("大盘宽基", {})
    commodity_item = item_map.get("大宗商品", {})

    high_beta_flow = _safe_float(high_beta_item.get("net_inflow"))
    growth_flow = _safe_float(growth_item.get("net_inflow"))
    dividend_flow = _safe_float(dividend_item.get("net_inflow"))
    finance_flow = _safe_float(finance_item.get("net_inflow"))
    broad_flow = _safe_float(broad_item.get("net_inflow"))
    commodity_flow = _safe_float(commodity_item.get("net_inflow"))

    if core_value > 15 and high_beta_flow > 0 and growth_flow > 0:
        mode = "题材进攻增强"
        mode_key = "theme_attack"
        risk_level = "中高"
        signal_labels = ["进攻增强", "题材活跃", "成长占优"]
    elif 5 < core_value <= 15 and growth_flow > 0:
        mode = "成长修复"
        mode_key = "growth_repair"
        risk_level = "中"
        signal_labels = ["成长修复", "进攻回暖"]
    elif -5 <= core_value <= 5:
        mode = "均衡震荡"
        mode_key = "balanced"
        risk_level = "中"
        signal_labels = ["风格均衡", "震荡整理"]
    elif -15 <= core_value < -5 and (dividend_flow > 0 or finance_flow > 0 or broad_flow > 0):
        mode = "防御偏好增强"
        mode_key = "defense_preferred"
        risk_level = "中"
        signal_labels = ["防御增强", "稳健偏好", "题材承压"]
    else:
        attack_out_count = sum(1 for value in [high_beta_flow, growth_flow, commodity_flow] if value < 0)
        if core_value < -15 or attack_out_count >= 2:
            mode = "全面防守"
            mode_key = "full_defense"
            risk_level = "高"
            signal_labels = ["全面防守", "风险偏好回落", "资金撤退"]
        else:
            mode = "均衡震荡"
            mode_key = "balanced"
            risk_level = "中"
            signal_labels = ["风格均衡", "主线不明"]

    summary = _build_intent_summary(mode, top_inflow_style, top_outflow_style)
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "ok": True,
        "message": "success",
        "data": {
            "item": {
                "mode": mode,
                "mode_key": mode_key,
                "summary": summary,
                "core_value": core_value,
                "attack_score": attack_score,
                "defense_score": defense_score,
                "risk_level": risk_level,
                "top_inflow_style": top_inflow_style,
                "top_outflow_style": top_outflow_style,
                "signal_labels": signal_labels,
                "update_time": update_time,
            },
            "summary": {
                "updated_at": update_time,
                "mode": mode,
                "mode_key": mode_key,
                "top_inflow_style": top_inflow_style,
                "top_outflow_style": top_outflow_style,
            },
        },
    }


def get_style_fund_flow_service() -> dict[str, Any]:
    payload = get_style_fund_flow()
    data = payload.get("data") or {}
    return {
        "items": list(data.get("items") or []),
        "summary": dict(data.get("summary") or {}),
        "elapsed_seconds": _safe_float((data.get("summary") or {}).get("elapsed_seconds")),
    }


def get_style_intent_service() -> dict[str, Any]:
    payload = get_style_intent()
    data = payload.get("data") or {}
    flow_payload = get_style_fund_flow()
    flow_data = flow_payload.get("data") or {}
    return {
        "item": dict(data.get("item") or {}),
        "styles": list(flow_data.get("items") or []),
        "summary": dict(data.get("summary") or {}),
        "elapsed_seconds": _safe_float((flow_data.get("summary") or {}).get("elapsed_seconds")),
    }


if __name__ == "__main__":
    from pprint import pprint

    pprint(get_style_fund_flow())
    pprint(get_style_intent())
