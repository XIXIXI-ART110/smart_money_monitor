from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from typing import Dict, Any, List


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if isfinite(parsed) else default


def normalize_score_input(data: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if isinstance(data, Mapping):
        return data
    return {}


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def score_level(total_score: int) -> str:
    if total_score >= 80:
        return "A"
    elif total_score >= 65:
        return "B"
    elif total_score >= 50:
        return "C"
    return "D"


@dataclass
class ScoreResult:
    total_score: int
    low_score: int
    volume_score: int
    trend_score: int
    capital_score: int
    level: str
    conclusion: str
    tags: List[str]
    details: Dict[str, Any]


class StockScoreService:
    """
    股票评分服务（低位机会模型 V1）
    总分 = 低位40 + 量能25 + 趋势25 + 资金10
    """

    def score(self, data: Mapping[str, Any] | None = None) -> Dict[str, Any]:
        data = normalize_score_input(data)
        low_score, low_detail, low_tags = self._score_low_position(data)
        volume_score, volume_detail, volume_tags = self._score_volume(data)
        trend_score, trend_detail, trend_tags = self._score_trend(data)
        capital_score, capital_detail, capital_tags = self._score_capital(data)

        total_score = low_score + volume_score + trend_score + capital_score
        total_score = int(clamp(round(total_score), 0, 100))
        level = score_level(total_score)

        tags = low_tags + volume_tags + trend_tags + capital_tags
        conclusion = self._build_conclusion(
            total_score=total_score,
            low_score=low_score,
            volume_score=volume_score,
            trend_score=trend_score,
            capital_score=capital_score,
            change_pct=safe_float(data.get("change_pct")),
            tags=tags,
        )

        result = ScoreResult(
            total_score=total_score,
            low_score=low_score,
            volume_score=volume_score,
            trend_score=trend_score,
            capital_score=capital_score,
            level=level,
            conclusion=conclusion,
            tags=tags,
            details={
                "low": low_detail,
                "volume": volume_detail,
                "trend": trend_detail,
                "capital": capital_detail,
            },
        )

        return {
            "total_score": result.total_score,
            "level": result.level,
            "sub_scores": {
                "low": result.low_score,
                "volume": result.volume_score,
                "trend": result.trend_score,
                "capital": result.capital_score,
            },
            "conclusion": result.conclusion,
            "tags": result.tags,
            "details": result.details,
        }

    def _score_low_position(self, data: Mapping[str, Any]):
        price = safe_float(data.get("price"))
        high_60d = safe_float(data.get("high_60d"))
        low_60d = safe_float(data.get("low_60d"))
        high_120d = safe_float(data.get("high_120d"), high_60d)

        score = 0.0
        tags = []
        detail = {}

        if price > 0 and high_60d > low_60d:
            range_pos = (price - low_60d) / (high_60d - low_60d)
            detail["range_position_60d"] = round(range_pos, 4)

            if range_pos <= 0.20:
                score += 24
                tags.append("60日强低位")
            elif range_pos <= 0.35:
                score += 18
                tags.append("60日偏低位")
            elif range_pos <= 0.50:
                score += 12
            elif range_pos <= 0.70:
                score += 6
            else:
                score += 0
                tags.append("位置偏高")

        if price > 0 and high_60d > 0:
            drawdown_60d = (high_60d - price) / high_60d
            detail["drawdown_from_60d_high"] = round(drawdown_60d, 4)

            if drawdown_60d >= 0.30:
                score += 10
                tags.append("距60日高点回撤较大")
            elif drawdown_60d >= 0.20:
                score += 7
            elif drawdown_60d >= 0.10:
                score += 4

        if price > 0 and high_120d > 0:
            drawdown_120d = (high_120d - price) / high_120d
            detail["drawdown_from_120d_high"] = round(drawdown_120d, 4)

            if drawdown_120d >= 0.40:
                score += 6
                tags.append("中期回撤充分")
            elif drawdown_120d >= 0.25:
                score += 4
            elif drawdown_120d >= 0.15:
                score += 2

        score = int(clamp(round(score), 0, 40))
        return score, detail, tags

    def _score_volume(self, data: Mapping[str, Any]):
        volume_ratio = safe_float(data.get("volume_ratio"))
        turnover_rate = safe_float(data.get("turnover_rate"))
        change_pct = safe_float(data.get("change_pct"))

        score = 0.0
        tags = []
        detail = {
            "volume_ratio": volume_ratio,
            "turnover_rate": turnover_rate,
            "change_pct": change_pct,
        }

        if volume_ratio >= 2.0:
            score += 12
            tags.append("明显放量")
        elif volume_ratio >= 1.5:
            score += 9
            tags.append("温和放量")
        elif volume_ratio >= 1.2:
            score += 6
        elif volume_ratio >= 0.9:
            score += 3

        if 2 <= turnover_rate <= 8:
            score += 8
            tags.append("换手健康")
        elif 1 <= turnover_rate < 2:
            score += 5
        elif 8 < turnover_rate <= 12:
            score += 4
            tags.append("换手偏高")
        elif turnover_rate > 12:
            score += 1
            tags.append("换手过热")

        if change_pct > 0:
            if volume_ratio >= 1.5 and change_pct >= 2:
                score += 5
                tags.append("价量配合较好")
            elif volume_ratio >= 1.2 and change_pct >= 0.5:
                score += 3
        else:
            if volume_ratio >= 1.5 and change_pct < -2:
                score -= 2
                tags.append("放量下跌")

        score = int(clamp(round(score), 0, 25))
        return score, detail, tags

    def _score_trend(self, data: Mapping[str, Any]):
        price = safe_float(data.get("price"))
        ma5 = safe_float(data.get("ma5"))
        ma10 = safe_float(data.get("ma10"))
        ma20 = safe_float(data.get("ma20"))
        ma60 = safe_float(data.get("ma60"))

        score = 0.0
        tags = []
        detail = {
            "price": price,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
        }

        if price > 0:
            if ma5 > 0 and price >= ma5:
                score += 4
            if ma10 > 0 and price >= ma10:
                score += 5
            if ma20 > 0 and price >= ma20:
                score += 6
                tags.append("站上20日线")
            if ma60 > 0 and price >= ma60:
                score += 4
                tags.append("站上60日线")

        if ma5 > 0 and ma10 > 0 and ma20 > 0:
            if ma5 >= ma10 >= ma20:
                score += 4
                tags.append("短线均线走强")
            elif ma5 >= ma10:
                score += 2

        if ma20 > 0 and ma60 > 0:
            if ma20 >= ma60:
                score += 2
                tags.append("中期趋势转强")

        score = int(clamp(round(score), 0, 25))
        return score, detail, tags

    def _score_capital(self, data: Mapping[str, Any]):
        main_net_inflow = safe_float(data.get("main_net_inflow"))

        score = 0.0
        tags = []
        detail = {
            "main_net_inflow": main_net_inflow,
        }

        if main_net_inflow >= 100000000:
            score += 10
            tags.append("主力明显流入")
        elif main_net_inflow >= 30000000:
            score += 7
            tags.append("主力净流入")
        elif main_net_inflow > 0:
            score += 4
        elif main_net_inflow <= -100000000:
            score += 0
            tags.append("主力大幅流出")
        elif main_net_inflow <= -30000000:
            score += 1
            tags.append("主力流出")
        else:
            score += 2

        score = int(clamp(round(score), 0, 10))
        return score, detail, tags

    def _build_conclusion(
        self,
        total_score: int,
        low_score: int,
        volume_score: int,
        trend_score: int,
        capital_score: int,
        change_pct: float,
        tags: List[str],
    ) -> str:
        if total_score >= 80:
            base = "综合评分很强，可作为重点跟踪对象"
        elif total_score >= 65:
            base = "综合表现较好，适合加入观察名单"
        elif total_score >= 50:
            base = "有一定信号，但还需要继续观察"
        else:
            base = "当前信号一般，暂时不算理想机会"

        reasons = []

        if low_score >= 28:
            reasons.append("位置偏低")
        if volume_score >= 16:
            reasons.append("量能有配合")
        if trend_score >= 16:
            reasons.append("趋势转强")
        if capital_score >= 6:
            reasons.append("资金偏积极")

        if change_pct >= 7:
            reasons.append("短线涨幅较大，注意别追高")
        elif change_pct <= -4:
            reasons.append("短线波动偏弱，先观察承接")

        if not reasons:
            return base

        return f"{base}；主要原因：{'、'.join(reasons[:3])}。"
