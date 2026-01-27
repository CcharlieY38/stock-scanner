"""
预测聚合器 - 生成核心预测摘要与置信度评估
模块X：整合所有预测信号，输出简洁的操作建议
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum


class ConfidenceLevel(Enum):
    """置信度等级"""
    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


@dataclass
class PredictionSummary:
    """预测摘要"""
    prediction_date: str                    # 预测目标日期（明日）
    generation_time: str                    # 生成时间
    
    # 核心预测
    top_mainline: str                       # 预测主线
    top_mainline_score: float               # 主线强度分
    top_mainline_confidence: str            # 置信度
    
    # 核心依据
    key_indicators: Dict[str, float]        # 关键指标
    core_reasons: List[str]                 # 核心理由
    
    # 龙头标的
    primary_leaders: List[Dict]             # 主力龙头（2-3只）
    alternative_leaders: List[Dict]         # 备选龙头（2-3只）
    
    # 超跌机会
    oversold_opportunities: List[Dict]      # 超跌反弹机会（2-3只）
    
    # 风险提示
    risk_warnings: List[str]                # 风险点
    market_sentiment: str                   # 市场情绪（"乐观"/"中性"/"谨慎"）
    
    # 操作建议
    action_recommendation: str              # 操作建议
    entry_timing: str                       # 入场时机
    position_sizing: str                    # 仓位建议
    
    def to_dict(self):
        return {
            'prediction_date': self.prediction_date,
            'generation_time': self.generation_time,
            'top_mainline': self.top_mainline,
            'top_mainline_score': round(self.top_mainline_score, 2),
            'top_mainline_confidence': self.top_mainline_confidence,
            'key_indicators': {k: round(v, 2) for k, v in self.key_indicators.items()},
            'core_reasons': self.core_reasons,
            'primary_leaders': self.primary_leaders,
            'alternative_leaders': self.alternative_leaders,
            'oversold_opportunities': self.oversold_opportunities,
            'risk_warnings': self.risk_warnings,
            'market_sentiment': self.market_sentiment,
            'action_recommendation': self.action_recommendation,
            'entry_timing': self.entry_timing,
            'position_sizing': self.position_sizing
        }


class PredictionAggregator:
    """预测聚合器"""
    
    def __init__(self, config=None):
        self.config = config or {}
        
        # 置信度阈值配置
        self.HIGH_CONFIDENCE_SCORE_GAP = 15  # 主线强度分断层领先阈值
        self.HIGH_CONFIDENCE_MIN_SCORE = 80  # 高置信度最低分数
        self.MEDIUM_CONFIDENCE_MIN_SCORE = 65  # 中置信度最低分数
    
    def generate_prediction_summary(self, 
                                   mainlines: List,
                                   leaders: List,
                                   oversold: List) -> PredictionSummary:
        """
        生成核心预测摘要
        
        Args:
            mainlines: 预测主线列表
            leaders: 龙头股池
            oversold: 超跌候选
        
        Returns:
            预测摘要对象
        """
        print("🔮 开始生成预测摘要...")
        
        # 1. 确定目标日期
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 2. 识别顶级主线
        if not mainlines:
            return self._generate_empty_summary(tomorrow, now_time)
        
        top_mainline = mainlines[0]
        
        # 3. 评估置信度
        confidence = self._evaluate_overall_confidence(mainlines, leaders)
        
        # 4. 提取关键指标
        key_indicators = {
            '动量加速度': top_mainline.momentum_acceleration,
            '内部强度': top_mainline.internal_strength,
            '资金背离度': top_mainline.capital_divergence,
            '情绪极点': top_mainline.sentiment_extreme,
            '趋势惯性': top_mainline.trend_inertia
        }
        
        # 5. 生成核心理由
        core_reasons = self._generate_core_reasons(top_mainline, mainlines)
        
        # 6. 筛选龙头标的
        primary_leaders, alternative_leaders = self._select_leader_targets(
            leaders, top_mainline.industry_name
        )
        
        # 7. 筛选超跌机会
        oversold_opportunities = self._select_oversold_opportunities(
            oversold, top_mainline.industry_name
        )
        
        # 8. 识别风险点
        risk_warnings = self._identify_risks(mainlines, leaders, top_mainline)
        
        # 9. 评估市场情绪
        market_sentiment = self._assess_market_sentiment(top_mainline, mainlines)
        
        # 10. 生成操作建议
        action_recommendation = self._generate_action_recommendation(
            confidence, top_mainline, primary_leaders
        )
        
        entry_timing = self._suggest_entry_timing(
            top_mainline, market_sentiment, confidence
        )
        
        position_sizing = self._suggest_position_sizing(confidence, risk_warnings)
        
        return PredictionSummary(
            prediction_date=tomorrow,
            generation_time=now_time,
            top_mainline=top_mainline.industry_name,
            top_mainline_score=top_mainline.prediction_score,
            top_mainline_confidence=confidence,
            key_indicators=key_indicators,
            core_reasons=core_reasons,
            primary_leaders=primary_leaders,
            alternative_leaders=alternative_leaders,
            oversold_opportunities=oversold_opportunities,
            risk_warnings=risk_warnings,
            market_sentiment=market_sentiment,
            action_recommendation=action_recommendation,
            entry_timing=entry_timing,
            position_sizing=position_sizing
        )
    
    def _evaluate_overall_confidence(self, mainlines: List, leaders: List) -> str:
        """
        评估整体置信度
        
        逻辑：
        - 高：主线强度分断层领先（>15分），且龙头资金情绪共振
        - 中：主线排名清晰，但龙头信号一般
        - 低：主线强度分接近，市场混沌
        """
        if not mainlines or len(mainlines) < 2:
            return ConfidenceLevel.LOW.value
        
        top_mainline = mainlines[0]
        second_mainline = mainlines[1]
        
        # 计算主线分数差距
        score_gap = top_mainline.prediction_score - second_mainline.prediction_score
        
        # 检查龙头资金情绪共振
        top_leaders_in_mainline = [
            l for l in leaders[:5] 
            if l.industry == top_mainline.industry_name
        ]
        
        capital_sentiment_resonance = False
        if top_leaders_in_mainline:
            avg_capital_score = sum(l.capital_score for l in top_leaders_in_mainline) / len(top_leaders_in_mainline)
            avg_sentiment_score = sum(l.sentiment_score for l in top_leaders_in_mainline) / len(top_leaders_in_mainline)
            
            if avg_capital_score > 80 and avg_sentiment_score > 75:
                capital_sentiment_resonance = True
        
        # 判断置信度
        if (score_gap >= self.HIGH_CONFIDENCE_SCORE_GAP and 
            top_mainline.prediction_score >= self.HIGH_CONFIDENCE_MIN_SCORE and
            capital_sentiment_resonance):
            return ConfidenceLevel.HIGH.value
        
        elif (score_gap >= 8 and 
              top_mainline.prediction_score >= self.MEDIUM_CONFIDENCE_MIN_SCORE):
            return ConfidenceLevel.MEDIUM.value
        
        else:
            return ConfidenceLevel.LOW.value
    
    def _generate_core_reasons(self, top_mainline, all_mainlines: List) -> List[str]:
        """生成核心理由"""
        reasons = []
        
        # 分数断层
        if len(all_mainlines) >= 2:
            gap = top_mainline.prediction_score - all_mainlines[1].prediction_score
            if gap >= 15:
                reasons.append(f"预测强度分断层领先（领先第2名 {gap:.1f} 分）")
        
        # 动量加速
        if top_mainline.momentum_acceleration > 75:
            reasons.append(f"动量加速度极强（{top_mainline.momentum_acceleration:.1f}）")
        elif top_mainline.momentum_acceleration > 60:
            reasons.append(f"动量加速度较强（+{top_mainline.momentum_acceleration:.1f}）")
        
        # 内部强度
        if top_mainline.internal_strength > 80:
            reasons.append(f"内部强度极高（{top_mainline.new_high_stocks}只创新高）")
        elif top_mainline.internal_strength > 65:
            reasons.append(f"内部分化有限，强势股占比高")
        
        # 资金背离
        if top_mainline.capital_divergence > 70:
            reasons.append("资金情绪共振（价涨资金同步增加）")
        elif top_mainline.capital_divergence < 40:
            reasons.append("⚠️ 资金背离（价涨资金减少，需警惕）")
        
        # 趋势惯性
        if top_mainline.continuity_days >= 5:
            reasons.append(f"趋势惯性强（已持续 {top_mainline.continuity_days} 天）")
        
        return reasons[:5]  # 最多5条
    
    def _select_leader_targets(self, leaders: List, top_industry: str) -> tuple:
        """筛选龙头标的"""
        # 主力龙头：属于顶级主线 + 综合评分最高
        primary = [
            {
                'code': l.stock_code,
                'name': l.stock_name,
                'type': l.leader_type,
                'score': round(l.leader_score, 1),
                'capital_flow': l.capital_flow_3d.get('main_force_net', 0),
                'boost': round(l.prediction_boost, 2)
            }
            for l in leaders[:5]
            if l.industry == top_industry
        ][:3]
        
        # 备选龙头：其他高分龙头
        alternative = [
            {
                'code': l.stock_code,
                'name': l.stock_name,
                'type': l.leader_type,
                'score': round(l.leader_score, 1),
                'industry': l.industry
            }
            for l in leaders[len(primary):len(primary)+3]
        ]
        
        return primary, alternative
    
    def _select_oversold_opportunities(self, oversold: List, top_industry: str) -> List[Dict]:
        """筛选超跌机会"""
        opportunities = []
        
        for candidate in oversold[:5]:
            # 优先推荐顶级主线的超跌股
            priority = "⭐⭐⭐" if candidate.industry == top_industry else "⭐⭐"
            
            opportunities.append({
                'code': candidate.stock_code,
                'name': candidate.stock_name,
                'industry': candidate.industry,
                'coefficient': round(candidate.adjusted_coefficient, 2),
                'drawdown': round(candidate.drawdown_from_high, 1),
                'target': round(candidate.target_price, 2),
                'potential': candidate.rebound_potential,
                'priority': priority
            })
        
        return opportunities[:3]
    
    def _identify_risks(self, mainlines: List, leaders: List, top_mainline) -> List[str]:
        """识别风险点"""
        warnings = []
        
        # 情绪过热
        if top_mainline.sentiment_extreme < 50:
            warnings.append("⚠️ 情绪极度乐观，警惕短期见顶")
        
        # 资金背离
        if top_mainline.capital_divergence < 40:
            warnings.append("⚠️ 资金背离（价涨量缩），持续性存疑")
        
        # 主线分化
        if len(mainlines) >= 2:
            gap = mainlines[0].prediction_score - mainlines[1].prediction_score
            if gap < 5:
                warnings.append("⚠️ 主线分化不明显，市场混沌")
        
        # 龙头资金流出
        outflow_leaders = [
            l for l in leaders[:5]
            if l.capital_flow_trend == "流出"
        ]
        if len(outflow_leaders) >= 2:
            warnings.append(f"⚠️ {len(outflow_leaders)}只龙头出现资金流出")
        
        # 内部分化
        if top_mainline.strong_stocks < top_mainline.total_stocks * 0.2:
            warnings.append("⚠️ 板块内部分化严重，强势股占比低")
        
        return warnings
    
    def _assess_market_sentiment(self, top_mainline, all_mainlines: List) -> str:
        """评估市场情绪"""
        # 综合多个主线的情绪指标
        avg_sentiment = np.mean([ml.sentiment_extreme for ml in all_mainlines[:3]])
        
        if avg_sentiment > 70:
            return "乐观"
        elif avg_sentiment > 50:
            return "中性"
        else:
            return "谨慎"
    
    def _generate_action_recommendation(self, confidence: str, 
                                       top_mainline, primary_leaders: List) -> str:
        """生成操作建议"""
        if confidence == ConfidenceLevel.HIGH.value:
            if primary_leaders:
                return f"✅ 建议重点关注 {top_mainline.industry_name}，可考虑配置龙头股 {', '.join([l['code'] for l in primary_leaders[:2]])}"
            else:
                return f"✅ 建议关注 {top_mainline.industry_name} 行业机会"
        
        elif confidence == ConfidenceLevel.MEDIUM.value:
            return f"⚡ {top_mainline.industry_name} 有一定机会，建议谨慎参与，控制仓位"
        
        else:
            return f"👀 市场主线不明确，建议观望或轻仓试探"
    
    def _suggest_entry_timing(self, top_mainline, market_sentiment: str, 
                             confidence: str) -> str:
        """建议入场时机"""
        if confidence == ConfidenceLevel.HIGH.value:
            if top_mainline.momentum_acceleration > 75:
                return "开盘后寻找龙头股回调机会"
            else:
                return "盘中观察，震荡回调时分批布局"
        
        elif confidence == ConfidenceLevel.MEDIUM.value:
            return "观察盘面强弱，强势时可小仓位参与"
        
        else:
            return "等待更明确信号，暂不入场"
    
    def _suggest_position_sizing(self, confidence: str, risk_warnings: List) -> str:
        """建议仓位控制"""
        base_position = {
            ConfidenceLevel.HIGH.value: "30-50%",
            ConfidenceLevel.MEDIUM.value: "20-30%",
            ConfidenceLevel.LOW.value: "10%以下或空仓"
        }
        
        suggested = base_position.get(confidence, "10%以下")
        
        if len(risk_warnings) >= 3:
            return f"{suggested}（风险较多，建议降低仓位）"
        else:
            return suggested
    
    def _generate_empty_summary(self, tomorrow: str, now_time: str) -> PredictionSummary:
        """生成空摘要（无有效预测）"""
        return PredictionSummary(
            prediction_date=tomorrow,
            generation_time=now_time,
            top_mainline="无明确主线",
            top_mainline_score=0,
            top_mainline_confidence=ConfidenceLevel.LOW.value,
            key_indicators={},
            core_reasons=["市场信号不明确"],
            primary_leaders=[],
            alternative_leaders=[],
            oversold_opportunities=[],
            risk_warnings=["市场缺乏明确方向"],
            market_sentiment="谨慎",
            action_recommendation="建议观望，等待更明确信号",
            entry_timing="暂不入场",
            position_sizing="空仓"
        )
    
    def format_summary_markdown(self, summary: PredictionSummary) -> str:
        """格式化为Markdown输出"""
        md = []
        md.append(f"# 🔮 核心预测摘要 ({summary.prediction_date})\n")
        md.append(f"*生成时间: {summary.generation_time}*\n")
        md.append("---\n")
        
        # 核心预测
        md.append("## 🎯 核心预测\n")
        md.append(f"- **预测主线**: {summary.top_mainline}")
        md.append(f"- **强度分**: {summary.top_mainline_score:.1f}")
        md.append(f"- **置信度**: {'🔥' if summary.top_mainline_confidence == '高' else '⚡' if summary.top_mainline_confidence == '中' else '👀'} {summary.top_mainline_confidence}\n")
        
        # 核心依据
        if summary.core_reasons:
            md.append("## 📊 核心依据\n")
            for reason in summary.core_reasons:
                md.append(f"- {reason}")
            md.append("")
        
        # 关键指标
        md.append("## 📈 关键指标\n")
        for indicator, value in summary.key_indicators.items():
            md.append(f"- **{indicator}**: {value:.1f}")
        md.append("")
        
        # 龙头标的
        if summary.primary_leaders:
            md.append("## 👑 主力龙头标的\n")
            for leader in summary.primary_leaders:
                md.append(f"- **{leader['code']} {leader['name']}** ({leader['type']}) - 评分: {leader['score']}, 资金流入: {leader['capital_flow']:.2f}亿")
            md.append("")
        
        # 超跌机会
        if summary.oversold_opportunities:
            md.append("## 🎣 超跌反弹机会\n")
            for opp in summary.oversold_opportunities:
                md.append(f"- {opp['priority']} **{opp['code']} {opp['name']}** - 回撤: {opp['drawdown']}%, 目标价: ¥{opp['target']}")
            md.append("")
        
        # 风险提示
        if summary.risk_warnings:
            md.append("## ⚠️ 风险提示\n")
            for warning in summary.risk_warnings:
                md.append(f"- {warning}")
            md.append("")
        
        # 操作建议
        md.append("## 💡 操作建议\n")
        md.append(f"- **市场情绪**: {summary.market_sentiment}")
        md.append(f"- **行动方案**: {summary.action_recommendation}")
        md.append(f"- **入场时机**: {summary.entry_timing}")
        md.append(f"- **仓位建议**: {summary.position_sizing}\n")
        
        md.append("---\n")
        md.append("*本预测基于量化模型生成，仅供参考，不构成投资建议*")
        
        return "\n".join(md)


# 需要numpy
import numpy as np
