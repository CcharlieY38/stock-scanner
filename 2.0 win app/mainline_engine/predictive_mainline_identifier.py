"""
预测型主线识别引擎 - 从"描述现状"升级为"预测未来"
重点：领先指标、动量加速、内部强度、资金背离
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum


class MainlineStatus(Enum):
    """主线状态"""
    STRONG_PREDICTION = "预测强势"      # 预测明日继续强势
    EMERGING_SIGNAL = "新兴信号"        # 出现加速信号
    WEAKENING = "即将转弱"              # 动量衰减
    OBSERVATION = "观察等待"            # 信号不明确


@dataclass
class PredictiveMainlineStrength:
    """预测型主线强度数据结构"""
    industry_name: str
    prediction_score: float          # 预测强度分（面向明日）
    current_score: float             # 当前强度分（描述今日）
    rank: int
    status: MainlineStatus
    continuity_days: int
    
    # 领先指标分解
    momentum_acceleration: float     # 动量加速度 (0-100)
    internal_strength: float         # 内部强度 (0-100)
    capital_divergence: float        # 资金背离度 (0-100)
    sentiment_extreme: float         # 情绪极点 (0-100)
    trend_inertia: float            # 趋势惯性 (0-100)
    
    # 统计信息
    total_stocks: int               # 成分股数量
    strong_stocks: int              # 强势股数量
    new_high_stocks: int            # 创新高股数量
    
    # 置信度评估
    prediction_confidence: str      # "高" / "中" / "低"
    confidence_reasons: List[str]   # 置信度原因
    
    def to_dict(self):
        return {
            'industry_name': self.industry_name,
            'prediction_score': round(self.prediction_score, 2),
            'current_score': round(self.current_score, 2),
            'rank': self.rank,
            'status': self.status.value,
            'continuity_days': self.continuity_days,
            'momentum_acceleration': round(self.momentum_acceleration, 2),
            'internal_strength': round(self.internal_strength, 2),
            'capital_divergence': round(self.capital_divergence, 2),
            'sentiment_extreme': round(self.sentiment_extreme, 2),
            'trend_inertia': round(self.trend_inertia, 2),
            'total_stocks': self.total_stocks,
            'strong_stocks': self.strong_stocks,
            'new_high_stocks': self.new_high_stocks,
            'prediction_confidence': self.prediction_confidence,
            'confidence_reasons': self.confidence_reasons
        }


class PredictiveMainlineEngine:
    """预测型主线识别引擎"""
    
    # 预测权重配置（领先指标优先）
    PREDICTION_WEIGHTS = {
        'momentum_acceleration': 0.30,   # 动量加速度（最重要）
        'internal_strength': 0.25,       # 内部强度
        'capital_divergence': 0.20,      # 资金背离度
        'sentiment_extreme': 0.15,       # 情绪极点
        'trend_inertia': 0.10           # 趋势惯性（降低滞后指标权重）
    }
    
    def __init__(self, analyzer, config=None):
        self.analyzer = analyzer
        self.config = config or {}
        self.history = {}  # 历史数据缓存
        
        # 可配置参数
        self.min_prediction_score = config.get('min_prediction_score', 60)
        self.top_n = config.get('top_n', 10)
        self.lookback_days = config.get('lookback_days', 30)
    
    def identify_predictive_mainlines(self, date=None) -> List[PredictiveMainlineStrength]:
        """
        识别预测型主线（面向明日）
        
        Args:
            date: 分析日期，默认为今日
        
        Returns:
            预测主线列表，按预测强度分排序
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"🔮 开始预测型主线识别 - 目标日期: {date}")
        
        # 1. 获取所有行业列表
        industries = self._get_all_industries()
        print(f"✅ 获取到 {len(industries)} 个行业")
        
        mainline_strengths = []
        
        # 2. 对每个行业计算预测强度
        for industry in industries:
            try:
                strength = self._calculate_predictive_industry_strength(industry, date)
                if strength and strength.prediction_score >= self.min_prediction_score:
                    mainline_strengths.append(strength)
            except Exception as e:
                print(f"⚠️ 分析行业 {industry} 失败: {str(e)}")
                continue
        
        # 3. 按预测强度排序
        mainline_strengths.sort(key=lambda x: x.prediction_score, reverse=True)
        
        # 4. 分配排名
        for i, strength in enumerate(mainline_strengths[:self.top_n], 1):
            strength.rank = i
        
        # 5. 更新历史记录
        self._update_history(date, mainline_strengths[:self.top_n])
        
        return mainline_strengths[:self.top_n]
    
    def _calculate_predictive_industry_strength(self, industry: str, date: str) -> Optional[PredictiveMainlineStrength]:
        """
        计算单个行业的预测强度
        核心公式：预测强度分 = 加权求和(5个领先指标)
        """
        # 获取行业成分股
        stocks = self._get_industry_stocks(industry)
        if not stocks or len(stocks) < 3:
            return None
        
        # 批量获取股票数据
        stock_data = self._batch_get_stock_data(stocks, days=30)
        if not stock_data:
            return None
        
        # 计算5个领先指标
        momentum_accel = self._calculate_momentum_acceleration(stock_data)
        internal_strength = self._calculate_internal_strength(stock_data)
        capital_divergence = self._calculate_capital_divergence(stock_data)
        sentiment_extreme = self._calculate_sentiment_extreme(stock_data)
        trend_inertia = self._calculate_trend_inertia(stock_data)
        
        # 计算预测强度分（面向明日）
        prediction_score = (
            self.PREDICTION_WEIGHTS['momentum_acceleration'] * momentum_accel +
            self.PREDICTION_WEIGHTS['internal_strength'] * internal_strength +
            self.PREDICTION_WEIGHTS['capital_divergence'] * capital_divergence +
            self.PREDICTION_WEIGHTS['sentiment_extreme'] * sentiment_extreme +
            self.PREDICTION_WEIGHTS['trend_inertia'] * trend_inertia
        )
        
        # 计算当前强度分（描述今日，用于对比）
        current_score = self._calculate_current_strength(stock_data)
        
        # 判断状态
        status = self._determine_prediction_status(
            prediction_score, current_score, momentum_accel, internal_strength
        )
        
        # 评估置信度
        confidence, reasons = self._evaluate_prediction_confidence(
            prediction_score, momentum_accel, internal_strength, 
            capital_divergence, stock_data
        )
        
        # 统计信息
        total_stocks = len(stocks)
        strong_stocks = self._count_strong_stocks(stock_data)
        new_high_stocks = self._count_new_high_stocks(stock_data)
        
        # 计算持续天数
        continuity_days = self._calculate_continuity_days(industry, date)
        
        return PredictiveMainlineStrength(
            industry_name=industry,
            prediction_score=prediction_score,
            current_score=current_score,
            rank=0,  # 稍后分配
            status=status,
            continuity_days=continuity_days,
            momentum_acceleration=momentum_accel,
            internal_strength=internal_strength,
            capital_divergence=capital_divergence,
            sentiment_extreme=sentiment_extreme,
            trend_inertia=trend_inertia,
            total_stocks=total_stocks,
            strong_stocks=strong_stocks,
            new_high_stocks=new_high_stocks,
            prediction_confidence=confidence,
            confidence_reasons=reasons
        )
    
    def _calculate_momentum_acceleration(self, stock_data: Dict) -> float:
        """
        计算动量加速度（核心领先指标）
        公式：(短期涨幅 - 长期涨幅) / 长期涨幅
        正值表示加速上涨，负值表示减速
        """
        short_returns = []  # 3日涨幅
        long_returns = []   # 10日涨幅
        
        for code, data in stock_data.items():
            if len(data) < 10:
                continue
            
            # 3日涨幅
            short_ret = (data[-1]['close'] / data[-3]['close'] - 1) * 100 if len(data) >= 3 else 0
            # 10日涨幅
            long_ret = (data[-1]['close'] / data[-10]['close'] - 1) * 100 if len(data) >= 10 else 0
            
            short_returns.append(short_ret)
            long_returns.append(long_ret)
        
        if not short_returns or not long_returns:
            return 50  # 默认中性
        
        avg_short = np.mean(short_returns)
        avg_long = np.mean(long_returns)
        
        # 计算加速度
        if abs(avg_long) < 0.1:
            acceleration = 0
        else:
            acceleration = (avg_short - avg_long) / (abs(avg_long) + 1e-8)
        
        # 归一化到 0-100
        # 加速度 > 0.5 视为极强，< -0.5 视为极弱
        score = 50 + acceleration * 50
        return np.clip(score, 0, 100)
    
    def _calculate_internal_strength(self, stock_data: Dict, days=20) -> float:
        """
        计算板块内部强度（关键领先指标）
        公式：创N日新高股数 / (创新低股数 + 1)
        比值越大，内部越强
        """
        high_break_count = 0
        low_break_count = 0
        
        for code, data in stock_data.items():
            if len(data) < days:
                continue
            
            current_close = data[-1]['close']
            
            # 检查是否创N日新高
            highest = max(d['high'] for d in data[-days:])
            if abs(current_close - highest) / highest < 0.01:  # 接近新高
                high_break_count += 1
            
            # 检查是否创N日新低
            lowest = min(d['low'] for d in data[-days:])
            if abs(current_close - lowest) / lowest < 0.01:  # 接近新低
                low_break_count += 1
        
        # 计算比值
        ratio = high_break_count / (low_break_count + 1)
        
        # 归一化到 0-100
        # 比值 > 10 视为极强，< 0.1 视为极弱
        if ratio > 10:
            score = 95
        elif ratio > 5:
            score = 85
        elif ratio > 2:
            score = 70
        elif ratio > 1:
            score = 60
        elif ratio > 0.5:
            score = 45
        else:
            score = 30
        
        return score
    
    def _calculate_capital_divergence(self, stock_data: Dict) -> float:
        """
        计算资金背离度（预警指标）
        公式：价格涨幅 vs 资金流入的背离程度
        正背离（价涨资金增）= 看涨，负背离（价涨资金减）= 预警
        """
        price_changes = []
        capital_flows = []
        
        for code, data in stock_data.items():
            if len(data) < 5:
                continue
            
            # 5日价格涨幅
            price_change = (data[-1]['close'] / data[-5]['close'] - 1) * 100
            price_changes.append(price_change)
            
            # 5日资金流向（从analyzer获取，这里用成交量变化近似）
            volume_change = (np.mean([d['volume'] for d in data[-3:]]) / 
                           np.mean([d['volume'] for d in data[-8:-5]]) - 1) * 100
            capital_flows.append(volume_change)
        
        if not price_changes or not capital_flows:
            return 50
        
        avg_price = np.mean(price_changes)
        avg_capital = np.mean(capital_flows)
        
        # 计算一致性
        # 价涨资金增 = 正背离 = 高分
        # 价涨资金减 = 负背离 = 低分
        if avg_price > 0 and avg_capital > 0:
            score = 70 + min(avg_capital, 30)  # 价涨资金增
        elif avg_price > 0 and avg_capital < 0:
            score = 40 - min(abs(avg_capital), 30)  # 价涨资金减（预警）
        elif avg_price < 0 and avg_capital > 0:
            score = 60  # 价跌资金增（抄底信号）
        else:
            score = 30  # 价跌资金减
        
        return np.clip(score, 0, 100)
    
    def _calculate_sentiment_extreme(self, stock_data: Dict) -> float:
        """
        计算情绪极点（反转指标）
        极度乐观 → 可能见顶
        极度悲观 → 可能见底
        """
        # 这里使用涨停/跌停板数量作为情绪极端的代理指标
        limit_up_count = 0
        limit_down_count = 0
        total_count = 0
        
        for code, data in stock_data.items():
            if not data:
                continue
            
            total_count += 1
            latest = data[-1]
            
            # 检查是否涨停（简化判断：涨幅 > 9.5%）
            if latest.get('change_pct', 0) > 9.5:
                limit_up_count += 1
            # 检查是否跌停
            elif latest.get('change_pct', 0) < -9.5:
                limit_down_count += 1
        
        if total_count == 0:
            return 50
        
        # 涨停比例
        limit_up_ratio = limit_up_count / total_count
        limit_down_ratio = limit_down_count / total_count
        
        # 适度乐观（10-20%涨停股）= 高分
        # 极度乐观（>30%涨停股）= 预警
        # 极度悲观（>20%跌停股）= 反转机会
        if 0.1 <= limit_up_ratio <= 0.2:
            score = 75  # 适度乐观
        elif limit_up_ratio > 0.3:
            score = 45  # 过度乐观，预警
        elif limit_down_ratio > 0.2:
            score = 60  # 极度悲观，反转机会
        else:
            score = 50
        
        return score
    
    def _calculate_trend_inertia(self, stock_data: Dict) -> float:
        """
        计算趋势惯性（辅助指标，权重降低）
        使用短期均线趋势
        """
        uptrend_count = 0
        total_count = 0
        
        for code, data in stock_data.items():
            if len(data) < 10:
                continue
            
            total_count += 1
            
            # 计算5日均线
            ma5 = np.mean([d['close'] for d in data[-5:]])
            ma10 = np.mean([d['close'] for d in data[-10:]])
            
            if ma5 > ma10:
                uptrend_count += 1
        
        if total_count == 0:
            return 50
        
        uptrend_ratio = uptrend_count / total_count
        return uptrend_ratio * 100
    
    def _calculate_current_strength(self, stock_data: Dict) -> float:
        """计算当前强度（传统方法，用于对比）"""
        # 简化实现：平均涨幅
        returns = []
        for code, data in stock_data.items():
            if len(data) < 5:
                continue
            ret = (data[-1]['close'] / data[-5]['close'] - 1) * 100
            returns.append(ret)
        
        if not returns:
            return 50
        
        avg_return = np.mean(returns)
        # 5日涨幅 > 10% = 90分，< -10% = 10分
        score = 50 + avg_return * 4
        return np.clip(score, 0, 100)
    
    def _determine_prediction_status(self, pred_score, curr_score, 
                                    momentum_accel, internal_strength) -> MainlineStatus:
        """判断预测状态"""
        # 预测分 > 当前分 + 10 且加速度强 = 预测强势
        if pred_score > curr_score + 10 and momentum_accel > 70:
            return MainlineStatus.STRONG_PREDICTION
        # 预测分 > 70 且内部强度高 = 新兴信号
        elif pred_score > 70 and internal_strength > 70:
            return MainlineStatus.EMERGING_SIGNAL
        # 预测分 < 当前分 - 10 = 即将转弱
        elif pred_score < curr_score - 10:
            return MainlineStatus.WEAKENING
        else:
            return MainlineStatus.OBSERVATION
    
    def _evaluate_prediction_confidence(self, pred_score, momentum_accel, 
                                       internal_strength, capital_divergence,
                                       stock_data) -> tuple:
        """
        评估预测置信度
        返回：(置信度等级, 原因列表)
        """
        reasons = []
        
        # 高置信度条件
        if pred_score > 80:
            reasons.append("预测强度分断层领先")
        
        if momentum_accel > 75:
            reasons.append("动量加速度极强")
        
        if internal_strength > 80:
            reasons.append("内部强度极高（多股创新高）")
        
        if capital_divergence > 70:
            reasons.append("资金情绪共振")
        
        # 低置信度条件
        if pred_score < 65:
            reasons.append("预测强度分不突出")
        
        if momentum_accel < 45:
            reasons.append("动量衰减")
        
        if capital_divergence < 40:
            reasons.append("资金背离（价涨资金减）")
        
        # 综合判断
        if len([r for r in reasons if "极强" in r or "极高" in r or "断层" in r]) >= 2:
            confidence = "高"
        elif len([r for r in reasons if "衰减" in r or "背离" in r or "不突出" in r]) >= 2:
            confidence = "低"
        else:
            confidence = "中"
        
        return confidence, reasons
    
    # ===== 辅助方法 =====
    
    def _get_all_industries(self) -> List[str]:
        """获取所有行业列表"""
        # BaoStock的行业分类不理想，直接使用主流板块概念
        return [
            "银行", "证券", "保险", "房地产", "建筑材料",
            "钢铁", "有色金属", "煤炭", "石油石化", "电力及公用事业",
            "家用电器", "食品饮料", "纺织服装", "轻工制造", "医药生物",
            "化工", "电子", "汽车", "机械设备", "国防军工",
            "计算机", "传媒", "通信", "非银金融", "综合"
        ]
    
    def _get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股"""
        # 使用知名个股代表各行业
        industry_stocks = {
            "银行": ["sh.600000", "sh.600036", "sh.601398", "sh.601939", "sz.000001"],
            "证券": ["sh.600030", "sh.600999", "sh.601688", "sz.000166", "sz.002736"],
            "保险": ["sh.601318", "sh.601601", "sh.601336"],
            "房地产": ["sz.000002", "sh.600048", "sh.600383", "sz.001979"],
            "食品饮料": ["sh.600519", "sz.000858", "sh.600887", "sz.000568"],
            "医药生物": ["sz.300760", "sh.600276", "sz.000661", "sz.300015"],
            "电子": ["sz.002415", "sz.000725", "sz.002475"],
            "汽车": ["sh.600104", "sz.000625", "sz.002594"],
            "家用电器": ["sz.000333", "sz.000651", "sh.600690"],
            "计算机": ["sz.002410", "sz.300059", "sh.600588"],
            "通信": ["sh.600050", "sz.000063", "sh.600941"],
            "传媒": ["sz.300251", "sz.002739", "sh.600637"],
            "化工": ["sz.002466", "sh.600309", "sz.000792"],
            "机械设备": ["sz.300124", "sh.601766", "sz.000157"],
            "国防军工": ["sh.600893", "sh.600118", "sz.002179"],
            "有色金属": ["sh.601899", "sz.000878", "sh.600362"],
            "钢铁": ["sh.600019", "sh.600010", "sz.000898"],
            "煤炭": ["sh.601088", "sh.601898", "sh.600188"],
            "石油石化": ["sh.601857", "sh.600028", "sh.601808"],
            "电力及公用事业": ["sh.600886", "sh.600900", "sz.000027"],
            "建筑材料": ["sh.600585", "sh.601636", "sz.000877"],
            "轻工制造": ["sh.603816", "sz.002032", "sh.603589"],
            "纺织服装": ["sz.002563", "sh.603288", "sz.002832"],
            "非银金融": ["sh.601628", "sz.002142", "sh.601066"],
            "综合": ["sh.600628", "sz.000690", "sh.600697"]
        }
        
        return industry_stocks.get(industry, [])
    
    def _batch_get_stock_data(self, stocks: List[str], days=30) -> Dict:
        """批量获取股票数据"""
        stock_data = {}
        
        # 直接使用 BaoStock 获取真实数据
        import baostock as bs
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days+10)).strftime('%Y-%m-%d')
        
        for stock_code in stocks:
            try:
                rs = bs.query_history_k_data_plus(
                    stock_code,
                    "date,code,open,high,low,close,volume,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2"
                )
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        row = rs.get_row_data()
                        if row and len(row) >= 8:
                            try:
                                data_list.append({
                                    'date': row[0],
                                    'open': float(row[2]) if row[2] else 0,
                                    'high': float(row[3]) if row[3] else 0,
                                    'low': float(row[4]) if row[4] else 0,
                                    'close': float(row[5]) if row[5] else 0,
                                    'volume': float(row[6]) if row[6] else 0,
                                    'amount': float(row[7]) if row[7] else 0
                                })
                            except (ValueError, IndexError):
                                continue
                    
                    if len(data_list) >= 10:  # 至少需要10天数据
                        stock_data[stock_code] = data_list
                        print(f"✅ {stock_code}: {len(data_list)}天数据")
                    else:
                        print(f"⚠️ {stock_code}: 数据不足({len(data_list)}天)")
                else:
                    print(f"❌ {stock_code}: API错误 {rs.error_msg}")
            except Exception as e:
                print(f"❌ {stock_code}: {str(e)}")
                continue
        
        print(f"📊 成功获取 {len(stock_data)}/{len(stocks)} 只股票的数据")
        return stock_data
    
    def _generate_mock_price_data(self, days: int) -> List[Dict]:
        """生成模拟价格数据"""
        data = []
        base_price = 10 + np.random.rand() * 90
        
        for i in range(days):
            change = np.random.randn() * 0.03  # 3%波动
            base_price *= (1 + change)
            
            data.append({
                'close': base_price,
                'high': base_price * 1.02,
                'low': base_price * 0.98,
                'volume': 1000000 * (1 + np.random.rand()),
                'change_pct': change * 100
            })
        
        return data
    
    def _count_strong_stocks(self, stock_data: Dict) -> int:
        """统计强势股数量（5日涨幅 > 5%）"""
        count = 0
        for code, data in stock_data.items():
            if len(data) >= 5:
                ret = (data[-1]['close'] / data[-5]['close'] - 1) * 100
                if ret > 5:
                    count += 1
        return count
    
    def _count_new_high_stocks(self, stock_data: Dict) -> int:
        """统计创新高股数量"""
        count = 0
        for code, data in stock_data.items():
            if len(data) >= 20:
                current = data[-1]['close']
                highest = max(d['high'] for d in data[-20:])
                if abs(current - highest) / highest < 0.01:
                    count += 1
        return count
    
    def _calculate_continuity_days(self, industry: str, date: str) -> int:
        """计算主线持续天数"""
        # 从历史记录中查找
        if industry not in self.history:
            return 1
        
        days = 1
        # 简化实现
        return min(days, 30)
    
    def _update_history(self, date: str, mainlines: List[PredictiveMainlineStrength]):
        """更新历史记录"""
        if date not in self.history:
            self.history[date] = {}
        
        for ml in mainlines:
            self.history[date][ml.industry_name] = ml.to_dict()
