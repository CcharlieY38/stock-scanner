"""
预测型龙头监控器 - 强化资金与情绪权重
增加板块地位加成
"""

import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class PredictiveLeaderStock:
    """预测型龙头股数据结构"""
    stock_code: str
    stock_name: str
    industry: str
    leader_type: str  # "综合龙头" / "资金龙头" / "潜在龙头"
    
    # 评分（提高资金和情绪权重）
    leader_score: float          # 龙头总分
    technical_score: float       # 技术分
    capital_score: float         # 资金分（权重提升）
    sentiment_score: float       # 情绪分（权重提升）
    
    rank: int
    
    # 资金流向详情
    capital_flow_3d: Dict        # 3日资金流向
    capital_flow_trend: str      # "持续流入" / "加速流入" / "流出"
    
    # 预测相关
    in_predictive_mainline: bool  # 是否属于预测主线前3
    mainline_rank: int            # 所属主线排名
    prediction_boost: float       # 预测加成系数
    
    def to_dict(self):
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'industry': self.industry,
            'leader_type': self.leader_type,
            'leader_score': round(self.leader_score, 2),
            'technical_score': round(self.technical_score, 2),
            'capital_score': round(self.capital_score, 2),
            'sentiment_score': round(self.sentiment_score, 2),
            'rank': self.rank,
            'capital_flow_3d': self.capital_flow_3d,
            'capital_flow_trend': self.capital_flow_trend,
            'in_predictive_mainline': self.in_predictive_mainline,
            'mainline_rank': self.mainline_rank,
            'prediction_boost': round(self.prediction_boost, 2)
        }


@dataclass
class EnhancedOversoldCandidate:
    """增强型超跌候选（带板块地位加成）"""
    stock_code: str
    stock_name: str
    industry: str
    
    base_oversold_coefficient: float      # 基础超跌系数
    adjusted_coefficient: float            # 调整后系数（含板块加成）
    
    drawdown_from_high: float              # 回撤幅度 (%)
    technical_divergence: bool             # 技术背离
    support_level: float                   # 支撑位
    
    current_price: float
    ma20_price: float
    ma50_price: float
    
    # 板块地位加成
    in_top3_mainline: bool                 # 是否属于预测主线前3
    mainline_rank: int                     # 主线排名
    position_boost: float                  # 板块地位加成系数 (1.0 - 1.2)
    
    # 反弹预期
    rebound_potential: str                 # "高" / "中" / "低"
    target_price: float                    # 目标反弹价位
    
    def to_dict(self):
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'industry': self.industry,
            'base_oversold_coefficient': round(self.base_oversold_coefficient, 2),
            'adjusted_coefficient': round(self.adjusted_coefficient, 2),
            'drawdown_from_high': round(self.drawdown_from_high, 2),
            'technical_divergence': self.technical_divergence,
            'support_level': round(self.support_level, 2),
            'current_price': round(self.current_price, 2),
            'in_top3_mainline': self.in_top3_mainline,
            'mainline_rank': self.mainline_rank,
            'position_boost': round(self.position_boost, 2),
            'rebound_potential': self.rebound_potential,
            'target_price': round(self.target_price, 2)
        }


class PredictiveLeaderMonitor:
    """预测型龙头监控器"""
    
    # 新评分权重（提高资金和情绪）
    LEADER_WEIGHTS = {
        'technical': 0.40,      # 技术分权重
        'capital': 0.35,        # 资金分权重（从0.2提升）
        'sentiment': 0.25       # 情绪分权重（从0.15提升）
    }
    
    # 板块地位加成配置
    MAINLINE_BOOST = {
        1: 1.20,  # 第1名主线：20%加成
        2: 1.15,  # 第2名主线：15%加成
        3: 1.10,  # 第3名主线：10%加成
    }
    
    def __init__(self, analyzer, config=None):
        self.analyzer = analyzer
        self.config = config or {}
        
        self.composite_leaders_top_n = config.get('composite_leaders_top_n', 20)  # 增加到20
        self.capital_leaders_top_n = config.get('capital_leaders_top_n', 10)     # 增加到10
        self.min_leader_score = config.get('min_leader_score', 50)  # 从70降低到50
    
    def build_predictive_leader_pool(self, predictive_mainlines: List) -> List[PredictiveLeaderStock]:
        """
        构建预测型龙头股池
        
        Args:
            predictive_mainlines: 预测主线列表
        
        Returns:
            龙头股列表
        """
        print("👑 开始构建预测型龙头股池...")
        
        all_leaders = []
        
        # 1. 从每个主线中筛选龙头
        for mainline in predictive_mainlines[:5]:  # 前5个主线
            industry_leaders = self._select_industry_leaders(
                mainline.industry_name, 
                mainline.rank
            )
            all_leaders.extend(industry_leaders)
        
        # 2. 去重并排序
        unique_leaders = self._deduplicate_leaders(all_leaders)
        unique_leaders.sort(key=lambda x: x.leader_score, reverse=True)
        
        # 3. 分配最终排名
        for i, leader in enumerate(unique_leaders[:self.composite_leaders_top_n], 1):
            leader.rank = i
        
        return unique_leaders[:self.composite_leaders_top_n]
    
    def _select_industry_leaders(self, industry: str, mainline_rank: int) -> List[PredictiveLeaderStock]:
        """从单个行业选择龙头"""
        # 获取行业成分股
        stocks = self._get_industry_stocks(industry)
        if not stocks:
            print(f"⚠️ 行业 {industry} 没有成分股")
            return []
        
        print(f"📊 分析行业: {industry} ({len(stocks)}只股票)")
        leaders = []
        success_count = 0
        
        for stock_code in stocks:  # 分析所有股票
            try:
                # 获取股票分析数据（调用现有analyzer）
                analysis = self._get_stock_analysis(stock_code)
                if not analysis:
                    print(f"  ⚠️ {stock_code}: 无分析数据")
                    continue
                
                success_count += 1
                
                # 计算龙头评分（新权重）
                leader_score = self._calculate_leader_score(
                    analysis['technical_score'],
                    analysis['capital_score'],
                    analysis['sentiment_score']
                )
                
                # 判断是否在预测主线前3
                in_top3 = mainline_rank <= 3
                
                # 应用预测加成
                if in_top3:
                    boost = self.MAINLINE_BOOST.get(mainline_rank, 1.0)
                    boosted_score = leader_score * boost
                else:
                    boost = 1.0
                    boosted_score = leader_score
                
                print(f"  📈 {stock_code} ({analysis.get('name', '?')}): "
                      f"基础={leader_score:.1f}, 加成后={boosted_score:.1f}, "
                      f"阈值={self.min_leader_score}")
                
                # 降低筛选阈值：评分 >= 50 即可
                if boosted_score >= 50:  # 从70降低到50
                    # 判断龙头类型
                    leader_type = self._determine_leader_type(
                        analysis, mainline_rank
                    )
                    
                    leader = PredictiveLeaderStock(
                        stock_code=stock_code,
                        stock_name=analysis.get('name', stock_code),
                        industry=industry,
                        leader_type=leader_type,
                        leader_score=boosted_score,  # 使用加成后的分数
                        technical_score=analysis['technical_score'],
                        capital_score=analysis['capital_score'],
                        sentiment_score=analysis['sentiment_score'],
                        rank=0,
                        capital_flow_3d=analysis.get('capital_flow_3d', {}),
                        capital_flow_trend=self._analyze_capital_trend(
                            analysis.get('capital_flow_3d', {})
                        ),
                        in_predictive_mainline=in_top3,
                        mainline_rank=mainline_rank,
                        prediction_boost=boost
                    )
                    leaders.append(leader)
                    print(f"  ✅ {stock_code} 入选龙头池 ({leader_type})")
                else:
                    print(f"  ❌ {stock_code} 分数不足")
            
            except Exception as e:
                print(f"  ❌ {stock_code}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"✅ {industry}: 成功分析 {success_count}/{len(stocks)}, 入选 {len(leaders)} 只龙头\n")
        return leaders
    
    def _calculate_leader_score(self, tech_score: float, capital_score: float, 
                               sentiment_score: float) -> float:
        """
        计算龙头评分（新权重公式）
        提高资金和情绪权重
        """
        score = (
            self.LEADER_WEIGHTS['technical'] * tech_score +
            self.LEADER_WEIGHTS['capital'] * capital_score +
            self.LEADER_WEIGHTS['sentiment'] * sentiment_score
        )
        return score
    
    def _determine_leader_type(self, analysis: Dict, mainline_rank: int) -> str:
        """判断龙头类型"""
        capital_score = analysis.get('capital_score', 0)
        leader_score = self._calculate_leader_score(
            analysis['technical_score'],
            capital_score,
            analysis['sentiment_score']
        )
        
        # 资金龙头：资金分 > 85 且 3日主力净流入 > 2亿
        if capital_score > 85 and analysis.get('capital_flow_3d', {}).get('main_force_net', 0) > 2:
            return "资金龙头"
        # 综合龙头：综合评分 > 88 且在预测主线前3
        elif leader_score > 88 and mainline_rank <= 3:
            return "综合龙头"
        # 潜在龙头
        else:
            return "潜在龙头"
    
    def _analyze_capital_trend(self, capital_flow_3d: Dict) -> str:
        """分析资金流向趋势"""
        if not capital_flow_3d:
            return "未知"
        
        main_force_net = capital_flow_3d.get('main_force_net', 0)
        
        # 简化判断（实际应该看连续多日）
        if main_force_net > 3:
            return "加速流入"
        elif main_force_net > 0.5:
            return "持续流入"
        elif main_force_net < -1:
            return "流出"
        else:
            return "平衡"
    
    def identify_enhanced_oversold_candidates(self, leader_pool: List[PredictiveLeaderStock],
                                             predictive_mainlines: List) -> List[EnhancedOversoldCandidate]:
        """
        识别增强型超跌候选（带板块地位加成）
        """
        print("🎣 开始识别增强型超跌候选...")
        
        oversold_candidates = []
        checked_stocks = set()
        
        # 1. 从龙头池中寻找超跌
        print("  从龙头池中寻找...")
        for leader in leader_pool:
            if leader.stock_code in checked_stocks:
                continue
            checked_stocks.add(leader.stock_code)
            
            candidate = self._check_oversold_stock(
                leader.stock_code, 
                leader.stock_name,
                leader.industry,
                leader.mainline_rank
            )
            if candidate:
                oversold_candidates.append(candidate)
                print(f"  ✅ {leader.stock_code} ({leader.stock_name}): "
                      f"系数={candidate.adjusted_coefficient:.2f}, 回撤={candidate.drawdown_from_high:.1f}%")
        
        # 2. 从预测主线前5的成分股中寻找（扩大范围）
        print("  从主线成分股中寻找...")
        for mainline in predictive_mainlines[:5]:
            stocks = self._get_industry_stocks(mainline.industry_name)
            for stock_code in stocks:
                if stock_code in checked_stocks:
                    continue
                checked_stocks.add(stock_code)
                
                # 获取股票名称
                analysis = self._get_stock_analysis(stock_code)
                stock_name = analysis.get('name', stock_code) if analysis else stock_code
                
                candidate = self._check_oversold_stock(
                    stock_code,
                    stock_name,
                    mainline.industry_name,
                    mainline.rank
                )
                if candidate:
                    oversold_candidates.append(candidate)
                    print(f"  ✅ {stock_code} ({stock_name}): "
                          f"系数={candidate.adjusted_coefficient:.2f}, 回撤={candidate.drawdown_from_high:.1f}%")
        
        # 排序并返回
        oversold_candidates.sort(key=lambda x: x.adjusted_coefficient, reverse=True)
        print(f"✅ 共找到 {len(oversold_candidates)} 只超跌候选\n")
        return oversold_candidates[:20]  # 返回前20只
    
    def _check_oversold_stock(self, stock_code: str, stock_name: str, 
                             industry: str, mainline_rank: int) -> Optional[EnhancedOversoldCandidate]:
        """检查单只股票是否超跌"""
        try:
            # 获取价格数据
            price_data = self._get_price_data(stock_code)
            if not price_data or len(price_data) < 20:
                return None
            
            # 计算基础超跌系数
            base_coefficient = self._calculate_base_oversold_coefficient(price_data)
            
            # 降低超跌阈值：0.4 即可（原来是0.6）
            if base_coefficient < 0.4:
                return None
            
            # 获取主线排名
            in_top3 = mainline_rank <= 3
            
            # 应用板块地位加成
            if in_top3:
                position_boost = self.MAINLINE_BOOST.get(mainline_rank, 1.0)
            else:
                position_boost = 1.0
            
            adjusted_coefficient = base_coefficient * position_boost
            
            # 计算其他指标
            drawdown = self._calculate_drawdown(price_data)
            divergence = self._detect_technical_divergence(price_data)
            support = self._calculate_support_level(price_data)
            
            current_price = price_data[-1]['close']
            ma20 = np.mean([d['close'] for d in price_data[-20:]])
            ma50 = np.mean([d['close'] for d in price_data[-50:]]) if len(price_data) >= 50 else ma20
            
            # 评估反弹潜力
            rebound_potential = self._evaluate_rebound_potential(
                adjusted_coefficient, divergence, in_top3
            )
            
            # 计算目标价位
            target_price = support * 1.1  # 支撑位上方10%
            
            return EnhancedOversoldCandidate(
                stock_code=stock_code,
                stock_name=stock_name,
                industry=industry,
                base_oversold_coefficient=base_coefficient,
                adjusted_coefficient=adjusted_coefficient,
                drawdown_from_high=drawdown,
                technical_divergence=divergence,
                support_level=support,
                current_price=current_price,
                ma20_price=ma20,
                ma50_price=ma50,
                in_top3_mainline=in_top3,
                mainline_rank=mainline_rank,
                position_boost=position_boost,
                rebound_potential=rebound_potential,
                target_price=target_price
            )
        
        except Exception as e:
            print(f"  ⚠️ 检查 {stock_code} 超跌失败: {e}")
            return None
    
    def _calculate_base_oversold_coefficient(self, price_data: List[Dict]) -> float:
        """
        计算基础超跌系数
        公式：0.6 × 回撤幅度 + 0.4 × 技术背离分
        """
        drawdown = self._calculate_drawdown(price_data)
        divergence = self._detect_technical_divergence(price_data)
        
        # 回撤分数（回撤越大分数越高）
        drawdown_score = min(drawdown / 30, 1.0)  # 30%回撤 = 满分
        
        # 背离分数
        divergence_score = 1.0 if divergence else 0.3
        
        coefficient = 0.6 * drawdown_score + 0.4 * divergence_score
        return coefficient
    
    def _calculate_drawdown(self, price_data: List[Dict]) -> float:
        """计算回撤幅度"""
        if len(price_data) < 20:
            return 0
        
        high_20d = max(d['high'] for d in price_data[-20:])
        current_price = price_data[-1]['close']
        
        drawdown = (high_20d - current_price) / high_20d * 100
        return max(drawdown, 0)
    
    def _detect_technical_divergence(self, price_data: List[Dict]) -> bool:
        """检测技术背离（价格新低但指标未新低）"""
        if len(price_data) < 20:
            return False
        
        # 简化实现：检查价格和成交量背离
        recent_prices = [d['close'] for d in price_data[-10:]]
        early_prices = [d['close'] for d in price_data[-20:-10]]
        
        recent_volumes = [d['volume'] for d in price_data[-10:]]
        early_volumes = [d['volume'] for d in price_data[-20:-10]]
        
        # 价格创新低
        price_new_low = min(recent_prices) < min(early_prices)
        
        # 成交量未创新低（放量）
        volume_not_low = min(recent_volumes) > min(early_volumes) * 0.8
        
        return price_new_low and volume_not_low
    
    def _calculate_support_level(self, price_data: List[Dict]) -> float:
        """计算支撑位"""
        if len(price_data) < 20:
            return price_data[-1]['close']
        
        # 使用MA20作为支撑
        ma20 = np.mean([d['close'] for d in price_data[-20:]])
        return ma20
    
    def _evaluate_rebound_potential(self, coefficient: float, 
                                   divergence: bool, in_top3: bool) -> str:
        """评估反弹潜力"""
        score = 0
        
        if coefficient > 0.8:
            score += 3
        elif coefficient > 0.7:
            score += 2
        elif coefficient > 0.6:
            score += 1
        
        if divergence:
            score += 2
        
        if in_top3:
            score += 2
        
        if score >= 6:
            return "高"
        elif score >= 4:
            return "中"
        else:
            return "低"
    
    # ===== 辅助方法 =====
    
    def _get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股"""
        # 使用与主线引擎相同的行业-股票映射
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
    
    def _get_stock_analysis(self, stock_code: str) -> Optional[Dict]:
        """获取股票分析数据（直接使用BaoStock，不依赖analyzer）"""
        try:
            import baostock as bs
            from datetime import datetime, timedelta
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,close,volume,peTTM,pbMRQ",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )
            
            if rs.error_code != '0':
                return None
            
            data_list = []
            while rs.next():
                row = rs.get_row_data()
                if row and len(row) >= 4:
                    try:
                        data_list.append({
                            'close': float(row[2]) if row[2] else 0,
                            'volume': float(row[3]) if row[3] else 0
                        })
                    except:
                        continue
            
            if len(data_list) < 10:
                return None
            
            # 简单技术评分
            recent_return = (data_list[-1]['close'] / data_list[-10]['close'] - 1) * 100
            tech_score = 50 + min(recent_return * 2, 40) if recent_return > 0 else 50 + max(recent_return * 2, -40)
            
            # 简单资金评分
            recent_vol = np.mean([d['volume'] for d in data_list[-5:]])
            early_vol = np.mean([d['volume'] for d in data_list[-20:-15]])
            vol_ratio = recent_vol / early_vol if early_vol > 0 else 1
            capital_score = 50 + min((vol_ratio - 1) * 50, 40)
            
            # 获取股票名称
            rs_stock = bs.query_stock_basic(code=stock_code)
            stock_name = stock_code
            if rs_stock.error_code == '0' and rs_stock.next():
                row_data = rs_stock.get_row_data()
                stock_name = row_data[1] if len(row_data) > 1 else stock_code
            
            return {
                'name': stock_name,
                'technical_score': tech_score,
                'capital_score': capital_score,
                'sentiment_score': 70,  # 默认中性
                'capital_flow_3d': {
                    'main_force_net': (vol_ratio - 1) * 2,  # 估算
                    'status': '净流入' if vol_ratio > 1 else '净流出'
                }
            }
        
        except Exception as e:
            print(f"⚠️ 获取 {stock_code} 分析数据失败: {e}")
            return None
    
    def _get_price_data(self, stock_code: str) -> List[Dict]:
        """获取价格数据"""
        try:
            import baostock as bs
            from datetime import datetime, timedelta
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            
            rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,open,high,low,close,volume",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )
            
            if rs.error_code != '0':
                return []
            
            data_list = []
            while rs.next():
                row = rs.get_row_data()
                if row and len(row) >= 7:
                    try:
                        data_list.append({
                            'close': float(row[5]) if row[5] else 0,
                            'high': float(row[3]) if row[3] else 0,
                            'low': float(row[4]) if row[4] else 0,
                            'volume': float(row[6]) if row[6] else 0
                        })
                    except:
                        continue
            
            return data_list
        
        except Exception as e:
            print(f"⚠️ 获取 {stock_code} 价格数据失败: {e}")
            return []
    
    def _deduplicate_leaders(self, leaders: List[PredictiveLeaderStock]) -> List[PredictiveLeaderStock]:
        """去重龙头股"""
        seen = set()
        unique = []
        
        for leader in leaders:
            if leader.stock_code not in seen:
                seen.add(leader.stock_code)
                unique.append(leader)
        
        return unique
