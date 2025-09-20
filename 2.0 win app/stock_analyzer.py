"""
增强版现代股票分析系统
支持25项财务指标、详细新闻分析、技术分析、情绪分析和AI增强分析
"""

import os
import sys
import logging
import warnings
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import re

# 忽略警告
warnings.filterwarnings('ignore')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EnhancedStockAnalyzer:
    """增强版综合股票分析器"""
    
    def __init__(self, config_file='config.json', weights: Optional[Dict[str, float]] = None, thresholds: Optional[Dict[str, float]] = None):
        """初始化分析器"""
        self.logger = logging.getLogger(__name__)
        self.config_file = config_file
        
        # 加载配置文件
        self.config = self._load_config()
        
        # 缓存配置
        cache_config = self.config.get('cache', {})
        self.cache_duration = timedelta(hours=cache_config.get('price_hours', 1))
        self.fundamental_cache_duration = timedelta(hours=cache_config.get('fundamental_hours', 6))
        self.news_cache_duration = timedelta(hours=cache_config.get('news_hours', 2))
        
        self.price_cache = {}
        self.fundamental_cache = {}
        self.news_cache = {}
        
        # 分析权重配置（允许通过构造函数覆盖）
        cfg_weights = self.config.get('analysis_weights', {})
        merged_weights = {
            'technical': cfg_weights.get('technical', 0.4),
            'fundamental': cfg_weights.get('fundamental', 0.4),
            'sentiment': cfg_weights.get('sentiment', 0.2)
        }
        if isinstance(weights, dict) and weights:
            merged_weights.update({k: float(v) for k, v in weights.items() if k in merged_weights})
        self.analysis_weights = merged_weights
        # 兼容：同时提供 self.weights 引用，便于外部或后续逻辑统一读取
        self.weights = merged_weights

        # 技术阈值配置（允许通过构造函数覆盖）
        default_thresholds = {
            'rsi_overbought': 70.0,
            'rsi_oversold': 30.0,
            'rsi_neutral_low': 45.0,
            'rsi_neutral_high': 65.0,
            'bb_lower': 0.2,
            'bb_upper': 0.8,
            'volume_ratio_low': 0.6,
            'volume_ratio_high': 1.5,
            'volume_ratio_very_low': 0.7,
            'volume_ratio_very_high': 2.0,
            'atr_high_risk': 6.0,
            'atr_low_vol': 2.0,
            'confirm_days': 1
        }
        cfg_thresholds = self.config.get('technical_thresholds', {})
        default_thresholds.update({k: cfg_thresholds.get(k, v) for k, v in default_thresholds.items()})
        if isinstance(thresholds, dict) and thresholds:
            default_thresholds.update({k: thresholds.get(k, v) for k, v in default_thresholds.items()})
        self.thresholds = default_thresholds
        
        # 流式推理配置
        streaming = self.config.get('streaming', {})
        self.streaming_config = {
            'enabled': streaming.get('enabled', True),
            'show_thinking': streaming.get('show_thinking', True),
            'delay': streaming.get('delay', 0.1)
        }
        
        # AI配置
        ai_config = self.config.get('ai', {})
        self.ai_config = {
            'max_tokens': ai_config.get('max_tokens', 4000),
            'temperature': ai_config.get('temperature', 0.7),
            'model_preference': ai_config.get('model_preference', 'openai')
        }
        
        # 分析参数配置
        params = self.config.get('analysis_params', {})
        self.analysis_params = {
            'max_news_count': params.get('max_news_count', 200),
            'technical_period_days': params.get('technical_period_days', 365),
            'financial_indicators_count': params.get('financial_indicators_count', 25)
        }
        
        # API密钥配置
        self.api_keys = self.config.get('api_keys', {})
        
        self.logger.info("增强版股票分析器初始化完成")
        self._log_config_status()

    def _load_config(self):
        """加载JSON配置文件"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.logger.info(f"✅ 成功加载配置文件: {self.config_file}")
                return config
            else:
                self.logger.warning(f"⚠️ 配置文件 {self.config_file} 不存在，使用默认配置")
                default_config = self._get_default_config()
                self._save_config(default_config)
                return default_config
                
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 配置文件格式错误: {e}")
            self.logger.info("使用默认配置并备份错误文件")
            
            if os.path.exists(self.config_file):
                backup_name = f"{self.config_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(self.config_file, backup_name)
                self.logger.info(f"错误配置文件已备份为: {backup_name}")
            
            default_config = self._get_default_config()
            self._save_config(default_config)
            return default_config
            
        except Exception as e:
            self.logger.error(f"❌ 加载配置文件失败: {e}")
            return self._get_default_config()

    def _get_default_config(self):
        """获取默认配置"""
        return {
            "api_keys": {
                "openai": "",
                "anthropic": "",
                "zhipu": "",
                "notes": "请填入您的API密钥"
            },
            "ai": {
                "model_preference": "openai",
                "models": {
                    "openai": "gpt-4o-mini",
                    "anthropic": "claude-3-haiku-20240307",
                    "zhipu": "chatglm_turbo"
                },
                "max_tokens": 6000,
                "temperature": 0.7,
                "api_base_urls": {
                    "openai": "https://api.openai.com/v1",
                    "notes": "如使用中转API，修改上述URL"
                }
            },
            "analysis_weights": {
                "technical": 0.4,
                "fundamental": 0.4,
                "sentiment": 0.2,
                "notes": "权重总和应为1.0"
            },
            "technical_thresholds": {
                "rsi_overbought": 70.0,
                "rsi_oversold": 30.0,
                "rsi_neutral_low": 45.0,
                "rsi_neutral_high": 65.0,
                "bb_lower": 0.2,
                "bb_upper": 0.8,
                "volume_ratio_low": 0.6,
                "volume_ratio_high": 1.5,
                "volume_ratio_very_low": 0.7,
                "volume_ratio_very_high": 2.0,
                "atr_high_risk": 6.0,
                "atr_low_vol": 2.0,
                "confirm_days": 1,
                "notes": "可调技术面阈值与确认天数"
            },
            "cache": {
                "price_hours": 1,
                "fundamental_hours": 6,
                "news_hours": 2
            },
            "streaming": {
                "enabled": True,
                "show_thinking": True,
                "delay": 0.1
            },
            "analysis_params": {
                "max_news_count": 200,
                "technical_period_days": 365,
                "financial_indicators_count": 25
            },
            "logging": {
                "level": "INFO",
                "file": "stock_analyzer.log"
            },
            "data_sources": {
                "akshare_token": "",
                "backup_sources": ["akshare"]
            },
            "ui": {
                "theme": "default",
                "language": "zh_CN",
                "window_size": [1200, 800]
            },
            "_metadata": {
                "version": "3.0.0",
                "created": datetime.now().isoformat(),
                "description": "增强版AI股票分析系统配置文件"
            }
        }

    def _save_config(self, config):
        """保存配置到文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
            self.logger.info(f"✅ 配置文件已保存: {self.config_file}")
        except Exception as e:
            self.logger.error(f"❌ 保存配置文件失败: {e}")

    def _log_config_status(self):
        """记录配置状态"""
        self.logger.info("=== 增强版系统配置状态 ===")
        
        # 检查API密钥状态
        available_apis = []
        for api_name, api_key in self.api_keys.items():
            if api_name != 'notes' and api_key and api_key.strip():
                available_apis.append(api_name)
        
        if available_apis:
            self.logger.info(f"🤖 可用AI API: {', '.join(available_apis)}")
        else:
            self.logger.warning("⚠️ 未配置任何AI API密钥")
        
        self.logger.info(f"📊 财务指标数量: {self.analysis_params['financial_indicators_count']}")
        self.logger.info(f"📰 最大新闻数量: {self.analysis_params['max_news_count']}")
        self.logger.info("=" * 35)

    # =============================
    # 通用辅助函数（不改外部接口）
    # =============================
    def _winsorize(self, series: pd.Series, lower: float = 0.02, upper: float = 0.98) -> pd.Series:
        """分位数缩尾，提升稳健性"""
        try:
            if series is None or len(series) == 0:
                return series
            low = series.quantile(lower)
            up = series.quantile(upper)
            return series.clip(lower=low, upper=up)
        except Exception:
            return series

    def _safe_ratio(self, a: float, b: float, default: float = 0.0) -> float:
        """安全除法"""
        try:
            a = float(a)
            b = float(b)
            if b == 0 or np.isnan(b):
                return default
            val = a / b
            if not np.isfinite(val):
                return default
            return float(val)
        except Exception:
            return default

    def _normalize01(self, val: float, vmin: float, vmax: float, clip: bool = True) -> float:
        """将数值标准化到[0,1]"""
        try:
            if vmax == vmin:
                return 0.5
            x = (float(val) - vmin) / (vmax - vmin)
            if clip:
                x = max(0.0, min(1.0, x))
            return x
        except Exception:
            return 0.5

    def _time_decay_weights(self, dates: List, half_life_days: float = 30.0) -> List[float]:
        """时间衰减权重，越新的新闻权重越高，返回和为1的权重列表"""
        try:
            now = datetime.now()
            weights = []
            for d in dates:
                try:
                    dt = pd.to_datetime(d, errors='coerce')
                    if pd.isna(dt):
                        w = 0.7  # 无法解析日期时给一个中等权重
                    else:
                        days = max(0.0, (now - dt).days)
                        w = np.exp(-np.log(2) * days / max(1e-6, half_life_days))
                except Exception:
                    w = 0.7
                weights.append(float(w))
            s = sum(weights)
            return [w / s if s > 0 else 0.0 for w in weights]
        except Exception:
            return [1.0 / max(1, len(dates))] * max(1, len(dates))

    def _tokenize_cn(self, text: str) -> List[str]:
        """中文优先的轻量分词，优先使用jieba，不可用时回退到简单拆分"""
        try:
            import jieba  # 可选依赖
            tokens = [t.strip() for t in jieba.lcut(text) if t.strip()]
            return tokens
        except Exception:
            # 退化：按中英文数字片段与标点分隔
            if not isinstance(text, str):
                return []
            text = text.strip()
            if not text:
                return []
            # 将英文小写化，保留中文
            text = text.lower()
            # 以非字母数字和非中文字符切分
            parts = re.split(r"[^0-9a-zA-Z\u4e00-\u9fa5]+", text)
            return [p for p in parts if p]

    def _sentiment_from_tokens(self, tokens: List[str], positive_words: set, negative_words: set) -> float:
        """基于否定词和程度副词的简易情绪打分，返回[-1,1]"""
        if not tokens:
            return 0.0

        negations = {"不", "未", "无", "非", "否", "没", "沒有", "并非", "並非"}
        degree_map = {
            # 程度副词 放大因子
            "极其": 2.0, "非常": 1.8, "特别": 1.6, "显著": 1.5, "明显": 1.4,
            "较为": 1.2, "比较": 1.2, "一定": 1.1, "略": 0.9, "有点": 0.8
        }

        window = 3  # 近3词窗口内的否定和程度影响
        score = 0.0
        hits = 0

        for i, tok in enumerate(tokens):
            base = 0.0
            if tok in positive_words:
                base = 1.0
            elif tok in negative_words:
                base = -1.0
            else:
                continue

            # 回看窗口
            neg_flip = 1
            degree = 1.0
            for j in range(max(0, i - window), i):
                t = tokens[j]
                if t in negations:
                    neg_flip *= -1
                if t in degree_map:
                    degree *= degree_map[t]

            score += base * neg_flip * degree
            hits += 1

        if hits == 0:
            return 0.0
        # 归一化到[-1,1]，对极端程度做tanh收敛
        return float(np.tanh(score / max(1.0, hits)))

    def get_stock_data(self, stock_code, period='1y'):
        """获取股票价格数据"""
        if stock_code in self.price_cache:
            cache_time, data = self.price_cache[stock_code]
            if datetime.now() - cache_time < self.cache_duration:
                self.logger.info(f"使用缓存的价格数据: {stock_code}")
                return data

        try:
            import akshare as ak
            
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=self.analysis_params['technical_period_days'])).strftime('%Y%m%d')
            
            self.logger.info(f"正在获取 {stock_code} 的历史数据...")
            
            stock_data = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if stock_data.empty:
                raise ValueError(f"无法获取股票 {stock_code} 的数据")
            
            # 智能处理列名映射 - 修复版本
            try:
                actual_columns = len(stock_data.columns)
                self.logger.info(f"获取到 {actual_columns} 列数据，列名: {list(stock_data.columns)}")
                
                # 根据实际返回的列数进行映射
                if actual_columns == 13:  # 包含code列的完整格式
                    standard_columns = ['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'change_pct', 'change_amount', 'turnover_rate', 'extra']
                elif actual_columns == 12:  # 包含code列
                    standard_columns = ['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'change_pct', 'change_amount', 'turnover_rate']
                elif actual_columns == 11:  # 不包含code列的标准格式
                    standard_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'change_pct', 'change_amount', 'turnover_rate']
                elif actual_columns == 10:  # 简化格式
                    standard_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'change_pct', 'change_amount']
                else:
                    # 对于未知格式，尝试智能识别
                    standard_columns = [f'col_{i}' for i in range(actual_columns)]
                    self.logger.warning(f"未知的列数格式 ({actual_columns} 列)，使用通用列名")
                
                # 创建列名映射
                column_mapping = dict(zip(stock_data.columns, standard_columns))
                stock_data = stock_data.rename(columns=column_mapping)
                
                self.logger.info(f"列名映射完成: {column_mapping}")
                
            except Exception as e:
                self.logger.warning(f"列名标准化失败: {e}，保持原列名")
            
            # 确保必要的列存在并且映射正确
            required_columns = ['close', 'open', 'high', 'low', 'volume']
            missing_columns = []
            
            for col in required_columns:
                if col not in stock_data.columns:
                    # 尝试找到相似的列名
                    similar_cols = [c for c in stock_data.columns if col in c.lower() or c.lower() in col]
                    if similar_cols:
                        stock_data[col] = stock_data[similar_cols[0]]
                        self.logger.info(f"✓ 映射列 {similar_cols[0]} -> {col}")
                    else:
                        missing_columns.append(col)
            
            if missing_columns:
                self.logger.warning(f"缺少必要的列: {missing_columns}")
                # 如果缺少必要列，尝试使用位置索引映射
                if len(stock_data.columns) >= 6:  # 至少有6列才能进行位置映射
                    cols = list(stock_data.columns)
                    # 通常akshare的列顺序是: 日期, [代码], 开盘, 收盘, 最高, 最低, 成交量, ...
                    if 'code' in cols[1].lower() or len(cols[1]) == 6:  # 第二列是股票代码
                        position_mapping = {
                            cols[0]: 'date',
                            cols[1]: 'code', 
                            cols[2]: 'open',
                            cols[3]: 'close',  # 确保第4列是收盘价
                            cols[4]: 'high',
                            cols[5]: 'low'
                        }
                        if len(cols) > 6:
                            position_mapping[cols[6]] = 'volume'
                    else:  # 没有代码列
                        position_mapping = {
                            cols[0]: 'date',
                            cols[1]: 'open', 
                            cols[2]: 'close',  # 确保第3列是收盘价
                            cols[3]: 'high',
                            cols[4]: 'low'
                        }
                        if len(cols) > 5:
                            position_mapping[cols[5]] = 'volume'
                    
                    # 应用位置映射
                    stock_data = stock_data.rename(columns=position_mapping)
                    self.logger.info(f"✓ 应用位置映射: {position_mapping}")
            
            # 处理日期列
            try:
                if 'date' in stock_data.columns:
                    stock_data['date'] = pd.to_datetime(stock_data['date'])
                    stock_data = stock_data.set_index('date')
                else:
                    stock_data.index = pd.to_datetime(stock_data.index)
            except Exception as e:
                self.logger.warning(f"日期处理失败: {e}")
            
            # 确保数值列为数值类型
            numeric_columns = ['open', 'close', 'high', 'low', 'volume']
            for col in numeric_columns:
                if col in stock_data.columns:
                    try:
                        stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
                    except:
                        pass
            
            # 验证数据质量
            if 'close' in stock_data.columns:
                latest_close = stock_data['close'].iloc[-1]
                latest_open = stock_data['open'].iloc[-1] if 'open' in stock_data.columns else 0
                self.logger.info(f"✓ 数据验证 - 最新收盘价: {latest_close}, 最新开盘价: {latest_open}")
                
                # 检查收盘价是否合理
                if pd.isna(latest_close) or latest_close <= 0:
                    self.logger.error(f"❌ 收盘价数据异常: {latest_close}")
                    raise ValueError(f"股票 {stock_code} 的收盘价数据异常")
            
            # 缓存数据
            self.price_cache[stock_code] = (datetime.now(), stock_data)
            
            self.logger.info(f"✓ 成功获取 {stock_code} 的价格数据，共 {len(stock_data)} 条记录")
            self.logger.info(f"✓ 数据列: {list(stock_data.columns)}")
            
            return stock_data
            
        except Exception as e:
            self.logger.error(f"获取股票数据失败: {str(e)}")
            return pd.DataFrame()

    def get_comprehensive_fundamental_data(self, stock_code):
        """获取25项综合财务指标数据"""
        if stock_code in self.fundamental_cache:
            cache_time, data = self.fundamental_cache[stock_code]
            if datetime.now() - cache_time < self.fundamental_cache_duration:
                self.logger.info(f"使用缓存的基本面数据: {stock_code}")
                return data
        
        try:
            import akshare as ak
            
            fundamental_data = {}
            self.logger.info(f"开始获取 {stock_code} 的25项综合财务指标...")
            
            # 1. 基本信息
            try:
                self.logger.info("正在获取股票基本信息...")
                stock_info = ak.stock_individual_info_em(symbol=stock_code)
                info_dict = dict(zip(stock_info['item'], stock_info['value']))
                fundamental_data['basic_info'] = info_dict
                self.logger.info("✓ 股票基本信息获取成功")
            except Exception as e:
                self.logger.warning(f"获取基本信息失败: {e}")
                fundamental_data['basic_info'] = {}
            
            # 2. 详细财务指标 - 25项核心指标
            try:
                self.logger.info("正在获取25项详细财务指标...")
                financial_indicators = {}
                
                # 获取主要财务数据
                try:
                    # 利润表数据
                    income_statement = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
                    if not income_statement.empty:
                        latest_income = income_statement.iloc[0].to_dict()
                        financial_indicators.update(latest_income)
                except Exception as e:
                    self.logger.warning(f"获取利润表数据失败: {e}")
                
                # 获取财务分析指标
                try:
                    balance_sheet = ak.stock_financial_analysis_indicator(symbol=stock_code)
                    if not balance_sheet.empty:
                        latest_balance = balance_sheet.iloc[-1].to_dict()
                        financial_indicators.update(latest_balance)
                except Exception as e:
                    self.logger.warning(f"获取财务分析指标失败: {e}")
                
                # 获取现金流量表
                try:
                    cash_flow = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
                    if not cash_flow.empty:
                        latest_cash = cash_flow.iloc[-1].to_dict()
                        financial_indicators.update(latest_cash)
                except Exception as e:
                    self.logger.warning(f"获取现金流量表失败: {e}")
                
                # 计算25项核心财务指标
                core_indicators = self._calculate_core_financial_indicators(financial_indicators)
                fundamental_data['financial_indicators'] = core_indicators
                
                self.logger.info(f"✓ 获取到 {len(core_indicators)} 项财务指标")
                
            except Exception as e:
                self.logger.warning(f"获取财务指标失败: {e}")
                fundamental_data['financial_indicators'] = {}
            
            # 3. 估值指标
            try:
                self.logger.info("正在获取估值指标...")
                # 尝试使用替代的估值指标API
                try:
                    valuation_data = ak.stock_a_indicator_lg(symbol=stock_code)
                except AttributeError:
                    # 如果上述方法不存在，尝试其他方法
                    try:
                        valuation_data = ak.tool_trade_date_hist_sina()  # 使用可用的方法作为替代
                        valuation_data = pd.DataFrame()  # 设为空DataFrame
                    except:
                        valuation_data = pd.DataFrame()
                        
                if not valuation_data.empty:
                    latest_valuation = valuation_data.iloc[-1].to_dict()
                    fundamental_data['valuation'] = latest_valuation
                    self.logger.info("✓ 估值指标获取成功")
                else:
                    fundamental_data['valuation'] = {}
                    self.logger.info("ℹ️ 估值指标暂时不可用")
            except Exception as e:
                self.logger.warning(f"获取估值指标失败: {e}")
                fundamental_data['valuation'] = {}
            
            # 4. 业绩预告和业绩快报
            try:
                self.logger.info("正在获取业绩预告...")
                # 尝试使用正确的业绩预告API
                try:
                    performance_forecast = ak.stock_yjbb_em(stock=stock_code)  # 修正参数名
                except (AttributeError, TypeError):
                    # 如果方法不存在或参数错误，尝试其他方法
                    try:
                        performance_forecast = ak.stock_yjbb_em()  # 不传参数
                        if not performance_forecast.empty:
                            # 筛选出对应股票的数据
                            performance_forecast = performance_forecast[
                                performance_forecast.iloc[:, 0].astype(str).str.contains(stock_code, na=False)
                            ]
                    except:
                        performance_forecast = pd.DataFrame()
                        
                if not performance_forecast.empty:
                    fundamental_data['performance_forecast'] = performance_forecast.head(10).to_dict('records')
                    self.logger.info("✓ 业绩预告获取成功")
                else:
                    fundamental_data['performance_forecast'] = []
                    self.logger.info("ℹ️ 业绩预告暂时不可用")
            except Exception as e:
                self.logger.warning(f"获取业绩预告失败: {e}")
                fundamental_data['performance_forecast'] = []
            
            # 5. 分红配股信息
            try:
                self.logger.info("正在获取分红配股信息...")
                # 尝试使用正确的分红配股API
                try:
                    dividend_info = ak.stock_fhpg_em(symbol=stock_code)
                except AttributeError:
                    # 如果方法不存在，尝试其他方法
                    try:
                        dividend_info = ak.stock_fhpg_detail_em(symbol=stock_code)
                    except:
                        dividend_info = pd.DataFrame()
                        
                if not dividend_info.empty:
                    fundamental_data['dividend_info'] = dividend_info.head(10).to_dict('records')
                    self.logger.info("✓ 分红配股信息获取成功")
                else:
                    fundamental_data['dividend_info'] = []
                    self.logger.info("ℹ️ 分红配股信息暂时不可用")
            except Exception as e:
                self.logger.warning(f"获取分红配股信息失败: {e}")
                fundamental_data['dividend_info'] = []
            
            # 6. 行业分析
            try:
                self.logger.info("正在获取行业分析数据...")
                industry_analysis = self._get_industry_analysis(stock_code)
                fundamental_data['industry_analysis'] = industry_analysis
                self.logger.info("✓ 行业分析数据获取成功")
            except Exception as e:
                self.logger.warning(f"获取行业分析失败: {e}")
                fundamental_data['industry_analysis'] = {}
            
            # 7. 股东信息
            try:
                self.logger.info("正在获取股东信息...")
                shareholder_info = ak.stock_zh_a_gdhs(symbol=stock_code)
                if not shareholder_info.empty:
                    fundamental_data['shareholders'] = shareholder_info.head(20).to_dict('records')
                    self.logger.info("✓ 股东信息获取成功")
                else:
                    fundamental_data['shareholders'] = []
            except Exception as e:
                self.logger.warning(f"获取股东信息失败: {e}")
                fundamental_data['shareholders'] = []
            
            # 8. 机构持股
            try:
                self.logger.info("正在获取机构持股信息...")
                institutional_holdings = ak.stock_institutional_holding_detail(symbol=stock_code)
                if not institutional_holdings.empty:
                    fundamental_data['institutional_holdings'] = institutional_holdings.head(20).to_dict('records')
                    self.logger.info("✓ 机构持股信息获取成功")
                else:
                    fundamental_data['institutional_holdings'] = []
            except Exception as e:
                self.logger.warning(f"获取机构持股失败: {e}")
                fundamental_data['institutional_holdings'] = []
            
            # 缓存数据
            self.fundamental_cache[stock_code] = (datetime.now(), fundamental_data)
            self.logger.info(f"✓ {stock_code} 综合基本面数据获取完成并已缓存")
            
            return fundamental_data
            
        except Exception as e:
            self.logger.error(f"获取综合基本面数据失败: {str(e)}")
            return {
                'basic_info': {},
                'financial_indicators': {},
                'valuation': {},
                'performance_forecast': [],
                'dividend_info': [],
                'industry_analysis': {},
                'shareholders': [],
                'institutional_holdings': []
            }

    def _calculate_core_financial_indicators(self, raw_data):
        """计算25项核心财务指标"""
        try:
            indicators = {}
            
            # 从原始数据中安全获取数值
            def safe_get(key, default=0):
                value = raw_data.get(key, default)
                try:
                    return float(value) if value not in [None, '', 'nan'] else default
                except:
                    return default
            
            # 1-5: 盈利能力指标
            indicators['净利润率'] = safe_get('净利润率')
            indicators['净资产收益率'] = safe_get('净资产收益率')
            indicators['总资产收益率'] = safe_get('总资产收益率')
            indicators['毛利率'] = safe_get('毛利率')
            indicators['营业利润率'] = safe_get('营业利润率')
            
            # 6-10: 偿债能力指标
            indicators['流动比率'] = safe_get('流动比率')
            indicators['速动比率'] = safe_get('速动比率')
            indicators['资产负债率'] = safe_get('资产负债率')
            indicators['产权比率'] = safe_get('产权比率')
            indicators['利息保障倍数'] = safe_get('利息保障倍数')
            
            # 11-15: 营运能力指标
            indicators['总资产周转率'] = safe_get('总资产周转率')
            indicators['存货周转率'] = safe_get('存货周转率')
            indicators['应收账款周转率'] = safe_get('应收账款周转率')
            indicators['流动资产周转率'] = safe_get('流动资产周转率')
            indicators['固定资产周转率'] = safe_get('固定资产周转率')
            
            # 16-20: 发展能力指标
            indicators['营收同比增长率'] = safe_get('营收同比增长率')
            indicators['净利润同比增长率'] = safe_get('净利润同比增长率')
            indicators['总资产增长率'] = safe_get('总资产增长率')
            indicators['净资产增长率'] = safe_get('净资产增长率')
            indicators['经营现金流增长率'] = safe_get('经营现金流增长率')
            
            # 21-25: 市场表现指标
            indicators['市盈率'] = safe_get('市盈率')
            indicators['市净率'] = safe_get('市净率')
            indicators['市销率'] = safe_get('市销率')
            indicators['PEG比率'] = safe_get('PEG比率')
            indicators['股息收益率'] = safe_get('股息收益率')
            
            # 计算一些衍生指标
            try:
                # 如果有基础数据，计算一些关键比率
                revenue = safe_get('营业收入')
                net_income = safe_get('净利润')
                total_assets = safe_get('总资产')
                shareholders_equity = safe_get('股东权益')
                
                if revenue > 0 and net_income > 0:
                    if indicators['净利润率'] == 0:
                        indicators['净利润率'] = (net_income / revenue) * 100
                
                if total_assets > 0 and net_income > 0:
                    if indicators['总资产收益率'] == 0:
                        indicators['总资产收益率'] = (net_income / total_assets) * 100
                
                if shareholders_equity > 0 and net_income > 0:
                    if indicators['净资产收益率'] == 0:
                        indicators['净资产收益率'] = (net_income / shareholders_equity) * 100
                        
            except Exception as e:
                self.logger.warning(f"计算衍生指标失败: {e}")
            
            # 过滤掉无效的指标
            valid_indicators = {k: v for k, v in indicators.items() if v not in [0, None, 'nan']}
            
            self.logger.info(f"✓ 成功计算 {len(valid_indicators)} 项有效财务指标")
            return valid_indicators
            
        except Exception as e:
            self.logger.error(f"计算核心财务指标失败: {e}")
            return {}

    def _get_industry_analysis(self, stock_code):
        """获取行业分析数据"""
        try:
            import akshare as ak
            
            industry_data = {}
            
            # 获取行业信息
            try:
                industry_info = ak.stock_board_industry_name_em()
                stock_industry = industry_info[industry_info.iloc[:, 0].astype(str).str.contains(stock_code, na=False)]
                if not stock_industry.empty:
                    industry_data['industry_info'] = stock_industry.iloc[0].to_dict()
                else:
                    industry_data['industry_info'] = {}
            except Exception as e:
                self.logger.warning(f"获取行业信息失败: {e}")
                industry_data['industry_info'] = {}
            
            # 获取行业排名
            try:
                # 尝试使用正确的行业排名API
                try:
                    industry_rank = ak.stock_rank_em(symbol="行业排名")
                except AttributeError:
                    # 如果方法不存在，使用替代方法
                    try:
                        # 尝试获取行业板块数据
                        industry_rank = ak.stock_board_industry_name_em()
                        # 查找包含股票代码的行业信息
                        industry_rank = pd.DataFrame()  # 暂时设为空
                    except:
                        industry_rank = pd.DataFrame()
                        
                if not industry_rank.empty:
                    stock_rank = industry_rank[industry_rank.iloc[:, 1].astype(str).str.contains(stock_code, na=False)]
                    if not stock_rank.empty:
                        industry_data['industry_rank'] = stock_rank.iloc[0].to_dict()
                    else:
                        industry_data['industry_rank'] = {}
                else:
                    industry_data['industry_rank'] = {}
                    self.logger.info("ℹ️ 行业排名数据暂时不可用")
            except Exception as e:
                self.logger.warning(f"获取行业排名失败: {e}")
                industry_data['industry_rank'] = {}
            
            return industry_data
            
        except Exception as e:
            self.logger.warning(f"行业分析失败: {e}")
            return {}

    def get_comprehensive_news_data(self, stock_code, days=30):
        """获取综合新闻数据（大幅增强）"""
        cache_key = f"{stock_code}_{days}"
        if cache_key in self.news_cache:
            cache_time, data = self.news_cache[cache_key]
            if datetime.now() - cache_time < self.news_cache_duration:
                self.logger.info(f"使用缓存的新闻数据: {stock_code}")
                return data
        
        self.logger.info(f"开始获取 {stock_code} 的综合新闻数据（最近{days}天）...")
        
        try:
            import akshare as ak
            
            stock_name = self.get_stock_name(stock_code)
            all_news_data = {
                'company_news': [],
                'announcements': [],
                'research_reports': [],
                'industry_news': [],
                'market_sentiment': {},
                'news_summary': {}
            }
            
            # 1. 公司新闻
            try:
                self.logger.info("正在获取公司新闻...")
                company_news = ak.stock_news_em(symbol=stock_code)
                if not company_news.empty:
                    # 处理新闻数据
                    processed_news = []
                    for _, row in company_news.head(50).iterrows():
                        news_item = {
                            'title': str(row.get(row.index[0], '')),  # 第一列通常是标题
                            'content': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'date': str(row.get(row.index[2], '')) if len(row.index) > 2 else datetime.now().strftime('%Y-%m-%d'),
                            'source': 'eastmoney',
                            'url': str(row.get(row.index[3], '')) if len(row.index) > 3 else '',
                            'relevance_score': 1.0
                        }
                        processed_news.append(news_item)
                    
                    all_news_data['company_news'] = processed_news
                    self.logger.info(f"✓ 获取公司新闻 {len(processed_news)} 条")
                else:
                    self.logger.info("公司新闻数据为空")
            except Exception as e:
                self.logger.warning(f"获取公司新闻失败: {e}")
            
            # 2. 公司公告
            try:
                self.logger.info("正在获取公司公告...")
                announcements = ak.stock_zh_a_alerts_cls(symbol=stock_code)
                if not announcements.empty:
                    processed_announcements = []
                    for _, row in announcements.head(30).iterrows():
                        announcement = {
                            'title': str(row.get(row.index[0], '')),
                            'content': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'date': str(row.get(row.index[2], '')) if len(row.index) > 2 else datetime.now().strftime('%Y-%m-%d'),
                            'type': str(row.get(row.index[3], '')) if len(row.index) > 3 else '公告',
                            'relevance_score': 1.0
                        }
                        processed_announcements.append(announcement)
                    
                    all_news_data['announcements'] = processed_announcements
                    self.logger.info(f"✓ 获取公司公告 {len(processed_announcements)} 条")
            except Exception as e:
                self.logger.warning(f"获取公司公告失败: {e}")
            
            # 3. 研究报告
            try:
                self.logger.info("正在获取研究报告...")
                research_reports = ak.stock_research_report_em(symbol=stock_code)
                if not research_reports.empty:
                    processed_reports = []
                    for _, row in research_reports.head(20).iterrows():
                        report = {
                            'title': str(row.get(row.index[0], '')),
                            'institution': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'rating': str(row.get(row.index[2], '')) if len(row.index) > 2 else '',
                            'target_price': str(row.get(row.index[3], '')) if len(row.index) > 3 else '',
                            'date': str(row.get(row.index[4], '')) if len(row.index) > 4 else datetime.now().strftime('%Y-%m-%d'),
                            'relevance_score': 0.9
                        }
                        processed_reports.append(report)
                    
                    all_news_data['research_reports'] = processed_reports
                    self.logger.info(f"✓ 获取研究报告 {len(processed_reports)} 条")
            except Exception as e:
                self.logger.warning(f"获取研究报告失败: {e}")
            
            # 4. 行业新闻
            try:
                self.logger.info("正在获取行业新闻...")
                industry_news = self._get_comprehensive_industry_news(stock_code, days)
                all_news_data['industry_news'] = industry_news
                self.logger.info(f"✓ 获取行业新闻 {len(industry_news)} 条")
            except Exception as e:
                self.logger.warning(f"获取行业新闻失败: {e}")
            
            # 5. 新闻摘要统计
            try:
                total_news = (len(all_news_data['company_news']) + 
                            len(all_news_data['announcements']) + 
                            len(all_news_data['research_reports']) + 
                            len(all_news_data['industry_news']))
                
                all_news_data['news_summary'] = {
                    'total_news_count': total_news,
                    'company_news_count': len(all_news_data['company_news']),
                    'announcements_count': len(all_news_data['announcements']),
                    'research_reports_count': len(all_news_data['research_reports']),
                    'industry_news_count': len(all_news_data['industry_news']),
                    'data_freshness': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
            except Exception as e:
                self.logger.warning(f"生成新闻摘要失败: {e}")
            
            # 缓存数据
            self.news_cache[cache_key] = (datetime.now(), all_news_data)
            
            self.logger.info(f"✓ 综合新闻数据获取完成，总计 {all_news_data['news_summary'].get('total_news_count', 0)} 条")
            return all_news_data
            
        except Exception as e:
            self.logger.error(f"获取综合新闻数据失败: {str(e)}")
            return {
                'company_news': [],
                'announcements': [],
                'research_reports': [],
                'industry_news': [],
                'market_sentiment': {},
                'news_summary': {'total_news_count': 0}
            }

    def _get_comprehensive_industry_news(self, stock_code, days=30):
        """获取详细的行业新闻"""
        try:
            # 这里可以根据实际需要扩展行业新闻获取逻辑
            # 目前返回一个示例结构
            industry_news = []
            
            # 可以添加更多的行业新闻源
            # 比如获取同行业其他公司的新闻
            # 获取行业政策新闻等
            
            self.logger.info(f"行业新闻获取完成，共 {len(industry_news)} 条")
            return industry_news
            
        except Exception as e:
            self.logger.warning(f"获取行业新闻失败: {e}")
            return []

    def calculate_advanced_sentiment_analysis(self, comprehensive_news_data):
        """计算高级情绪分析（强化版）"""
        self.logger.info("开始高级情绪分析...")

        try:
            # 统一抽取文本与元数据
            items = []
            def add_item(text, kind, date=None, source_weight=1.0):
                if not isinstance(text, str) or not text.strip():
                    return
                items.append({
                    'text': text.strip(),
                    'type': kind,
                    'date': date,
                    'source_weight': float(source_weight)
                })

            for x in comprehensive_news_data.get('company_news', []) or []:
                add_item(f"{x.get('title','')} {x.get('content','')}", 'company_news', x.get('date'), 1.0)

            for x in comprehensive_news_data.get('announcements', []) or []:
                add_item(f"{x.get('title','')} {x.get('content','')}", 'announcement', x.get('date'), 1.3)

            for x in comprehensive_news_data.get('research_reports', []) or []:
                add_item(f"{x.get('title','')} {x.get('rating','')}", 'research_report', x.get('date'), 1.1)

            for x in comprehensive_news_data.get('industry_news', []) or []:
                add_item(f"{x.get('title','')} {x.get('content','')}", 'industry_news', x.get('date'), 0.8)

            if not items:
                return {
                    'overall_sentiment': 0.0,
                    'sentiment_by_type': {},
                    'sentiment_trend': '中性',
                    'confidence_score': 0.0,
                    'total_analyzed': 0
                }

            # 情感词表（可扩展）
            positive_words = {
                '上涨','涨停','利好','突破','增长','盈利','收益','回升','强势','看好',
                '买入','推荐','优秀','领先','创新','发展','机会','潜力','稳定','改善',
                '提升','超预期','积极','乐观','向好','受益','龙头','热点','爆发','翻倍',
                '业绩','增收','扩张','合作','签约','中标','获得','成功','完成','达成'
            }
            negative_words = {
                '下跌','跌停','利空','破位','下滑','亏损','风险','回调','弱势','看空',
                '卖出','减持','较差','落后','滞后','困难','危机','担忧','悲观','恶化',
                '下降','低于预期','消极','压力','套牢','被套','暴跌','崩盘','踩雷','退市',
                '违规','处罚','调查','停牌','债务','违约','诉讼','纠纷','问题'
            }

            # 去重：相同标题/文本的只取一次
            seen = set()
            dedup_items = []
            for it in items:
                key = (it['type'], it['text'])
                if key in seen:
                    continue
                seen.add(key)
                dedup_items.append(it)

            # 计算时间衰减权重
            dates = [it.get('date') for it in dedup_items]
            tweights = self._time_decay_weights(dates, half_life_days=20.0)
            for it, tw in zip(dedup_items, tweights):
                it['time_weight'] = float(tw)

            # 逐条计算情绪分
            by_type_scores = {}
            weighted_scores = []
            for it in dedup_items:
                tokens = self._tokenize_cn(it['text'])
                raw = self._sentiment_from_tokens(tokens, positive_words, negative_words)  # [-1,1]
                # 综合权重：来源权重 * 时间权重
                w = float(it.get('source_weight', 1.0)) * float(it.get('time_weight', 1.0))
                weighted = raw * w
                weighted_scores.append(weighted)

                typ = it['type']
                by_type_scores.setdefault(typ, []).append(weighted)

            # 鲁棒聚合：采用缩尾与均值
            ws = pd.Series(weighted_scores, dtype=float)
            ws = self._winsorize(ws, 0.05, 0.95)
            overall = float(ws.mean()) if len(ws) else 0.0

            # 类型均值
            avg_by_type = {}
            for typ, arr in by_type_scores.items():
                s = pd.Series(arr, dtype=float)
                s = self._winsorize(s, 0.05, 0.95)
                avg_by_type[typ] = float(s.mean()) if len(s) else 0.0

            # 情绪趋势
            if overall > 0.35:
                trend = '非常积极'
            elif overall > 0.15:
                trend = '偏向积极'
            elif overall > -0.15:
                trend = '相对中性'
            elif overall > -0.35:
                trend = '偏向消极'
            else:
                trend = '非常消极'

            # 置信度：数据量与时间新鲜度合成
            n = len(dedup_items)
            recency = float(np.mean([it.get('time_weight', 0.0) for it in dedup_items])) if n else 0.0
            confidence = float(min(1.0, (n / 60.0) * 0.6 + recency * 0.4))

            result = {
                'overall_sentiment': overall,
                'sentiment_by_type': avg_by_type,
                'sentiment_trend': trend,
                'confidence_score': confidence,
                'total_analyzed': n,
                'type_distribution': {k: len(v) for k, v in by_type_scores.items()},
                'positive_ratio': float((ws > 0).mean()) if len(ws) else 0.0,
                'negative_ratio': float((ws < 0).mean()) if len(ws) else 0.0
            }

            self.logger.info(f"✓ 高级情绪分析完成: {trend} (得分: {overall:.3f})")
            return result

        except Exception as e:
            self.logger.error(f"高级情绪分析失败: {e}")
            return {
                'overall_sentiment': 0.0,
                'sentiment_by_type': {},
                'sentiment_trend': '分析失败',
                'confidence_score': 0.0,
                'total_analyzed': 0
            }

    def calculate_technical_indicators(self, price_data):
        """计算技术指标（增强版）"""
        try:
            if price_data.empty or 'close' not in price_data.columns:
                return self._get_default_technical_analysis()

            technical_analysis = {}

            # 为确认机制预留序列
            confirm_n = int(max(1, min(10, float(self.thresholds.get('confirm_days', 1) or 1))))
            rsi_series = None
            macd_hist_series = None
            macd_line_series = None
            signal_line_series = None
            ma20_series = None
            close_series = None

            close = price_data['close'].astype(float)
            high = price_data['high'].astype(float) if 'high' in price_data.columns else close
            low = price_data['low'].astype(float) if 'low' in price_data.columns else close
            volume = price_data['volume'].astype(float) if 'volume' in price_data.columns else pd.Series([np.nan]*len(price_data), index=price_data.index)

            # 移动平均线（短中长期）
            try:
                price_data['ma20'] = close.rolling(window=20, min_periods=1).mean()
                price_data['ma50'] = close.rolling(window=50, min_periods=1).mean()
                price_data['ma120'] = close.rolling(window=120, min_periods=1).mean()

                latest = float(close.iloc[-1])
                ma20 = float(price_data['ma20'].iloc[-1])
                ma50 = float(price_data['ma50'].iloc[-1]) if len(price_data) >= 50 else ma20
                ma120 = float(price_data['ma120'].iloc[-1]) if len(price_data) >= 120 else ma50

                if latest > ma20 > ma50 > ma120:
                    technical_analysis['ma_trend'] = '多头排列'
                elif latest < ma20 < ma50 < ma120:
                    technical_analysis['ma_trend'] = '空头排列'
                else:
                    technical_analysis['ma_trend'] = '震荡整理'

                technical_analysis['price_above_ma20'] = latest / ma20 if ma20 else 1.0
                technical_analysis['ma_slope20'] = float(price_data['ma20'].diff().iloc[-1]) if len(price_data) >= 2 else 0.0
                ma20_series = price_data['ma20']
                close_series = close
            except Exception:
                technical_analysis['ma_trend'] = '计算失败'

            # RSI (Wilder) 14
            try:
                delta = close.diff()
                gain = delta.clip(lower=0)
                loss = -delta.clip(upper=0)
                window = 14
                avg_gain = gain.ewm(alpha=1/window, adjust=False, min_periods=window).mean()
                avg_loss = loss.ewm(alpha=1/window, adjust=False, min_periods=window).mean()
                rs = avg_gain / avg_loss.replace(0, np.nan)
                rsi = 100 - (100 / (1 + rs))
                rsi_series = rsi
                rsi_val = float(rsi.iloc[-1]) if np.isfinite(rsi.iloc[-1]) else 50.0
                technical_analysis['rsi'] = rsi_val
            except Exception:
                technical_analysis['rsi'] = 50.0

            # MACD with confirmation
            try:
                ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
                ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
                macd_line = ema12 - ema26
                signal_line = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
                hist = macd_line - signal_line
                macd_hist_series = hist
                macd_line_series = macd_line
                signal_line_series = signal_line

                macd_signal = '数据不足'
                if len(hist) >= 3:
                    last = float(hist.iloc[-1])
                    prev = float(hist.iloc[-2])
                    prev2 = float(hist.iloc[-3])
                    cross_up = (macd_line.iloc[-2] < signal_line.iloc[-2]) and (macd_line.iloc[-1] > signal_line.iloc[-1])
                    cross_down = (macd_line.iloc[-2] > signal_line.iloc[-2]) and (macd_line.iloc[-1] < signal_line.iloc[-1])
                    trend_up = last > prev > prev2
                    trend_down = last < prev < prev2

                    if (cross_up or trend_up) and last > 0:
                        macd_signal = '金叉向上'
                    elif (cross_down or trend_down) and last < 0:
                        macd_signal = '死叉向下'
                    else:
                        macd_signal = '横盘整理'

                technical_analysis['macd_signal'] = macd_signal
            except Exception:
                technical_analysis['macd_signal'] = '计算失败'

            # 布林带位置与带宽
            try:
                bb_window = min(20, len(close))
                mid = close.rolling(bb_window, min_periods=1).mean()
                std = close.rolling(bb_window, min_periods=1).std()
                upper = mid + 2*std
                lower = mid - 2*std
                last_close = float(close.iloc[-1])
                u = float(upper.iloc[-1])
                l = float(lower.iloc[-1])
                if u != l:
                    pos = (last_close - l) / (u - l)
                else:
                    pos = 0.5
                technical_analysis['bb_position'] = float(pos)
                technical_analysis['bb_width'] = float(self._safe_ratio(u - l, mid.iloc[-1], default=0.0))
            except Exception:
                technical_analysis['bb_position'] = 0.5
                technical_analysis['bb_width'] = 0.0

            # 成交量与量能确认
            try:
                vol_ma20 = volume.rolling(window=min(20, len(volume)), min_periods=1).mean()
                vol_ratio = float(self._safe_ratio(volume.iloc[-1], vol_ma20.iloc[-1], default=1.0)) if len(volume) else 1.0
                technical_analysis['volume_ratio'] = vol_ratio

                # 价格变动百分比
                if 'change_pct' in price_data.columns and not pd.isna(price_data['change_pct'].iloc[-1]):
                    price_change = float(price_data['change_pct'].iloc[-1])
                elif len(close) >= 2 and close.iloc[-2] > 0:
                    price_change = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
                else:
                    price_change = 0.0

                if vol_ratio > 1.5:
                    technical_analysis['volume_status'] = '放量上涨' if price_change > 0 else '放量下跌'
                elif vol_ratio < 0.6:
                    technical_analysis['volume_status'] = '缩量调整'
                else:
                    technical_analysis['volume_status'] = '温和放量'
            except Exception:
                technical_analysis['volume_status'] = '数据不足'
                technical_analysis['volume_ratio'] = 1.0

            # 换手率（如有）
            try:
                if 'turnover_rate' in price_data.columns:
                    tr_series = pd.to_numeric(price_data['turnover_rate'], errors='coerce')
                    if len(tr_series.dropna()):
                        technical_analysis['turnover_rate'] = float(tr_series.iloc[-1])
                        tr_ma20 = tr_series.rolling(window=min(20, len(tr_series)), min_periods=1).mean().iloc[-1]
                        technical_analysis['turnover_rate_ma20'] = float(tr_ma20)
                        technical_analysis['turnover_rate_ratio'] = float(self._safe_ratio(tr_series.iloc[-1], tr_ma20, default=1.0))
            except Exception:
                pass

            # ATR(14) 波动率
            try:
                prev_close = close.shift(1)
                tr = pd.concat([
                    (high - low).abs(),
                    (high - prev_close).abs(),
                    (low - prev_close).abs()
                ], axis=1).max(axis=1)
                atr = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
                atr_pct = float(self._safe_ratio(atr.iloc[-1], close.iloc[-1], default=0.0) * 100.0) if len(atr) else 0.0
                technical_analysis['atr_pct'] = atr_pct
            except Exception:
                technical_analysis['atr_pct'] = 0.0

            # 附加用于多日确认的序列尾部片段（最多10根K线）
            try:
                tail_len = 10
                series_tail = {}
                if rsi_series is not None and len(rsi_series.dropna()) >= 1:
                    series_tail['rsi'] = [float(x) if np.isfinite(x) else 50.0 for x in rsi_series.tail(tail_len).tolist()]
                if macd_hist_series is not None and len(macd_hist_series.dropna()) >= 1:
                    series_tail['macd_hist'] = [float(x) if np.isfinite(x) else 0.0 for x in macd_hist_series.tail(tail_len).tolist()]
                if macd_line_series is not None and len(macd_line_series.dropna()) >= 1:
                    series_tail['macd_line'] = [float(x) if np.isfinite(x) else 0.0 for x in macd_line_series.tail(tail_len).tolist()]
                if signal_line_series is not None and len(signal_line_series.dropna()) >= 1:
                    series_tail['signal_line'] = [float(x) if np.isfinite(x) else 0.0 for x in signal_line_series.tail(tail_len).tolist()]
                if ma20_series is not None and len(ma20_series.dropna()) >= 1:
                    series_tail['ma20'] = [float(x) if np.isfinite(x) else 0.0 for x in ma20_series.tail(tail_len).tolist()]
                if close_series is not None and len(close_series.dropna()) >= 1:
                    series_tail['close'] = [float(x) if np.isfinite(x) else 0.0 for x in close_series.tail(tail_len).tolist()]
                if series_tail:
                    technical_analysis['series'] = series_tail
            except Exception:
                pass

            return technical_analysis

        except Exception as e:
            self.logger.error(f"技术指标计算失败: {str(e)}")
            return self._get_default_technical_analysis()

    def _get_default_technical_analysis(self):
        """获取默认技术分析结果"""
        return {
            'ma_trend': '数据不足',
            'rsi': 50.0,
            'macd_signal': '数据不足',
            'bb_position': 0.5,
            'volume_status': '数据不足'
        }

    def calculate_technical_score(self, technical_analysis, confirm_days: Optional[int] = None):
        """计算技术分析得分（增强版）"""
        try:
            score = 50.0

            # 读取阈值
            rsi_ob = float(self.thresholds.get('rsi_overbought', 70.0))
            rsi_os = float(self.thresholds.get('rsi_oversold', 30.0))
            rsi_nl = float(self.thresholds.get('rsi_neutral_low', 45.0))
            rsi_nh = float(self.thresholds.get('rsi_neutral_high', 65.0))
            bb_lo = float(self.thresholds.get('bb_lower', 0.2))
            bb_hi = float(self.thresholds.get('bb_upper', 0.8))
            vr_low = float(self.thresholds.get('volume_ratio_low', 0.6))
            vr_high = float(self.thresholds.get('volume_ratio_high', 1.5))
            vr_vlow = float(self.thresholds.get('volume_ratio_very_low', 0.7))
            vr_vhigh = float(self.thresholds.get('volume_ratio_very_high', 2.0))
            atr_hi = float(self.thresholds.get('atr_high_risk', 6.0))
            atr_low = float(self.thresholds.get('atr_low_vol', 2.0))

            n_confirm = int(confirm_days if confirm_days is not None else self.thresholds.get('confirm_days', 1))
            n_confirm = max(1, min(10, n_confirm))

            series = technical_analysis.get('series', {}) or {}
            rsi_series = series.get('rsi') or []
            macd_hist = series.get('macd_hist') or []
            macd_line = series.get('macd_line') or []
            signal_line = series.get('signal_line') or []
            ma20 = series.get('ma20') or []
            close = series.get('close') or []

            def last_n_all(pred_arr, predicate):
                try:
                    arr = list(pred_arr)[-n_confirm:]
                    return len(arr) >= n_confirm and all(predicate(x) for x in arr)
                except Exception:
                    return False

            # 均线趋势评分（含多日确认：收盘价连续高于MA20，并且MA20向上/向下）
            ma_trend = technical_analysis.get('ma_trend', '数据不足')
            if ma_trend == '多头排列':
                confirmed_ma = False
                if n_confirm > 1 and close and ma20 and len(close) >= n_confirm and len(ma20) >= n_confirm:
                    above = [c > m for c, m in zip(close[-n_confirm:], ma20[-n_confirm:])]
                    slope_up = all((ma20[i] - ma20[i-1]) >= 0 for i in range(1, min(len(ma20), n_confirm)))
                    confirmed_ma = all(above) and slope_up
                score += 18 if (n_confirm == 1 or confirmed_ma) else 12
            elif ma_trend == '空头排列':
                confirmed_ma = False
                if n_confirm > 1 and close and ma20 and len(close) >= n_confirm and len(ma20) >= n_confirm:
                    below = [c < m for c, m in zip(close[-n_confirm:], ma20[-n_confirm:])]
                    slope_down = all((ma20[i] - ma20[i-1]) <= 0 for i in range(1, min(len(ma20), n_confirm)))
                    confirmed_ma = all(below) and slope_down
                score -= 18 if (n_confirm == 1 or confirmed_ma) else 12

            # RSI评分（用阈值）
            rsi_val = float(technical_analysis.get('rsi', 50))
            if rsi_nl <= rsi_val <= rsi_nh:
                if n_confirm > 1 and rsi_series:
                    in_band = last_n_all(rsi_series, lambda v: rsi_nl <= float(v) <= rsi_nh)
                    score += 8 if in_band else 4
                else:
                    score += 8
            elif rsi_os <= rsi_val < rsi_nl:
                score += 4
            elif rsi_nh < rsi_val <= rsi_ob + 5:
                score -= 3
            elif rsi_val < rsi_os:
                score += 6  # 超卖反弹潜力
            elif rsi_val > rsi_ob + 5:
                score -= 8  # 超买风险

            # MACD评分（多日确认：直方图持续走高/走低，且当前在零轴之上/之下）
            macd_signal = technical_analysis.get('macd_signal', '横盘整理')
            if macd_signal == '金叉向上':
                if n_confirm > 1 and macd_hist and macd_line and signal_line:
                    arr_h = macd_hist[-n_confirm:]
                    arr_m = macd_line[-n_confirm:]
                    arr_s = signal_line[-n_confirm:]
                    inc = all(arr_h[i] >= arr_h[i-1] for i in range(1, len(arr_h)))
                    above = (arr_m[-1] > arr_s[-1]) and (arr_h[-1] > 0)
                    score += 14 if (inc and above) else 8
                else:
                    score += 14
            elif macd_signal == '死叉向下':
                if n_confirm > 1 and macd_hist and macd_line and signal_line:
                    arr_h = macd_hist[-n_confirm:]
                    arr_m = macd_line[-n_confirm:]
                    arr_s = signal_line[-n_confirm:]
                    dec = all(arr_h[i] <= arr_h[i-1] for i in range(1, len(arr_h)))
                    below = (arr_m[-1] < arr_s[-1]) and (arr_h[-1] < 0)
                    score -= 14 if (dec and below) else 8
                else:
                    score -= 14

            # 布林带位置评分（用阈值）
            bb_position = float(technical_analysis.get('bb_position', 0.5))
            if (bb_lo + 0.05) <= bb_position <= (bb_hi - 0.05):
                score += 4
            elif bb_position < bb_lo:
                score += 7  # 下轨附近，反弹潜力
            elif bb_position > (bb_hi + 0.05):
                score -= 6  # 上轨附近，回落风险

            # 成交量状态与量能数值化
            volume_status = technical_analysis.get('volume_status', '数据不足')
            if '放量上涨' in volume_status:
                score += 10
            elif '放量下跌' in volume_status:
                score -= 10

            vol_ratio = float(technical_analysis.get('volume_ratio', 1.0))
            if vol_ratio > vr_vhigh:
                score += 4
            elif vol_ratio < vr_vlow:
                score -= 3

            # 波动率（ATR%）
            atr_pct = float(technical_analysis.get('atr_pct', 0.0))
            if atr_pct > atr_hi:
                score -= 6  # 波动过大
            elif atr_pct < atr_low and ma_trend == '多头排列':
                score += 3  # 稳定上行

            score = float(max(0.0, min(100.0, score)))
            return score

        except Exception as e:
            self.logger.error(f"技术分析评分失败: {str(e)}")
            return 50.0

    def calculate_fundamental_score(self, fundamental_data):
        """计算基本面得分（增强版）"""
        try:
            score = 50.0

            fi = fundamental_data.get('financial_indicators', {}) or {}
            count = len(fi)
            if count >= 15:
                score += 18
            elif count >= 8:
                score += 8

            # 盈利能力：ROE
            roe = float(fi.get('净资产收益率', 0) or 0)
            if roe > 20:
                score += 12
            elif roe > 15:
                score += 8
            elif roe > 10:
                score += 4
            elif roe < 5:
                score -= 4

            # 偿债能力：资产负债率（越低越好）
            debt_ratio = float(fi.get('资产负债率', 50) or 50)
            if debt_ratio < 30:
                score += 6
            elif debt_ratio > 70:
                score -= 8

            # 成长性：营收/净利增速
            rev_g = float(fi.get('营收同比增长率', 0) or 0)
            np_g = float(fi.get('净利润同比增长率', 0) or 0)
            if rev_g > 20:
                score += 6
            elif rev_g > 10:
                score += 3
            elif rev_g < -10:
                score -= 6

            if np_g > 25:
                score += 6
            elif np_g > 10:
                score += 3
            elif np_g < -10:
                score -= 8

            # 估值：PE、PB（越低越好，行业差异忽略，给区间分）
            val = fundamental_data.get('valuation', {}) or {}
            try:
                pe = float(val.get('市盈率', 0) or 0)
            except Exception:
                pe = 0
            try:
                pb = float(val.get('市净率', 0) or 0)
            except Exception:
                pb = 0
            try:
                dy = float(val.get('股息收益率', 0) or 0)
            except Exception:
                dy = 0

            if 0 < pe <= 15:
                score += 6
            elif 15 < pe <= 30:
                score += 2
            elif pe > 60:
                score -= 8

            if 0 < pb <= 2:
                score += 4
            elif pb > 5:
                score -= 6

            if dy >= 3:
                score += 4

            # 业绩预告：正向措辞加分
            forecasts = fundamental_data.get('performance_forecast', []) or []
            positive_kw = ['预增','上调','超预期','扭亏','增长','改善']
            negative_kw = ['预减','下调','低于预期','亏损','恶化']
            pos_hit = 0
            neg_hit = 0
            for f in forecasts[:10]:
                text = ' '.join([str(v) for v in (f.values() if isinstance(f, dict) else [f])])
                if any(k in text for k in positive_kw):
                    pos_hit += 1
                if any(k in text for k in negative_kw):
                    neg_hit += 1
            score += min(10, pos_hit * 2)
            score -= min(10, neg_hit * 2)

            score = float(max(0.0, min(100.0, score)))
            return score

        except Exception as e:
            self.logger.error(f"基本面评分失败: {str(e)}")
            return 50.0

    def calculate_sentiment_score(self, sentiment_analysis):
        """计算情绪分析得分"""
        try:
            overall_sentiment = sentiment_analysis.get('overall_sentiment', 0.0)
            confidence_score = sentiment_analysis.get('confidence_score', 0.0)
            total_analyzed = sentiment_analysis.get('total_analyzed', 0)
            
            # 基础得分：将情绪得分从[-1,1]映射到[0,100]
            base_score = (overall_sentiment + 1) * 50
            
            # 置信度调整
            confidence_adjustment = confidence_score * 10
            
            # 新闻数量调整
            news_adjustment = min(total_analyzed / 100, 1.0) * 10
            
            final_score = base_score + confidence_adjustment + news_adjustment
            final_score = max(0, min(100, final_score))
            
            return final_score
            
        except Exception as e:
            self.logger.error(f"情绪得分计算失败: {e}")
            return 50

    def calculate_comprehensive_score(self, scores):
        """计算综合得分（动态权重）"""
        try:
            t = float(scores.get('technical', 50.0))
            f = float(scores.get('fundamental', 50.0))
            s = float(scores.get('sentiment', 50.0))

            # 统一使用 self.weights（构造器可覆盖），向后兼容 self.analysis_weights
            base_w = (getattr(self, 'weights', None) or self.analysis_weights).copy()
            # 动态缩放：依据最近一次数据质量
            dq = getattr(self, '_last_data_quality', {}) or {}
            fi_count = float(dq.get('financial_indicators_count', 0) or 0)
            news_count = float(dq.get('total_news_count', dq.get('news_count', 0)) or 0)
            s_conf = float(dq.get('sentiment_confidence', dq.get('confidence_score', 0.0)) or 0.0)

            f_scale = 0.6 + min(1.0, fi_count / 15.0) * 0.6  # 0.6~1.2
            s_scale = 0.6 + min(1.0, news_count / 60.0) * 0.3 + min(1.0, s_conf) * 0.3  # 0.6~1.2
            t_scale = 1.0  # 技术面保持基准

            w_t = base_w.get('technical', 0.4) * t_scale
            w_f = base_w.get('fundamental', 0.4) * f_scale
            w_s = base_w.get('sentiment', 0.2) * s_scale

            w_sum = w_t + w_f + w_s
            if w_sum <= 0:
                w_t, w_f, w_s = 0.4, 0.4, 0.2
                w_sum = 1.0

            w_t, w_f, w_s = w_t / w_sum, w_f / w_sum, w_s / w_sum

            composite = t * w_t + f * w_f + s * w_s
            return float(max(0.0, min(100.0, composite)))

        except Exception as e:
            self.logger.error(f"计算综合得分失败: {e}")
            return 50.0

    def get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            import akshare as ak
            
            try:
                stock_info = ak.stock_individual_info_em(symbol=stock_code)
                if not stock_info.empty:
                    info_dict = dict(zip(stock_info['item'], stock_info['value']))
                    stock_name = info_dict.get('股票简称', stock_code)
                    if stock_name and stock_name != stock_code:
                        return stock_name
            except Exception as e:
                self.logger.warning(f"获取股票名称失败: {e}")
            
            return stock_code
            
        except Exception as e:
            self.logger.warning(f"获取股票名称时出错: {e}")
            return stock_code

    def get_price_info(self, price_data):
        """从价格数据中提取关键信息 - 修复版本"""
        try:
            if price_data.empty or 'close' not in price_data.columns:
                self.logger.warning("价格数据为空或缺少收盘价列")
                return {
                    'current_price': 0.0,
                    'price_change': 0.0,
                    'volume_ratio': 1.0,
                    'volatility': 0.0
                }
            
            # 获取最新数据
            latest = price_data.iloc[-1]
            
            # 确保使用收盘价作为当前价格
            current_price = float(latest['close'])
            self.logger.info(f"✓ 当前价格(收盘价): {current_price}")
            
            # 如果收盘价异常，尝试使用其他价格
            if pd.isna(current_price) or current_price <= 0:
                if 'open' in price_data.columns and not pd.isna(latest['open']) and latest['open'] > 0:
                    current_price = float(latest['open'])
                    self.logger.warning(f"⚠️ 收盘价异常，使用开盘价: {current_price}")
                elif 'high' in price_data.columns and not pd.isna(latest['high']) and latest['high'] > 0:
                    current_price = float(latest['high'])
                    self.logger.warning(f"⚠️ 收盘价异常，使用最高价: {current_price}")
                else:
                    self.logger.error(f"❌ 所有价格数据都异常")
                    return {
                        'current_price': 0.0,
                        'price_change': 0.0,
                        'volume_ratio': 1.0,
                        'volatility': 0.0
                    }
            
            # 计算价格变化
            price_change = 0.0
            try:
                if 'change_pct' in price_data.columns and not pd.isna(latest['change_pct']):
                    price_change = float(latest['change_pct'])
                    self.logger.info(f"✓ 使用现成的涨跌幅: {price_change}%")
                elif len(price_data) > 1:
                    prev = price_data.iloc[-2]
                    prev_price = float(prev['close'])
                    if prev_price > 0 and not pd.isna(prev_price):
                        price_change = ((current_price - prev_price) / prev_price * 100)
                        self.logger.info(f"✓ 计算涨跌幅: {price_change}%")
            except Exception as e:
                self.logger.warning(f"计算价格变化失败: {e}")
                price_change = 0.0
            
            # 计算成交量比率
            volume_ratio = 1.0
            try:
                if 'volume' in price_data.columns:
                    volume_data = price_data['volume'].dropna()
                    if len(volume_data) >= 5:
                        recent_volume = volume_data.tail(5).mean()
                        avg_volume = volume_data.mean()
                        if avg_volume > 0:
                            volume_ratio = recent_volume / avg_volume
            except Exception as e:
                self.logger.warning(f"计算成交量比率失败: {e}")
                volume_ratio = 1.0
            
            # 计算波动率
            volatility = 0.0
            try:
                close_prices = price_data['close'].dropna()
                if len(close_prices) >= 20:
                    returns = close_prices.pct_change().dropna()
                    if len(returns) >= 20:
                        volatility = returns.tail(20).std() * 100
            except Exception as e:
                self.logger.warning(f"计算波动率失败: {e}")
                volatility = 0.0
            
            result = {
                'current_price': current_price,
                'price_change': price_change,
                'volume_ratio': volume_ratio,
                'volatility': volatility
            }
            
            self.logger.info(f"✓ 价格信息提取完成: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"获取价格信息失败: {e}")
            return {
                'current_price': 0.0,
                'price_change': 0.0,
                'volume_ratio': 1.0,
                'volatility': 0.0
            }

    def generate_recommendation(self, scores):
        """根据得分生成投资建议"""
        try:
            comprehensive_score = scores.get('comprehensive', 50)
            technical_score = scores.get('technical', 50)
            fundamental_score = scores.get('fundamental', 50)
            sentiment_score = scores.get('sentiment', 50)
            
            if comprehensive_score >= 80:
                if technical_score >= 75 and fundamental_score >= 75:
                    return "强烈推荐买入"
                else:
                    return "推荐买入"
            elif comprehensive_score >= 65:
                if sentiment_score >= 60:
                    return "建议买入"
                else:
                    return "谨慎买入"
            elif comprehensive_score >= 45:
                return "持有观望"
            elif comprehensive_score >= 30:
                return "建议减仓"
            else:
                return "建议卖出"
                
        except Exception as e:
            self.logger.warning(f"生成投资建议失败: {e}")
            return "数据不足，建议谨慎"

    def _build_enhanced_ai_analysis_prompt(self, stock_code, stock_name, scores, technical_analysis, 
                                        fundamental_data, sentiment_analysis, price_info):
        """构建增强版AI分析提示词，包含所有详细数据"""
        
        # 提取25项财务指标
        financial_indicators = fundamental_data.get('financial_indicators', {})
        financial_text = ""
        if financial_indicators:
            financial_text = "**25项核心财务指标：**\n"
            for i, (key, value) in enumerate(financial_indicators.items(), 1):
                if isinstance(value, (int, float)) and value != 0:
                    financial_text += f"{i}. {key}: {value}\n"
        
        # 提取新闻详细信息
        news_summary = sentiment_analysis.get('news_summary', {})
        company_news = sentiment_analysis.get('company_news', [])
        announcements = sentiment_analysis.get('announcements', [])
        research_reports = sentiment_analysis.get('research_reports', [])
        
        news_text = f"""
**新闻数据详情：**
- 公司新闻：{len(company_news)}条
- 公司公告：{len(announcements)}条  
- 研究报告：{len(research_reports)}条
- 总新闻数：{news_summary.get('total_news_count', 0)}条

**重要新闻标题（前10条）：**
"""
        
        for i, news in enumerate(company_news[:5], 1):
            news_text += f"{i}. {news.get('title', '未知标题')}\n"
        
        for i, announcement in enumerate(announcements[:5], 1):
            news_text += f"{i+5}. [公告] {announcement.get('title', '未知标题')}\n"
        
        # 提取研究报告信息
        research_text = ""
        if research_reports:
            research_text = "\n**研究报告摘要：**\n"
            for i, report in enumerate(research_reports[:5], 1):
                research_text += f"{i}. {report.get('institution', '未知机构')}: {report.get('rating', '未知评级')} - {report.get('title', '未知标题')}\n"
        
        # 构建完整的提示词
        prompt = f"""请作为一位资深的股票分析师，基于以下详细数据对股票进行深度分析：

**股票基本信息：**
- 股票代码：{stock_code}
- 股票名称：{stock_name}
- 当前价格：{price_info.get('current_price', 0):.2f}元
- 涨跌幅：{price_info.get('price_change', 0):.2f}%
- 成交量比率：{price_info.get('volume_ratio', 1):.2f}
- 波动率：{price_info.get('volatility', 0):.2f}%

**技术分析详情：**
- 均线趋势：{technical_analysis.get('ma_trend', '未知')}
- RSI指标：{technical_analysis.get('rsi', 50):.1f}
- MACD信号：{technical_analysis.get('macd_signal', '未知')}
- 布林带位置：{technical_analysis.get('bb_position', 0.5):.2f}
- 成交量状态：{technical_analysis.get('volume_status', '未知')}

{financial_text}

**估值指标：**
{self._format_dict_data(fundamental_data.get('valuation', {}))}

**业绩预告：**
共{len(fundamental_data.get('performance_forecast', []))}条业绩预告
{self._format_list_data(fundamental_data.get('performance_forecast', [])[:3])}

**分红配股：**
共{len(fundamental_data.get('dividend_info', []))}条分红配股信息
{self._format_list_data(fundamental_data.get('dividend_info', [])[:3])}

**股东结构：**
前10大股东信息：{len(fundamental_data.get('shareholders', []))}条
机构持股：{len(fundamental_data.get('institutional_holdings', []))}条

{news_text}

{research_text}

**市场情绪分析：**
- 整体情绪得分：{sentiment_analysis.get('overall_sentiment', 0):.3f}
- 情绪趋势：{sentiment_analysis.get('sentiment_trend', '中性')}
- 置信度：{sentiment_analysis.get('confidence_score', 0):.2f}
- 各类新闻情绪：{sentiment_analysis.get('sentiment_by_type', {})}

**综合评分：**
- 技术面得分：{scores.get('technical', 50):.1f}/100
- 基本面得分：{scores.get('fundamental', 50):.1f}/100
- 情绪面得分：{scores.get('sentiment', 50):.1f}/100
- 综合得分：{scores.get('comprehensive', 50):.1f}/100

**分析要求：**

请基于以上详细数据，从以下维度进行深度分析：

1. **财务健康度深度解读**：
   - 基于25项财务指标，全面评估公司财务状况
   - 识别财务优势和风险点
   - 与行业平均水平对比分析
   - 预测未来财务发展趋势

2. **技术面精准分析**：
   - 结合多个技术指标，判断短中长期趋势
   - 识别关键支撑位和阻力位
   - 分析成交量与价格的配合关系
   - 评估当前位置的风险收益比

3. **市场情绪深度挖掘**：
   - 分析公司新闻、公告、研报的影响
   - 评估市场对公司的整体预期
   - 识别情绪拐点和催化剂
   - 判断情绪对股价的推动或拖累作用

4. **基本面价值判断**：
   - 评估公司内在价值和成长潜力
   - 分析行业地位和竞争优势
   - 评估业绩预告和分红政策
   - 判断当前估值的合理性

5. **综合投资策略**：
   - 给出明确的买卖建议和理由
   - 设定目标价位和止损点
   - 制定分批操作策略
   - 评估投资时间周期

6. **风险机会识别**：
   - 列出主要投资风险和应对措施
   - 识别潜在催化剂和成长机会
   - 分析宏观环境和政策影响
   - 提供动态调整建议

请用专业、客观的语言进行分析，确保逻辑清晰、数据支撑充分、结论明确可执行。"""

        return prompt

    def _format_dict_data(self, data_dict, max_items=5):
        """格式化字典数据"""
        if not data_dict:
            return "无数据"
        
        formatted = ""
        for i, (key, value) in enumerate(data_dict.items()):
            if i >= max_items:
                break
            formatted += f"- {key}: {value}\n"
        
        return formatted if formatted else "无有效数据"

    def _format_list_data(self, data_list, max_items=3):
        """格式化列表数据"""
        if not data_list:
            return "无数据"
        
        formatted = ""
        for i, item in enumerate(data_list):
            if i >= max_items:
                break
            if isinstance(item, dict):
                # 取字典的前几个键值对
                item_str = ", ".join([f"{k}: {v}" for k, v in list(item.items())[:3]])
                formatted += f"- {item_str}\n"
            else:
                formatted += f"- {item}\n"
        
        return formatted if formatted else "无有效数据"

    def generate_ai_analysis(self, analysis_data, enable_streaming=False):
        """生成AI增强分析"""
        try:
            self.logger.info("🤖 开始AI深度分析...")
            
            stock_code = analysis_data.get('stock_code', '')
            stock_name = analysis_data.get('stock_name', stock_code)
            scores = analysis_data.get('scores', {})
            technical_analysis = analysis_data.get('technical_analysis', {})
            fundamental_data = analysis_data.get('fundamental_data', {})
            sentiment_analysis = analysis_data.get('sentiment_analysis', {})
            price_info = analysis_data.get('price_info', {})
            
            # 构建增强版AI分析提示词
            prompt = self._build_enhanced_ai_analysis_prompt(
                stock_code, stock_name, scores, technical_analysis, 
                fundamental_data, sentiment_analysis, price_info
            )
            
            # 调用AI API
            ai_response = self._call_ai_api(prompt, enable_streaming)
            
            if ai_response:
                self.logger.info("✅ AI深度分析完成")
                return ai_response
            else:
                self.logger.warning("⚠️ AI API不可用，使用高级分析模式")
                return self._advanced_rule_based_analysis(analysis_data)
                
        except Exception as e:
            self.logger.error(f"AI分析失败: {e}")
            return self._advanced_rule_based_analysis(analysis_data)

    def _call_ai_api(self, prompt, enable_streaming=False):
        """调用AI API"""
        try:
            model_preference = self.config.get('ai', {}).get('model_preference', 'openai')
            
            if model_preference == 'openai' and self.api_keys.get('openai'):
                result = self._call_openai_api(prompt, enable_streaming)
                if result:
                    return result
            
            elif model_preference == 'anthropic' and self.api_keys.get('anthropic'):
                result = self._call_claude_api(prompt, enable_streaming)
                if result:
                    return result
                    
            elif model_preference == 'zhipu' and self.api_keys.get('zhipu'):
                result = self._call_zhipu_api(prompt, enable_streaming)
                if result:
                    return result
            
            # 尝试其他可用的服务
            if self.api_keys.get('openai') and model_preference != 'openai':
                self.logger.info("尝试备用OpenAI API...")
                result = self._call_openai_api(prompt, enable_streaming)
                if result:
                    return result
                    
            if self.api_keys.get('anthropic') and model_preference != 'anthropic':
                self.logger.info("尝试备用Claude API...")
                result = self._call_claude_api(prompt, enable_streaming)
                if result:
                    return result
                    
            if self.api_keys.get('zhipu') and model_preference != 'zhipu':
                self.logger.info("尝试备用智谱AI API...")
                result = self._call_zhipu_api(prompt, enable_streaming)
                if result:
                    return result
            
            return None
                
        except Exception as e:
            self.logger.error(f"AI API调用失败: {e}")
            return None

    def _call_openai_api(self, prompt, enable_streaming=False):
        """调用OpenAI API"""
        try:
            import openai
            
            api_key = self.api_keys.get('openai')
            if not api_key:
                return None
                
            openai.api_key = api_key
            
            api_base = self.config.get('ai', {}).get('api_base_urls', {}).get('openai')
            if api_base:
                openai.api_base = api_base
            
            model = self.config.get('ai', {}).get('models', {}).get('openai', 'gpt-4o-mini')
            max_tokens = self.config.get('ai', {}).get('max_tokens', 6000)
            temperature = self.config.get('ai', {}).get('temperature', 0.7)
            
            self.logger.info(f"正在调用OpenAI {model} 进行深度分析...")
            
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "你是一位资深的股票分析师，具有丰富的市场经验和深厚的金融知识。请提供专业、客观、有深度的股票分析。"},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=temperature
            )
            
            return response.choices[0].message.content
                
        except Exception as e:
            self.logger.error(f"OpenAI API调用失败: {e}")
            return None

    def _call_claude_api(self, prompt, enable_streaming=False):
        """调用Claude API"""
        try:
            import anthropic
            
            api_key = self.api_keys.get('anthropic')
            if not api_key:
                return None
            
            client = anthropic.Anthropic(api_key=api_key)
            
            model = self.config.get('ai', {}).get('models', {}).get('anthropic', 'claude-3-haiku-20240307')
            max_tokens = self.config.get('ai', {}).get('max_tokens', 6000)
            
            self.logger.info(f"正在调用Claude {model} 进行深度分析...")
            
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            return response.content[0].text
            
        except Exception as e:
            self.logger.error(f"Claude API调用失败: {e}")
            return None

    def _call_zhipu_api(self, prompt, enable_streaming=False):
        """调用智谱AI API"""
        try:
            import zhipuai
            
            api_key = self.api_keys.get('zhipu')
            if not api_key:
                return None
            
            zhipuai.api_key = api_key
            
            model = self.config.get('ai', {}).get('models', {}).get('zhipu', 'chatglm_turbo')
            max_tokens = self.config.get('ai', {}).get('max_tokens', 6000)
            temperature = self.config.get('ai', {}).get('temperature', 0.7)
            
            self.logger.info(f"正在调用智谱AI {model} 进行深度分析...")
            
            response = zhipuai.model_api.invoke(
                model=model,
                prompt=[
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            return response['data']['choices'][0]['content']
            
        except Exception as e:
            self.logger.error(f"智谱AI API调用失败: {e}")
            return None

    def _advanced_rule_based_analysis(self, analysis_data):
        """高级规则分析（AI备用方案）"""
        try:
            self.logger.info("🧠 使用高级规则引擎进行分析...")
            
            stock_code = analysis_data.get('stock_code', '')
            stock_name = analysis_data.get('stock_name', stock_code)
            scores = analysis_data.get('scores', {})
            technical_analysis = analysis_data.get('technical_analysis', {})
            fundamental_data = analysis_data.get('fundamental_data', {})
            sentiment_analysis = analysis_data.get('sentiment_analysis', {})
            price_info = analysis_data.get('price_info', {})
            
            analysis_sections = []
            
            # 1. 综合评估
            comprehensive_score = scores.get('comprehensive', 50)
            analysis_sections.append(f"""## 📊 综合评估

基于技术面、基本面和市场情绪的综合分析，{stock_name}({stock_code})的综合得分为{comprehensive_score:.1f}分。

- 技术面得分：{scores.get('technical', 50):.1f}/100
- 基本面得分：{scores.get('fundamental', 50):.1f}/100  
- 情绪面得分：{scores.get('sentiment', 50):.1f}/100""")
            
            # 2. 财务分析
            financial_indicators = fundamental_data.get('financial_indicators', {})
            if financial_indicators:
                key_metrics = []
                for key, value in list(financial_indicators.items())[:10]:
                    if isinstance(value, (int, float)) and value != 0:
                        key_metrics.append(f"- {key}: {value}")
                
                financial_text = f"""## 💰 财务健康度分析

获取到{len(financial_indicators)}项财务指标，主要指标如下：

{chr(10).join(key_metrics[:8])}

财务健康度评估：{'优秀' if scores.get('fundamental', 50) >= 70 else '良好' if scores.get('fundamental', 50) >= 50 else '需关注'}"""
                analysis_sections.append(financial_text)
            
            # 3. 技术面分析
            tech_analysis = f"""## 📈 技术面分析

当前技术指标显示：
- 均线趋势：{technical_analysis.get('ma_trend', '未知')}
- RSI指标：{technical_analysis.get('rsi', 50):.1f}
- MACD信号：{technical_analysis.get('macd_signal', '未知')}
- 成交量状态：{technical_analysis.get('volume_status', '未知')}

技术面评估：{'强势' if scores.get('technical', 50) >= 70 else '中性' if scores.get('technical', 50) >= 50 else '偏弱'}"""
            analysis_sections.append(tech_analysis)
            
            # 4. 市场情绪
            sentiment_desc = f"""## 📰 市场情绪分析

基于{sentiment_analysis.get('total_analyzed', 0)}条新闻的分析：
- 整体情绪：{sentiment_analysis.get('sentiment_trend', '中性')}
- 情绪得分：{sentiment_analysis.get('overall_sentiment', 0):.3f}
- 置信度：{sentiment_analysis.get('confidence_score', 0):.2%}

新闻分布：
- 公司新闻：{len(sentiment_analysis.get('company_news', []))}条
- 公司公告：{len(sentiment_analysis.get('announcements', []))}条  
- 研究报告：{len(sentiment_analysis.get('research_reports', []))}条"""
            analysis_sections.append(sentiment_desc)
            
            # 5. 投资建议 + 可执行操作计划
            recommendation = self.generate_recommendation(scores)

            # 构造关键位与风控参数
            bb_pos = float(technical_analysis.get('bb_position', 0.5) or 0.5)
            ma_trend = technical_analysis.get('ma_trend', '未知')
            rsi_val = float(technical_analysis.get('rsi', 50))
            macd_sig = technical_analysis.get('macd_signal', '未知')
            vol_ratio = float(technical_analysis.get('volume_ratio', 1.0))
            atr_pct = float(technical_analysis.get('atr_pct', 0.0))
            price = float(price_info.get('current_price', 0.0))
            ma20 = float(technical_analysis.get('price_above_ma20', 1.0) * price) if price > 0 else None

            # 止损/止盈的基础（根据波动率ATR设定）
            sl_pct = 0.06 if atr_pct == 0 else min(0.06, max(0.03, atr_pct / 100 * 2.5))  # 约2.5倍ATR%
            tp_pct = max(0.06, min(0.18, sl_pct * 2.0))

            # 下一交易日操作建议
            next_day_plan = []
            if ma_trend == '多头排列' and macd_sig == '金叉向上' and rsi_val < 70 and bb_pos <= 0.8:
                next_day_plan.append("若高开并放量(量比>1.5)，可考虑分批跟进；低开不破 MA20 可在回踩时加仓。")
            elif ma_trend == '震荡整理' and 0.3 <= bb_pos <= 0.7:
                next_day_plan.append("震荡区间内高抛低吸：接近下轨/MA20附近小仓试多，靠近上轨逐步减仓。")
            elif ma_trend == '空头排列' or macd_sig == '死叉向下':
                next_day_plan.append("反弹缩量时逢高减仓；仅在强势放量收复MA20/MA50时考虑试探性仓位。")
            else:
                next_day_plan.append("等待方向选择：观察是否放量突破 MA20/MA50 或 MACD 重新转强后再行动。")

            # 换手率与量能提醒
            tr = technical_analysis.get('turnover_rate')
            tr_ma20 = technical_analysis.get('turnover_rate_ma20')
            tr_ratio = technical_analysis.get('turnover_rate_ratio')
            turnover_tips = []
            if tr is not None and tr_ma20 is not None and tr_ratio is not None:
                if tr_ratio >= 1.8:
                    turnover_tips.append(f"上一交易日换手率 {tr:.2f}% 显著高于20日均值({tr_ma20:.2f}%)，关注资金博弈与主力异动。")
                elif tr_ratio <= 0.6:
                    turnover_tips.append(f"上一交易日换手率 {tr:.2f}% 远低于20日均值({tr_ma20:.2f}%)，短线参与度偏低，突破需等待放量确认。")
                else:
                    turnover_tips.append(f"上一交易日换手率 {tr:.2f}%，接近20日均值({tr_ma20:.2f}%)，量能中性。")
            else:
                turnover_tips.append("未获取到换手率数据，量能研判以成交量比对为准。")

            # 风险提醒
            risks = []
            if atr_pct >= 6.0:
                risks.append("短期波动率偏高，严格控制仓位与止损，避免追高。")
            if rsi_val >= 75:
                risks.append("RSI 超买区域，出现长上影/放量滞涨需及时止盈。")
            if ma_trend == '空头排列':
                risks.append("中期趋势偏弱，反抽未站稳 MA20/MA50 前不宜重仓。")
            if vol_ratio > 2.0 and macd_sig != '金叉向上':
                risks.append("放量但动能未同步转强，警惕冲高回落。")
            if not risks:
                risks.append("常规市场风险：宏观政策、行业监管、黑天鹅事件等，建议分散持仓并设置风控阈值。")

            strategy = f"""## 🎯 投资策略建议

**投资建议：{recommendation}**

根据综合分析，执行要点如下：

### 📅 下一交易日操作
- {next_day_plan[0]}

### 🛡️ 风控与仓位管理
- 初始止损：{sl_pct*100:.1f}%（以入场价为基准），触发即退出，避免亏损扩大。
- 分批止盈：第一目标 {tp_pct*100:.1f}%，第二目标 {(tp_pct*100*1.5):.1f}%；到价分批落袋。
- 动态跟踪止损：若盈利超过第一目标，将止损上移至成本价上方 1%-2%。

### 🔁 换手率与量能提醒
- {turnover_tips[0]}
- 关注量比与成交额变化，突破关键均线（如 MA20/MA50）时需放量配合方可确认有效性。

### ⚠️ 风险提醒
- {risks[0]}
"""
            analysis_sections.append(strategy)
            
            return "\n\n".join(analysis_sections)
            
        except Exception as e:
            self.logger.error(f"高级规则分析失败: {e}")
            return "分析系统暂时不可用，请稍后重试。"

    def set_streaming_config(self, enabled=True, show_thinking=True):
        """设置流式推理配置"""
        self.streaming_config.update({
            'enabled': enabled,
            'show_thinking': show_thinking
        })

    def analyze_stock(self, stock_code, enable_streaming=None):
        """分析股票的主方法（增强版）"""
        if enable_streaming is None:
            enable_streaming = self.streaming_config.get('enabled', False)
        
        try:
            self.logger.info(f"开始增强版股票分析: {stock_code}")
            
            # 获取股票名称
            stock_name = self.get_stock_name(stock_code)
            
            # 1. 获取价格数据和技术分析
            self.logger.info("正在进行技术分析...")
            price_data = self.get_stock_data(stock_code)
            if price_data.empty:
                raise ValueError(f"无法获取股票 {stock_code} 的价格数据")
            
            price_info = self.get_price_info(price_data)
            technical_analysis = self.calculate_technical_indicators(price_data)
            technical_score = self.calculate_technical_score(technical_analysis)
            
            # 2. 获取25项财务指标和综合基本面分析
            self.logger.info("正在进行25项财务指标分析...")
            fundamental_data = self.get_comprehensive_fundamental_data(stock_code)
            fundamental_score = self.calculate_fundamental_score(fundamental_data)
            
            # 3. 获取综合新闻数据和高级情绪分析
            self.logger.info("正在进行综合新闻和情绪分析...")
            comprehensive_news_data = self.get_comprehensive_news_data(stock_code, days=30)
            sentiment_analysis = self.calculate_advanced_sentiment_analysis(comprehensive_news_data)
            sentiment_score = self.calculate_sentiment_score(sentiment_analysis)
            
            # 合并新闻数据到情绪分析结果中，方便AI分析使用
            sentiment_analysis.update(comprehensive_news_data)
            
            # 4. 计算综合得分
            scores = {
                'technical': technical_score,
                'fundamental': fundamental_score,
                'sentiment': sentiment_score,
                'comprehensive': self.calculate_comprehensive_score({
                    'technical': technical_score,
                    'fundamental': fundamental_score,
                    'sentiment': sentiment_score
                })
            }
            
            # 5. 生成投资建议
            recommendation = self.generate_recommendation(scores)
            
            # 6. AI增强分析（包含所有详细数据）
            ai_analysis = self.generate_ai_analysis({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'price_info': price_info,
                'technical_analysis': technical_analysis,
                'fundamental_data': fundamental_data,
                'sentiment_analysis': sentiment_analysis,
                'scores': scores
            }, enable_streaming)
            
            # 7. 生成最终报告
            report = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'price_info': price_info,
                'technical_analysis': technical_analysis,
                'fundamental_data': fundamental_data,
                'comprehensive_news_data': comprehensive_news_data,
                'sentiment_analysis': sentiment_analysis,
                'scores': scores,
                'analysis_weights': self.analysis_weights,
                'recommendation': recommendation,
                'ai_analysis': ai_analysis,
                'data_quality': {
                    'financial_indicators_count': len(fundamental_data.get('financial_indicators', {})),
                    'total_news_count': sentiment_analysis.get('total_analyzed', 0),
                    'sentiment_confidence': sentiment_analysis.get('confidence_score', 0.0),
                    'analysis_completeness': '完整' if len(fundamental_data.get('financial_indicators', {})) >= 15 else '部分'
                }
            }

            # 保存最近一次数据质量供动态权重使用
            try:
                self._last_data_quality = {
                    'financial_indicators_count': report['data_quality']['financial_indicators_count'],
                    'total_news_count': report['data_quality']['total_news_count'],
                    'sentiment_confidence': report['data_quality']['sentiment_confidence']
                }
            except Exception:
                self._last_data_quality = {}
            
            self.logger.info(f"✓ 增强版股票分析完成: {stock_code}")
            self.logger.info(f"  - 财务指标: {len(fundamental_data.get('financial_indicators', {}))} 项")
            self.logger.info(f"  - 新闻数据: {sentiment_analysis.get('total_analyzed', 0)} 条")
            self.logger.info(f"  - 综合得分: {scores['comprehensive']:.1f}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"增强版股票分析失败 {stock_code}: {str(e)}")
            raise

    # 兼容旧版本的方法名
    def get_fundamental_data(self, stock_code):
        """兼容方法：获取基本面数据"""
        return self.get_comprehensive_fundamental_data(stock_code)
    
    def get_news_data(self, stock_code, days=30):
        """兼容方法：获取新闻数据"""
        return self.get_comprehensive_news_data(stock_code, days)
    
    def calculate_news_sentiment(self, news_data):
        """兼容方法：计算新闻情绪"""
        return self.calculate_advanced_sentiment_analysis(news_data)
    
    def get_sentiment_analysis(self, stock_code):
        """兼容方法：获取情绪分析"""
        news_data = self.get_comprehensive_news_data(stock_code)
        return self.calculate_advanced_sentiment_analysis(news_data)


def main():
    """主函数"""
    analyzer = EnhancedStockAnalyzer()
    
    # 测试分析
    test_stocks = ['000001', '600036', '300019', '000525']
    
    for stock_code in test_stocks:
        try:
            print(f"\n=== 开始增强版分析 {stock_code} ===")
            report = analyzer.analyze_stock(stock_code)
            
            print(f"股票代码: {report['stock_code']}")
            print(f"股票名称: {report['stock_name']}")
            print(f"当前价格: {report['price_info']['current_price']:.2f}元")
            print(f"涨跌幅: {report['price_info']['price_change']:.2f}%")
            print(f"财务指标数量: {report['data_quality']['financial_indicators_count']}")
            print(f"新闻数据量: {report['data_quality']['total_news_count']}")
            print(f"综合得分: {report['scores']['comprehensive']:.1f}")
            print(f"投资建议: {report['recommendation']}")
            print("=" * 60)
            
        except Exception as e:
            print(f"分析 {stock_code} 失败: {e}")


if __name__ == "__main__":
    main()
