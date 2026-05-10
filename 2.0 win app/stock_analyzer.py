"""
增强版现代股票分析系统
支持25项财务指标、详细新闻分析、技术分析、情绪分析和AI增强分析
数据源：BaoStock + akshare（备用）
"""

import os
import sys
import logging
import warnings

# ── 绕过系统代理 ──────────────────────────────────────────────────────────────
# macOS 可能配置了本地代理（如 Clash/V2Ray），若代理未运行会导致所有请求失败。
# requests 会通过 urllib.request.getproxies() 读取 macOS 系统级代理（即使环境变量未设置），
# 此处用 monkey-patch 强制让所有 Session 不使用代理。
for _proxy_key in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
                   'all_proxy', 'ALL_PROXY'):
    os.environ.pop(_proxy_key, None)
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

try:
    import requests as _requests
    _orig_merge_env = _requests.Session.merge_environment_settings
    def _no_proxy_settings(self, url, proxies, stream, verify, cert):
        settings = _orig_merge_env(self, url, proxies, stream, verify, cert)
        settings['proxies'] = {}   # 强制清空代理
        return settings
    _requests.Session.merge_environment_settings = _no_proxy_settings
except Exception:
    pass  # requests 未安装时跳过
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import re
import concurrent.futures

# BaoStock数据接口
import baostock as bs

def check_market_status():
    """检查A股市场开盘状态"""
    now = datetime.now()
    weekday = now.weekday()  # 0=周一, 6=周日
    hour = now.hour
    minute = now.minute
    current_time = hour * 100 + minute  # 转换为HHMM格式便于比较
    
    # 检查是否为交易日（周一到周五）
    if weekday >= 5:  # 周六周日
        return False, "市场休市（周末）"
    
    # A股交易时间：
    # 上午：9:30-11:30
    # 下午：13:00-15:00
    if (930 <= current_time <= 1130) or (1300 <= current_time <= 1500):
        return True, "市场开盘中"
    elif current_time < 930:
        return False, "市场未开盘（早盘前）"
    elif 1130 < current_time < 1300:
        return False, "市场休市（午休）"
    else:  # current_time > 1500
        return False, "市场收盘"

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
    def _get_baostock_snapshot(self) -> pd.DataFrame:
        """【已替换】原BaoStock快照 → 改用腾讯接口批量拉取"""
        return self._get_snapshot_from_tencent()

    def _get_snapshot_from_tencent(self) -> pd.DataFrame:
        """
        主快照接口：腾讯行情 (qt.gtimg.cn)
        1. 用新浪分页API获取全量A股代码+名称+PE+市值
        2. 用腾讯批量接口补充实时价格/涨跌幅
        返回列：代码, 名称, 最新价, 涨跌幅, 市盈率-动态, 总市值, 换手率, 流通市值
        """
        import urllib.request, json, time

        # ── Step1：新浪分页获取全量A股（含PE、市值）──────────────────────────
        self.logger.info("📊 [腾讯快照] 开始通过新浪API获取全量A股列表...")
        all_stocks = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'http://finance.sina.com.cn'
        }
        page = 1
        while True:
            url = (f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                   f"Market_Center.getHQNodeData?page={page}&num=100&sort=symbol"
                   f"&asc=1&node=hs_a&_s_r_a=page")
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=8) as r:
                    batch = json.loads(r.read().decode('gbk', 'ignore'))
                if not batch:
                    break
                all_stocks.extend(batch)
                if page % 10 == 0:
                    self.logger.info(f"  已获取 {len(all_stocks)} 只...")
                page += 1
            except Exception as e:
                self.logger.warning(f"[腾讯快照] 新浪第{page}页失败: {e}")
                break

        if not all_stocks:
            self.logger.warning("[腾讯快照] 新浪API未返回数据，降级到纯腾讯接口")
            return pd.DataFrame()

        self.logger.info(f"[腾讯快照] 新浪API获取到 {len(all_stocks)} 只A股")

        # ── Step2：构建基础DataFrame ───────────────────────────────────────────
        rows = []
        for s in all_stocks:
            try:
                symbol = str(s.get('symbol', ''))  # e.g. sh600000
                code   = str(s.get('code', ''))    # e.g. 600000
                if len(code) != 6 or not code.isdigit():
                    continue
                price  = float(s.get('trade', 0) or 0)
                chg    = float(s.get('changepercent', 0) or 0)
                pe     = float(s.get('per', 0) or 0)
                mktcap_wan = float(s.get('mktcap', 0) or 0)   # 万元
                nmc_wan    = float(s.get('nmc', 0) or 0)       # 流通市值万元
                turnover   = float(s.get('turnoverratio', 0) or 0)
                rows.append({
                    '代码':      code,
                    '名称':      s.get('name', ''),
                    '最新价':    price,
                    '涨跌幅':    chg,
                    '市盈率-动态': pe,
                    '总市值':    round(mktcap_wan / 10000, 2),   # 转亿元
                    '流通市值':  round(nmc_wan / 10000, 2),
                    '换手率':    turnover,
                    '_symbol':   symbol,
                })
            except Exception:
                continue

        df = pd.DataFrame(rows)
        if df.empty:
            self.logger.warning("[腾讯快照] 解析后为空")
            return pd.DataFrame()

        # 过滤无效行
        df = df[(df['最新价'] > 0) & (df['市盈率-动态'] > 0) & (df['市盈率-动态'] < 1200)]
        self.logger.info(f"[腾讯快照] 有效股票: {len(df)} 只，保存缓存...")

        # ── Step3：用腾讯接口二次校准实时价格（可选，默认跳过以提速）────────
        # 若需要腾讯实时价格可取消注释，但会增加约30秒耗时
        # df = self._patch_price_from_tencent(df)

        return df.drop(columns=['_symbol'], errors='ignore').reset_index(drop=True)

    def _patch_price_from_tencent(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """用腾讯批量接口校准价格（每批100只，按需调用）"""
        import urllib.request
        codes = df['代码'].tolist()
        price_map = {}
        batch_size = 100
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i+batch_size]
            tencent_codes = []
            for c in batch:
                prefix = 'sh' if c.startswith(('6', '9')) else 'sz'
                tencent_codes.append(f"{prefix}{c}")
            url = 'http://qt.gtimg.cn/q=' + ','.join(tencent_codes)
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=5) as r:
                    text = r.read().decode('gbk', 'ignore')
                for line in text.strip().split('\n'):
                    parts = line.split('~')
                    if len(parts) > 32:
                        code = parts[2]
                        price = float(parts[3]) if parts[3] else 0
                        if price > 0:
                            price_map[code] = price
            except Exception:
                pass
        if price_map:
            df['最新价'] = df['代码'].map(lambda c: price_map.get(c, df.loc[df['代码']==c, '最新价'].values[0] if len(df[df['代码']==c])>0 else 0))
        return df

    # ─── 原BaoStock快照（旧版，已停用）─────────────────────────────────────────
    def _get_baostock_snapshot_legacy(self) -> pd.DataFrame:
        """[已停用] 原BaoStock逐股拉取快照，速度极慢，保留备查"""
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != '0':
                self.logger.warning(f"BaoStock登录失败: {lg.error_msg}")
                return pd.DataFrame()
            
            # 获取最近交易日（向前推5天，确保能找到交易日）
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            rs = bs.query_stock_basic()
            stocks = []
            while (rs.error_code == '0') & rs.next():
                stocks.append(rs.get_row_data())
            df_basic = pd.DataFrame(stocks, columns=rs.fields)
            
            print(f"[BaoStock快照] 获取到 {len(df_basic)} 只股票基础信息")
            
            # 获取最新行情（取最近5天内的最后一条数据）
            df_list = []
            for i, row in df_basic.iterrows():
                code = row['code']
                name = row['code_name']
                
                if i < 3 or i % 500 == 0:  # 只打印前3条和每500条
                    print(f"[BaoStock快照] 进度: {i}/{len(df_basic)} - {code} {name}")
                
                # 获取K线数据（含市值）
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,close,pctChg,peTTM,pbMRQ,isST,tradestatus",
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    frequency="d"
                )
                rows = []
                while (rs.error_code == '0') & rs.next():
                    rows.append(rs.get_row_data())
                
                if rows:
                    # 只取最后一条（最新交易日）
                    last_row = rows[-1]
                    row_dict = dict(zip(rs.fields, last_row))
                    row_dict['name'] = name  # 添加名称
                    
                    # 打印前几条样本数据
                    if i < 3:
                        print(f"[BaoStock快照] {code} 最新数据: {last_row}")
                    
                    df_list.append(row_dict)
            
            bs.logout()
            
            if df_list:
                df = pd.DataFrame(df_list)
                print(f"[BaoStock快照] 原始获取到 {len(df)} 只股票行情数据")
                
                # 清理代码格式（去掉 sh./sz. 前缀）
                df['code'] = df['code'].apply(lambda x: str(x).split('.')[-1] if '.' in str(x) else str(x))
                
                # 过滤掉指数、债券等非股票代码（只保留6位纯数字代码）
                df = df[df['code'].str.len() == 6]
                df = df[df['code'].str.isdigit()]
                
                # 过滤掉 PE 为 0 或空的数据（这些通常是指数或无效数据）
                df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
                df = df[df['peTTM'] > 0]
                df = df[df['peTTM'] < 1200]  # 过滤异常PE
                
                print(f"[BaoStock快照] 过滤后剩余 {len(df)} 只有效股票")
                
                if df.empty:
                    self.logger.warning("BaoStock快照过滤后为空")
                    return pd.DataFrame()
                
                print(f"[BaoStock快照] 字段: {list(df.columns)}")
                print(f"[BaoStock快照] 前3条有效数据样本:")
                print(df.head(3))
                
                # 字段标准化
                df.rename(columns={
                    'code': '代码',
                    'name': '名称',
                    'close': '最新价',
                    'pctChg': '涨跌幅',
                    'peTTM': '市盈率TTM',
                    'pbMRQ': '市净率'
                }, inplace=True)
                
                # 确保数值列是 float 类型（BaoStock 返回的是字符串）
                for col in ['最新价', '涨跌幅', '市盈率TTM', '市净率']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 估算市值（BaoStock 无市值字段，基于 PE 和股价估算）
                print("[BaoStock快照] 基于 PE 和股价估算市值...")
                
                def estimate_market_cap(row):
                    """
                    基于 PE、PB、股价估算市值
                    
                    逻辑：
                    1. PE = 市值 / 净利润，假设不同PE区间对应不同规模公司
                    2. 结合股价辅助判断
                    3. 参考行业平均值
                    """
                    try:
                        pe = float(row['市盈率TTM'])
                        price = float(row['最新价'])
                        code = str(row['代码'])
                        
                        # 根据代码判断板块（不同板块市值特征不同）
                        if code.startswith('688'):  # 科创板
                            base_cap = 80.0
                        elif code.startswith('300'):  # 创业板
                            base_cap = 70.0
                        elif code.startswith('002'):  # 中小板
                            base_cap = 60.0
                        elif code.startswith('60'):  # 主板（沪市）
                            base_cap = 150.0
                        elif code.startswith('00'):  # 主板（深市）
                            base_cap = 100.0
                        else:
                            base_cap = 100.0
                        
                        # PE 调整系数（PE越高，市值相对越大）
                        pe_factor = 1.0
                        if pe < 15:
                            pe_factor = 0.6  # 低PE可能是大盘蓝筹或夕阳行业
                        elif pe < 30:
                            pe_factor = 0.8
                        elif pe < 50:
                            pe_factor = 1.0
                        elif pe < 80:
                            pe_factor = 1.3
                        else:
                            pe_factor = 1.5  # 高PE可能是成长股
                        
                        # 股价调整（高价股倾向于大市值）
                        price_factor = 1.0
                        if price > 100:
                            price_factor = 2.0
                        elif price > 50:
                            price_factor = 1.5
                        elif price > 20:
                            price_factor = 1.2
                        elif price < 5:
                            price_factor = 0.7
                        
                        # 综合估算
                        estimated_cap = base_cap * pe_factor * price_factor
                        
                        # 限制在合理范围
                        return max(20.0, min(5000.0, estimated_cap))
                        
                    except:
                        return 100.0  # 默认值
                
                df['总市值(亿)'] = df.apply(estimate_market_cap, axis=1)
                print(f"[BaoStock快照] 市值估算完成，范围: {df['总市值(亿)'].min():.1f} - {df['总市值(亿)'].max():.1f} 亿")
                
                return df
            else:
                self.logger.warning("BaoStock行情数据为空")
                return pd.DataFrame()
        except Exception as e:
            import traceback
            self.logger.warning(f"BaoStock快照获取异常: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()
    """增强版综合股票分析器"""
    
    def __init__(self, config_file='config.json', weights: Optional[Dict[str, float]] = None, thresholds: Optional[Dict[str, float]] = None):
        """初始化分析器"""
        self.logger = logging.getLogger(__name__)
        self.config_file = config_file
        
        # ── 数据源初始化：腾讯为主，新浪为副，BaoStock/东财已停用 ──────────
        self.baostock_connected = False   # 不再使用BaoStock
        self.akshare_available  = False   # 不再使用AkShare/东财
        self.tencent_available  = True    # 腾讯接口（qt.gtimg.cn，HTTP无代理）
        self.sina_available     = True    # 新浪接口（新浪分页列表API）
        self.logger.info("✅ 数据源: 腾讯(主) + 新浪(副)，BaoStock/东财已停用")
        
        # 加载配置文件
        self.config = self._load_config()

        # 可选：网络代理设置（用于东财等网络访问不畅场景）
        try:
            net = self.config.get('network', {}) if isinstance(self.config, dict) else {}
            if net.get('enable_proxies'):
                http_p = net.get('http_proxy') or net.get('HTTP_PROXY')
                https_p = net.get('https_proxy') or net.get('HTTPS_PROXY')
                no_p = net.get('no_proxy') or net.get('NO_PROXY')
                if http_p:
                    os.environ['HTTP_PROXY'] = str(http_p)
                if https_p:
                    os.environ['HTTPS_PROXY'] = str(https_p)
                if no_p:
                    os.environ['NO_PROXY'] = str(no_p)
                self.logger.info("✓ 已应用网络代理配置到环境变量")
            # 读取资金流接口稳健性配置
            try:
                self.fund_flow_timeout_seconds = float(net.get('fund_flow_timeout_seconds', 3.0))
            except Exception:
                self.fund_flow_timeout_seconds = 3.0
            try:
                self.fund_flow_retries = int(net.get('fund_flow_retries', 1))
            except Exception:
                self.fund_flow_retries = 1
            # 是否跳过资金流数据（避免超时卡顿）
            self.skip_fund_flow = net.get('skip_fund_flow', False)
            # 数据源偏好配置
            self.prefer_baostock = net.get('prefer_baostock', True)
            self.disable_akshare_on_failure = net.get('disable_akshare_on_failure', True)
            self.akshare_available = True  # 初始假设可用，后续测试后更新
        except Exception as e:
            self.logger.warning(f"代理配置应用失败: {e}")
            self.prefer_baostock = True
            self.disable_akshare_on_failure = True
            self.akshare_available = True
        
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

    # =============================
    # 市场与代码辅助
    # =============================
    def _get_trading_dates(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """获取指定日期范围内的A股交易日列表
        
        使用简单规则过滤：排除周末（周六日），暂不处理节假日（因K线数据本身已是交易日）
        实际使用中，从数据源获取的K线数据已经是交易日，此方法主要用于验证和说明
        """
        try:
            trading_dates = []
            current_date = start_date
            while current_date <= end_date:
                # 排除周末（0=周一, 6=周日）
                if current_date.weekday() < 5:  # 周一到周五
                    trading_dates.append(current_date)
                current_date += timedelta(days=1)
            return trading_dates
        except Exception as e:
            self.logger.warning(f"获取交易日历失败: {e}")
            return []

    def _format_code_for_eastmoney(self, stock_code: str) -> str:
        """格式化股票代码为Eastmoney需要的前缀格式，如 sh600000 / sz000001 / bj8xxxxx"""
        try:
            s = str(stock_code)
            if s.startswith(('60', '68')):
                return f"sh{s}"
            if s.startswith(('00', '30', '20')):
                return f"sz{s}"
            if s.startswith(('83', '87', '43')):
                return f"bj{s}"
            # 兜底：大概率深市
            return f"sz{s}"
        except Exception:
            return str(stock_code)

    def _detect_board(self, stock_code: str) -> str:
        """检测板块：Main/SME/ChiNext/STAR/Beijing"""
        s = str(stock_code)
        if s.startswith(('600', '601', '603', '605')):
            return 'Main'
        if s.startswith('002'):
            return 'SME'
        if s.startswith('300'):
            return 'ChiNext'
        if s.startswith('688'):
            return 'STAR'
        if s.startswith(('83', '87', '43')):
            return 'Beijing'
        return 'Other'

    def _estimate_market_cap(self, fundamental_data: dict) -> Optional[float]:
        """从已获取的估值信息估算总市值（元）。若不可用返回None"""
        try:
            val = (fundamental_data or {}).get('valuation') or {}
            m = val.get('总市值')
            if m is None:
                # 有些接口返回单位亿
                m = val.get('总市值(亿)')
                if m is not None:
                    return float(m) * 1e8
            return float(m) if m is not None else None
        except Exception:
            return None

    def _call_with_timeout(self, func, timeout_seconds: float, *args, **kwargs):
        """在线数据调用的超时保护，避免第三方库在网络不畅时长时间阻塞。

        返回：
        - 正常：func 的返回值
        - 超时：特殊对象 TimeoutError('timeout')
        - 异常：原始异常对象
        """
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        fut = executor.submit(func, *args, **kwargs)
        try:
            return fut.result(timeout=max(0.5, float(timeout_seconds)))
        except concurrent.futures.TimeoutError:
            try:
                self.logger.info(f"调用超时({timeout_seconds}s): {getattr(func, '__name__', 'func')}")
            except Exception:
                pass
            return TimeoutError("timeout")
        except Exception as e:
            return e
        finally:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    # =============================
    # 资金流向（近1个月）
    # =============================
    def _calculate_money_flow_from_kline(self, stock_code: str, window_days: int = 30) -> dict:
        """
        从K线数据自计算资金流（备用方案）
        当API失败时使用
        """
        try:
            from money_flow_calculator import MoneyFlowCalculator
            
            # 获取K线数据
            df = self.get_stock_data(stock_code)
            if df is None or df.empty:
                return {
                    'window_days': window_days,
                    'buckets': {},
                    'main_force_net': None,
                    'main_force_ratio_to_turnover': None,
                    'status': '数据不足',
                    'note': 'K线数据不可用',
                    'source': 'none'
                }
            
            # 使用计算器
            calculator = MoneyFlowCalculator()
            flow_result = calculator.calculate_money_flow(df, window_days)
            
            # 转换为系统格式
            buckets = {}
            for key, name in [('super_large', '超大单'), ('large', '大单'), 
                             ('medium', '中单'), ('small', '小单')]:
                detail = flow_result['flow_details'][key]
                buckets[name] = {
                    'net': detail['net'],
                    'ratio': detail['ratio'],
                    'positive_days': detail['positive_days'],
                    'negative_days': detail['negative_days'],
                    'status': detail['status']
                }
            
            result = {
                'window_days': window_days,
                'buckets': buckets,
                'main_force_net': flow_result['main_net_inflow'],
                'main_force_ratio_to_turnover': flow_result['main_net_inflow_ratio'],
                'status': flow_result['main_status'],
                'sum_amount': flow_result['total_amount'],
                'note': '基于K线数据自计算',
                'source': 'calculated'
            }
            
            self.logger.info(f"✓ 自计算资金流完成: {stock_code}, 主力净流入 {flow_result['main_net_inflow']/1e8:.2f}亿")
            return result
            
        except Exception as e:
            self.logger.error(f"自计算资金流失败: {e}")
            return {
                'window_days': window_days,
                'buckets': {},
                'main_force_net': None,
                'main_force_ratio_to_turnover': None,
                'status': '计算失败',
                'note': f'自计算失败: {str(e)[:60]}',
                'source': 'none'
            }
    
    def calculate_capital_flow(self, stock_code: str, price_data: pd.DataFrame, fundamental_data: Optional[dict] = None, window_days: int = 30) -> dict:
        """
        计算近1个月（按交易日回溯约30天）分档资金流向：超大/大/中/小单净流额合计，主力净流入等。

        返回结构示例：
        {
          'window_days': 30,
          'sum_amount': <总成交额>,
          'buckets': {
             'extra_large': {'net': x, 'pos_days': n1, 'neg_days': n2, 'ratio_to_turnover': r1, 'status': '强劲流入/...' },
             'large': {...},
             'medium': {...},
             'small': {...}
          },
          'main_force_net': <超大+大净额>,
          'main_force_ratio_to_turnover': <占总成交额比例>,
          'status': '主力净流入强/中/弱/净流出',
          'note': '单位与口径说明'
        }
        """
        result = {
            'window_days': int(window_days),
            'buckets': {},
            'main_force_net': None,
            'main_force_ratio_to_turnover': None,
            'status': '数据不足',
            'sum_amount': None,
            'note': '',
            'source': ''
        }

        try:
            import akshare as ak
        except Exception as e:
            self.logger.info(f"资金流接口不可用(akshare未安装): {e}")
            result['note'] = 'akshare未安装'
            return result

        # 获取资金流，带重试
        df = None
        em_code = self._format_code_for_eastmoney(stock_code)
        last_err = None
        # 检查是否跳过资金流
        if getattr(self, 'skip_fund_flow', False):
            self.logger.debug(f"已配置跳过资金流数据获取: {em_code}")
            result['note'] = '已跳过资金流数据（配置）'
            return result
        
        retries = max(1, int(getattr(self, 'fund_flow_retries', 1)))
        timeout_s = max(1.0, float(getattr(self, 'fund_flow_timeout_seconds', 3.0)))
        for i in range(retries):
            try:
                self.logger.info(f"正在获取近月资金流: {em_code}")
                # 为避免网络阻塞，增加调用超时保护
                res = self._call_with_timeout(ak.stock_individual_fund_flow, timeout_s, stock=em_code)
                if isinstance(res, TimeoutError):
                    last_err = res
                elif isinstance(res, Exception):
                    last_err = res
                else:
                    df = res
                    if df is not None and not getattr(df, 'empty', True):
                        result['source'] = 'akshare_em'
                        break
                    last_err = RuntimeError("空数据")
            except Exception as e:
                last_err = e
            # 退避
            try:
                time.sleep(0.6 * (i + 1))
            except Exception:
                pass
        if df is None or (hasattr(df, 'empty') and df.empty):
            self.logger.info(f"获取资金流失败: {last_err}，尝试自计算模式")
            # 回退到自计算模式
            return self._calculate_money_flow_from_kline(stock_code, window_days)

        if df is None or df.empty:
            result['note'] = '无资金流数据'
            return result

        # 解析日期并截取窗口
        date_col = None
        for c in ['日期', 'date', df.columns[0]]:
            if c in df.columns:
                date_col = c
                break
        try:
            df['_dt'] = pd.to_datetime(df[date_col], errors='coerce')
        except Exception:
            df['_dt'] = pd.NaT
        cutoff = datetime.now() - timedelta(days=max(7, int(window_days)))
        df = df[df['_dt'] >= cutoff]
        if df.empty:
            result['note'] = '窗口内无数据'
            return result

        # 列名映射，兼容不同字段
        def pick(*cands):
            for c in cands:
                if c in df.columns:
                    return c
            return None

        cols = {
            'xd_net': pick('超大单净流入-净额', '超大单-净额', 'xl_net'),
            'xl_ratio': pick('超大单净流入-净占比', '超大单-净占比', 'xl_ratio'),
            'lg_net': pick('大单净流入-净额', '大单-净额', 'lg_net'),
            'lg_ratio': pick('大单净流入-净占比', '大单-净占比', 'lg_ratio'),
            'md_net': pick('中单净流入-净额', '中单-净额', 'md_net'),
            'md_ratio': pick('中单净流入-净占比', '中单-净占比', 'md_ratio'),
            'sm_net': pick('小单净流入-净额', '小单-净额', 'sm_net'),
            'sm_ratio': pick('小单净流入-净占比', '小单-净占比', 'sm_ratio'),
        }

        # 数值化
        for key, c in cols.items():
            if c and c in df.columns:
                try:
                    df[key] = pd.to_numeric(df[c], errors='coerce')
                except Exception:
                    df[key] = np.nan
            else:
                df[key] = np.nan

        # 计算窗口内合计净额与天数
        def agg_bucket(net_col: str):
            s = df[net_col].dropna()
            net = float(s.sum()) if len(s) else 0.0
            pos_days = int((s > 0).sum()) if len(s) else 0
            neg_days = int((s < 0).sum()) if len(s) else 0
            return net, pos_days, neg_days

        xd_net, xd_pos, xd_neg = agg_bucket('xd_net')
        lg_net, lg_pos, lg_neg = agg_bucket('lg_net')
        md_net, md_pos, md_neg = agg_bucket('md_net')
        sm_net, sm_pos, sm_neg = agg_bucket('sm_net')

        # 计算成交额基准（来自价格数据的 amount 列，BaoStock为交易额）
        try:
            if price_data is not None and not price_data.empty and 'amount' in price_data.columns:
                p = price_data.copy()
                p['_dt'] = pd.to_datetime(p.get('date', p.index), errors='coerce')
                p = p[p['_dt'] >= cutoff]
                sum_amount = float(pd.to_numeric(p['amount'], errors='coerce').dropna().sum()) if not p.empty else np.nan
            else:
                sum_amount = np.nan
        except Exception:
            sum_amount = np.nan

        result['sum_amount'] = None if not np.isfinite(sum_amount) else float(sum_amount)

        # 比例（对总成交额），若成交额不可用则仅返回净额
        def ratio(v):
            if np.isfinite(sum_amount) and sum_amount > 0:
                return float(v / sum_amount)
            return None

        result['buckets'] = {
            'extra_large': {
                'net': float(xd_net), 'pos_days': xd_pos, 'neg_days': xd_neg, 'ratio_to_turnover': ratio(xd_net)
            },
            'large': {
                'net': float(lg_net), 'pos_days': lg_pos, 'neg_days': lg_neg, 'ratio_to_turnover': ratio(lg_net)
            },
            'medium': {
                'net': float(md_net), 'pos_days': md_pos, 'neg_days': md_neg, 'ratio_to_turnover': ratio(md_net)
            },
            'small': {
                'net': float(sm_net), 'pos_days': sm_pos, 'neg_days': sm_neg, 'ratio_to_turnover': ratio(sm_net)
            }
        }

        # 主力（超大+大）
        main_net = float((xd_net if np.isfinite(xd_net) else 0.0) + (lg_net if np.isfinite(lg_net) else 0.0))
        result['main_force_net'] = main_net
        result['main_force_ratio_to_turnover'] = ratio(main_net)

        # 根据板块与体量动态阈值打分/评级
        board = self._detect_board(stock_code)
        # 基础阈值（占成交额比例）
        th_strong = 0.08
        th_mid = 0.03
        th_weak = 0.01
        # 板块缩放
        scale = 1.0
        if board in ('ChiNext', 'STAR'):
            scale = 0.7
        elif board == 'SME':
            scale = 0.8
        elif board == 'Beijing':
            scale = 0.6

        # 体量缩放（市值小的更容易达到强度门槛）
        try:
            mktcap = self._estimate_market_cap(fundamental_data or {})
            if mktcap is not None:
                if mktcap < 1e10:      # < 100亿
                    scale *= 0.7
                elif mktcap < 4e10:   # 100-400亿
                    scale *= 0.85
                elif mktcap > 2e11:   # > 2000亿
                    scale *= 1.15
        except Exception:
            pass

        th_strong *= scale
        th_mid *= scale
        th_weak *= scale

        # 天数占比
        total_days = int(len(df))
        pos_ratio_main = float((xd_pos + lg_pos) / total_days) if total_days > 0 else 0.0

        def label_status(ratio_val: Optional[float], pos_ratio: float) -> str:
            if ratio_val is None:
                # 用净额方向兜底
                r = main_net
                if r > 0:
                    return '净流入(无法估算强度)'
                elif r < 0:
                    return '净流出'
                else:
                    return '持平'
            if ratio_val >= th_strong and pos_ratio >= 0.6:
                return '主力净流入强'
            if ratio_val >= th_mid and pos_ratio >= 0.5:
                return '主力净流入中'
            if ratio_val >= th_weak and pos_ratio >= 0.4:
                return '主力净流入弱'
            if ratio_val <= -th_mid:
                return '主力净流出较强'
            if ratio_val < 0:
                return '主力净流出'
            return '主力资金趋于中性'

        result['status'] = label_status(result['main_force_ratio_to_turnover'], pos_ratio_main)

        # 单桶状态
        def bucket_label(v_net: float, v_ratio: Optional[float], pos_days: int) -> str:
            if v_ratio is None:
                return '净流入' if v_net > 0 else ('净流出' if v_net < 0 else '持平')
            if v_ratio >= th_strong * 0.6 and pos_days >= max(6, total_days // 3):
                return '强'
            if v_ratio >= th_mid * 0.6 and pos_days >= max(5, total_days // 4):
                return '中'
            if v_ratio >= th_weak * 0.5 and pos_days >= max(4, total_days // 5):
                return '弱'
            if v_ratio < 0:
                return '流出'
            return '中性'

        for k in ['extra_large', 'large', 'medium', 'small']:
            b = result['buckets'][k]
            b['status'] = bucket_label(b['net'], b['ratio_to_turnover'], b['pos_days'])

        result['note'] = '资金净额按东财口径，近约30自然日内的交易日累计；比例以同期成交额估算（若可用）'
        return result

        

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
            "network": {
                "snapshot_cache_ttl_hours": 4,
                "enable_proxies": False,
                "http_proxy": "",
                "https_proxy": "",
                "no_proxy": ""
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

    def _init_baostock(self):
        """初始化BaoStock连接"""
        try:
            # 登录BaoStock系统
            lg = bs.login()
            if lg.error_code != '0':
                self.logger.error(f"BaoStock登录失败: {lg.error_msg}")
                raise Exception(f"BaoStock登录失败: {lg.error_msg}")
            else:
                self.logger.info("✅ BaoStock连接成功")
                self.baostock_connected = True
        except Exception as e:
            self.logger.warning(f"⚠️ BaoStock连接失败，将使用akshare作为备用: {e}")
            self.baostock_connected = False

    def _format_stock_code_for_baostock(self, stock_code):
        """格式化股票代码为BaoStock格式"""
        # BaoStock需要带交易所前缀的格式，如sh.600000, sz.000001
        if stock_code.startswith(('60', '68', '90')):
            return f"sh.{stock_code}"
        elif stock_code.startswith(('00', '30', '20')):
            return f"sz.{stock_code}"
        else:
            # 默认认为是深交所
            return f"sz.{stock_code}"

    def _query_baostock_data(self, query_func, *args, **kwargs):
        """安全查询BaoStock数据的通用方法"""
        try:
            if not self.baostock_connected:
                raise Exception("BaoStock未连接")
            
            result = query_func(*args, **kwargs)
            if result.error_code != '0':
                raise Exception(f"查询失败: {result.error_msg}")
            
            # 转换为DataFrame
            data_list = []
            while (result.error_code == '0') & result.next():
                data_list.append(result.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            # 使用result的字段名作为列名
            columns = result.fields if hasattr(result, 'fields') else None
            df = pd.DataFrame(data_list, columns=columns)
            return df
            
        except Exception as e:
            self.logger.warning(f"BaoStock查询失败: {e}")
            return pd.DataFrame()

    def __del__(self):
        """析构函数：不再在此处调用全局 bs.logout()，避免影响其他实例或在用会话。"""
        try:
            # 仅记录，不主动登出（BaoStock为进程级会话，交由显式生命周期管理或进程结束）
            self.logger.debug("Analyzer 实例销毁")
        except Exception:
            pass

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

    # ===== 快照缓存辅助 =====
    def _snapshot_cache_path(self) -> str:
        try:
            cache_dir = os.path.dirname(os.path.abspath(self.config_file)) or os.getcwd()
        except Exception:
            cache_dir = os.getcwd()
        return os.path.join(cache_dir, 'spot_snapshot_cache.csv')

    def _snapshot_cache_ttl(self) -> timedelta:
        try:
            net = self.config.get('network', {}) if isinstance(self.config, dict) else {}
            hours = float(net.get('snapshot_cache_ttl_hours', 4))
            return timedelta(hours=max(0.5, min(24.0, hours)))
        except Exception:
            return timedelta(hours=4)

    def _load_spot_snapshot_cache(self) -> pd.DataFrame:
        path = self._snapshot_cache_path()
        try:
            if not os.path.exists(path):
                return pd.DataFrame()
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if datetime.now() - mtime > self._snapshot_cache_ttl():
                return pd.DataFrame()
            df = pd.read_csv(path)
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _save_spot_snapshot_cache(self, df: pd.DataFrame):
        try:
            if df is None or df.empty:
                return
            path = self._snapshot_cache_path()
            # 仅保留常用列，减小体积
            keep_cols = [c for c in df.columns if c in ['代码','名称','涨跌幅','最新价','总市值','总市值(亿)','市盈率','市盈率-动态','市盈率TTM','市净率','股息率TTM(%)','股息率TTM','股息率']]
            df_to_save = df[keep_cols] if keep_cols else df
            df_to_save.to_csv(path, index=False)
            self.logger.info("✓ 估值快照已写入本地缓存")
        except Exception as e:
            self.logger.debug(f"写入快照缓存失败: {e}")

    # ===== 行业成份缓存辅助 =====
    def _industry_cache_dir(self) -> str:
        try:
            base = os.path.dirname(os.path.abspath(self.config_file)) or os.getcwd()
        except Exception:
            base = os.getcwd()
        path = os.path.join(base, 'industry_cons_cache')
        os.makedirs(path, exist_ok=True)
        return path

    def _industry_cons_cache_path(self, industry_name: str) -> str:
        safe = re.sub(r"[^\w\u4e00-\u9fa5]+", "_", str(industry_name or 'unknown'))[:40]
        return os.path.join(self._industry_cache_dir(), f"{safe}.csv")

    def _industry_cons_cache_ttl(self) -> timedelta:
        try:
            net = self.config.get('network', {}) if isinstance(self.config, dict) else {}
            days = float(net.get('industry_cache_ttl_days', 7))
            return timedelta(days=max(1.0, min(30.0, days)))
        except Exception:
            return timedelta(days=7)

    def _load_industry_cons_cache(self, industry_name: str) -> pd.DataFrame:
        path = self._industry_cons_cache_path(industry_name)
        try:
            if not os.path.exists(path):
                return pd.DataFrame()
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if datetime.now() - mtime > self._industry_cons_cache_ttl():
                return pd.DataFrame()
            df = pd.read_csv(path)
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _save_industry_cons_cache(self, industry_name: str, df: pd.DataFrame):
        try:
            if df is None or df.empty:
                return
            path = self._industry_cons_cache_path(industry_name)
            # 仅保留常见列
            keep = [c for c in df.columns if c in ['代码','名称','股票代码','证券代码','股票简称','名称_x','名称_y']]
            df_to_save = df[keep] if keep else df
            df_to_save.to_csv(path, index=False)
            self.logger.info(f"✓ 行业成份缓存已写入: {os.path.basename(path)}")
        except Exception as e:
            self.logger.debug(f"写入行业缓存失败: {e}")

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
        """
        获取股票历史K线 - 腾讯接口(主) + 新浪接口(副)，BaoStock/AkShare已停用
        返回标准化 DataFrame：date, open, high, low, close, volume, amount,
                               change_pct, change_amount, turnover_rate, amplitude
        """
        # ── 缓存命中 ─────────────────────────────────────────────────────────
        if stock_code in self.price_cache:
            cache_time, data = self.price_cache[stock_code]
            if datetime.now() - cache_time < self.cache_duration:
                return data

        days = self.analysis_params.get('technical_period_days', 365)
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days + 60)   # 多取60天保证够用

        # ── 主接口：腾讯前复权日K线 ──────────────────────────────────────────
        try:
            data = self._get_kline_from_tencent(stock_code, start_dt, end_dt)
            if data is not None and not data.empty:
                self.price_cache[stock_code] = (datetime.now(), data)
                self.logger.info(f"✓ 腾讯K线 {stock_code}：{len(data)} 条")
                return data
        except Exception as e:
            self.logger.warning(f"腾讯K线失败({stock_code}): {e}")

        # ── 备用接口：新浪日K线 ───────────────────────────────────────────────
        try:
            data = self._get_kline_from_sina(stock_code, start_dt, end_dt)
            if data is not None and not data.empty:
                self.price_cache[stock_code] = (datetime.now(), data)
                self.logger.info(f"✓ 新浪K线 {stock_code}：{len(data)} 条")
                return data
        except Exception as e:
            self.logger.warning(f"新浪K线失败({stock_code}): {e}")

        raise Exception(f"腾讯/新浪均无法获取 {stock_code} 历史K线")

    def _tencent_code(self, stock_code: str) -> str:
        """将6位代码转为腾讯格式，如 600000→sh600000，000001→sz000001"""
        code = str(stock_code).strip()
        if code.startswith(('6', '9')):
            return f"sh{code}"
        return f"sz{code}"

    def _get_kline_from_tencent(self, stock_code: str, start_dt, end_dt) -> pd.DataFrame:
        """
        腾讯 JSONP 前复权日K线
        URL: https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600000,day,start,end,640,qfq
        """
        import urllib.request, json
        tc = self._tencent_code(stock_code)
        s  = start_dt.strftime('%Y-%m-%d')
        e  = end_dt.strftime('%Y-%m-%d')
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               f"?_var=kline_dayqfq&param={tc},day,{s},{e},640,qfq&r=0.1")
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://gu.qq.com/'
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            text = r.read().decode('utf-8', 'ignore')
        # 剥离 JSONP 包装
        if text.startswith('kline_dayqfq='):
            text = text[len('kline_dayqfq='):]
        obj = json.loads(text)
        # 数据路径：data → tc → qfqday / day
        tc_data = obj.get('data', {}).get(tc, {})
        klines = tc_data.get('qfqday') or tc_data.get('day') or []
        if not klines:
            return pd.DataFrame()
        # 每条：[日期, 开, 收, 高, 低, 成交量]  注意：腾讯顺序是 开收高低量
        rows = []
        for k in klines:
            try:
                rows.append({
                    'date':   pd.to_datetime(k[0]),
                    'open':   float(k[1]),
                    'close':  float(k[2]),
                    'high':   float(k[3]),
                    'low':    float(k[4]),
                    'volume': float(k[5]) * 100,   # 手→股
                    'amount': 0.0,
                })
            except Exception:
                continue
        df = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
        return self._standardize_kline(df, stock_code)

    def _get_kline_from_sina(self, stock_code: str, start_dt, end_dt) -> pd.DataFrame:
        """
        新浪日K线（前复权）
        URL: https://finance.sina.com.cn/realcharts/vline/sh600000.js
        """
        import urllib.request, json
        tc = self._tencent_code(stock_code)  # sh600000 / sz000001
        url = f"https://finance.sina.com.cn/realcharts/vline/{tc}.js?type=daily&_={int(datetime.now().timestamp())}"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer':    'https://finance.sina.com.cn'
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            text = r.read().decode('utf-8', 'ignore')
        # 新浪返回 JSON 数组：[[日期, 开, 高, 低, 收, 成交量, 成交额], ...]
        # 或带变量名的 JSONP，剥离前缀
        idx = text.find('[')
        if idx == -1:
            return pd.DataFrame()
        text = text[idx:]
        end_idx = text.rfind(']')
        klines = json.loads(text[:end_idx+1])
        rows = []
        for k in klines:
            try:
                dt = pd.to_datetime(str(k[0]))
                if dt < start_dt or dt > end_dt:
                    continue
                rows.append({
                    'date':   dt,
                    'open':   float(k[1]),
                    'high':   float(k[2]),
                    'low':    float(k[3]),
                    'close':  float(k[4]),
                    'volume': float(k[5]) * 100,
                    'amount': float(k[6]) if len(k) > 6 else 0.0,
                })
            except Exception:
                continue
        df = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
        return self._standardize_kline(df, stock_code)

    def _standardize_kline(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """统一K线字段：补全 preclose/change_pct/turnover_rate/amplitude 等"""
        if df.empty:
            return df
        df = df.copy()
        df['preclose'] = df['close'].shift(1)
        df.loc[df.index[0], 'preclose'] = df['open'].iloc[0]
        df['change_amount'] = df['close'] - df['preclose']
        df['change_pct']    = (df['change_amount'] / df['preclose'] * 100).round(4)
        df['amplitude']     = ((df['high'] - df['low']) / df['preclose'] * 100).round(4)
        if 'amount' not in df.columns or df['amount'].sum() == 0:
            df['amount'] = df['close'] * df['volume']
        df['turnover']      = df['amount']
        df['turnover_rate'] = 0.0   # 无流通股本数据时设0
        df['volume_ratio']  = 1.0
        df['code']          = stock_code
        df = df.dropna(subset=['close', 'open']).reset_index(drop=True)
        return df

    def _get_stock_data_from_akshare(self, stock_code, period='1y'):
        """[已停用] 原AkShare/东财历史K线，保留方法避免调用报错，直接转发到腾讯接口"""
        days = self.analysis_params.get('technical_period_days', 365)
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days + 60)
        return self._get_kline_from_tencent(stock_code, start_dt, end_dt)

    def _preprocess_baostock_data(self, stock_data, stock_code):
        """[已停用] BaoStock数据预处理，兼容性保留"""
        return self._standardize_kline(stock_data, stock_code)

    def get_comprehensive_fundamental_data(self, stock_code):
        """获取25项综合财务指标数据 - 优先使用BaoStock"""
        if stock_code in self.fundamental_cache:
            cache_time, data = self.fundamental_cache[stock_code]
            if datetime.now() - cache_time < self.fundamental_cache_duration:
                self.logger.info(f"使用缓存的基本面数据: {stock_code}")
                return data
        
        # 首先尝试使用BaoStock获取部分数据
        fundamental_data = {}
        
        # BaoStock主要用于获取基本面概览
        if self.baostock_connected:
            try:
                fundamental_data.update(self._get_fundamental_data_from_baostock(stock_code))
            except Exception as e:
                self.logger.warning(f"BaoStock获取基本面数据失败: {e}")
        
        # 使用akshare补充详细的财务指标（BaoStock的财务数据相对有限）
        try:
            akshare_data = self._get_fundamental_data_from_akshare(stock_code)
            # 合并数据，akshare的数据优先级更高（更详细）
            for key, value in akshare_data.items():
                if key not in fundamental_data or not fundamental_data[key]:
                    fundamental_data[key] = value
                elif isinstance(value, dict) and isinstance(fundamental_data.get(key), dict):
                    fundamental_data[key].update(value)
        except Exception as e:
            self.logger.warning(f"akshare获取基本面数据失败: {e}")
        
        # 缓存数据
        self.fundamental_cache[stock_code] = (datetime.now(), fundamental_data)
        self.logger.info(f"✓ {stock_code} 综合基本面数据获取完成并已缓存")
        
        return fundamental_data

    def _get_fundamental_data_from_baostock(self, stock_code):
        """使用BaoStock获取基本面数据"""
        try:
            fundamental_data = {}
            formatted_code = self._format_stock_code_for_baostock(stock_code)
            
            self.logger.info(f"正在从BaoStock获取 {stock_code} 的基本面数据...")
            
            # 1. 获取股票基本信息
            try:
                rs = bs.query_stock_basic(code=formatted_code)
                if rs.error_code == '0':
                    basic_data = []
                    while (rs.error_code == '0') & rs.next():
                        basic_data.append(rs.get_row_data())
                    
                    if basic_data:
                        basic_df = pd.DataFrame(basic_data, columns=rs.fields)
                        if not basic_df.empty:
                            basic_info = basic_df.iloc[0].to_dict()
                            fundamental_data['basic_info'] = basic_info
                            self.logger.info("✓ BaoStock基本信息获取成功")
            except Exception as e:
                self.logger.warning(f"BaoStock获取基本信息失败: {e}")
                fundamental_data['basic_info'] = {}
            
            # 2. 获取最新的季度财务数据
            try:
                # 获取最近的财务报告期
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
                
                # 获取盈利能力指标
                rs_profit = bs.query_profit_data(
                    code=formatted_code,
                    year=datetime.now().year,
                    quarter=((datetime.now().month-1) // 3) + 1
                )
                
                if rs_profit.error_code == '0':
                    profit_data = []
                    while (rs_profit.error_code == '0') & rs_profit.next():
                        profit_data.append(rs_profit.get_row_data())
                    
                    if profit_data:
                        profit_df = pd.DataFrame(profit_data, columns=rs_profit.fields)
                        if not profit_df.empty:
                            latest_profit = profit_df.iloc[-1].to_dict()
                            fundamental_data['profit_data'] = latest_profit
                            self.logger.info("✓ BaoStock盈利能力数据获取成功")
                
                # 获取成长能力指标
                rs_growth = bs.query_growth_data(
                    code=formatted_code,
                    year=datetime.now().year,
                    quarter=((datetime.now().month-1) // 3) + 1
                )
                
                if rs_growth.error_code == '0':
                    growth_data = []
                    while (rs_growth.error_code == '0') & rs_growth.next():
                        growth_data.append(rs_growth.get_row_data())
                    
                    if growth_data:
                        growth_df = pd.DataFrame(growth_data, columns=rs_growth.fields)
                        if not growth_df.empty:
                            latest_growth = growth_df.iloc[-1].to_dict()
                            fundamental_data['growth_data'] = latest_growth
                            self.logger.info("✓ BaoStock成长能力数据获取成功")
                
                # 获取杜邦指标
                rs_dupont = bs.query_dupont_data(
                    code=formatted_code,
                    year=datetime.now().year,
                    quarter=((datetime.now().month-1) // 3) + 1
                )
                
                if rs_dupont.error_code == '0':
                    dupont_data = []
                    while (rs_dupont.error_code == '0') & rs_dupont.next():
                        dupont_data.append(rs_dupont.get_row_data())
                    
                    if dupont_data:
                        dupont_df = pd.DataFrame(dupont_data, columns=rs_dupont.fields)
                        if not dupont_df.empty:
                            latest_dupont = dupont_df.iloc[-1].to_dict()
                            fundamental_data['dupont_data'] = latest_dupont
                            self.logger.info("✓ BaoStock杜邦指标获取成功")
                            
            except Exception as e:
                self.logger.warning(f"BaoStock获取财务指标失败: {e}")
            
            return fundamental_data
            
        except Exception as e:
            self.logger.error(f"BaoStock获取基本面数据失败: {e}")
            return {}

    def _get_fundamental_data_from_akshare(self, stock_code):
        """使用akshare获取基本面数据（详细版）"""
        try:
            import akshare as ak
            
            fundamental_data = {}
            self.logger.info(f"开始从akshare获取 {stock_code} 的25项综合财务指标...")
            
            # 1. 基本信息
            try:
                self.logger.info("正在获取股票基本信息...")
                stock_info = ak.stock_individual_info_em(symbol=stock_code)
                if stock_info is not None and not stock_info.empty:
                    info_dict = dict(zip(stock_info['item'], stock_info['value']))
                    fundamental_data['basic_info'] = info_dict
                    self.logger.info("✓ 股票基本信息获取成功")
                else:
                    fundamental_data['basic_info'] = {}
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
                    if income_statement is not None and not income_statement.empty:
                        if self._is_data_stale(income_statement, ['报告期','发布日期','date'], max_days=540, what='利润表'):
                            income_statement = pd.DataFrame()
                    if income_statement is not None and not income_statement.empty:
                        latest_income = income_statement.iloc[0].to_dict()
                        financial_indicators.update(latest_income)
                except Exception as e:
                    self.logger.warning(f"获取利润表数据失败: {e}")
                
                try:
                    # 财务分析指标
                    balance_sheet = ak.stock_financial_analysis_indicator(symbol=stock_code)
                    if balance_sheet is not None and not balance_sheet.empty:
                        if self._is_data_stale(balance_sheet, ['日期','报告期','date'], max_days=540, what='财务分析指标'):
                            balance_sheet = pd.DataFrame()
                    if balance_sheet is not None and not balance_sheet.empty:
                        latest_balance = balance_sheet.iloc[-1].to_dict()
                        financial_indicators.update(latest_balance)
                except Exception as e:
                    self.logger.warning(f"获取财务分析指标失败: {e}")
                
                try:
                    # 现金流量表 - 修复None检查
                    cash_flow = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_code)
                    if cash_flow is not None and not cash_flow.empty:
                        if self._is_data_stale(cash_flow, ['报告期','公告日期','date'], max_days=540, what='现金流量表'):
                            cash_flow = pd.DataFrame()
                    if cash_flow is not None and not cash_flow.empty:
                        latest_cash = cash_flow.iloc[-1].to_dict()
                        financial_indicators.update(latest_cash)
                    else:
                        self.logger.info("现金流量表数据为空")
                except Exception as e:
                    self.logger.warning(f"获取现金流量表失败: {e}")
                
                # 计算25项核心财务指标
                core_indicators = self._calculate_core_financial_indicators(financial_indicators)
                fundamental_data['financial_indicators'] = core_indicators
                
                self.logger.info(f"✓ 成功计算 {len(core_indicators)} 项有效财务指标")
                self.logger.info(f"✓ 获取到 {len(core_indicators)} 项财务指标")
                
            except Exception as e:
                self.logger.warning(f"获取财务指标失败: {e}")
                fundamental_data['financial_indicators'] = {}
            
            # 3. 估值指标 - 使用更稳定的API
            try:
                self.logger.info("正在获取估值指标...")
                try:
                    spot = ak.stock_zh_a_spot_em()
                    if spot is not None and not spot.empty:
                        self._save_spot_snapshot_cache(spot)
                except Exception as _e:
                    self.logger.info("ℹ️ 在线估值快照获取失败，尝试使用本地缓存")
                    spot = self._load_spot_snapshot_cache()
                if spot is not None and not spot.empty:
                    try:
                        row = spot[spot['代码'].astype(str) == str(stock_code)]
                        if not row.empty:
                            r = row.iloc[0]
                            def pick(*names):
                                for n in names:
                                    if n in row.columns:
                                        return r.get(n)
                                return None
                            pe = pick('市盈率-动态','市盈率TTM','市盈率')
                            pb = pick('市净率')
                            mktcap = pick('总市值','总市值(亿)')
                            divy = pick('股息率TTM(%)','股息率TTM','股息率')
                            # 规范化
                            def to_float(x):
                                try:
                                    if isinstance(x, str) and x.endswith('%'):
                                        return float(x.strip('%'))
                                    return float(x)
                                except Exception:
                                    return None
                            valuation = {
                                '市盈率': to_float(pe),
                                '市净率': to_float(pb),
                                '总市值': to_float(mktcap),
                                '股息收益率': to_float(divy)
                            }
                            # 去除None
                            valuation = {k: v for k, v in valuation.items() if v is not None}
                            fundamental_data['valuation'] = valuation
                            self.logger.info("✓ 估值指标获取成功(快照)")
                        else:
                            fundamental_data['valuation'] = {}
                            self.logger.info("ℹ️ 未在快照中找到该代码")
                    except Exception as e2:
                        self.logger.warning(f"处理估值快照失败: {e2}")
                        fundamental_data['valuation'] = {}
                else:
                    fundamental_data['valuation'] = {}
                    self.logger.info("ℹ️ 估值快照不可用")
            except Exception as e:
                self.logger.warning(f"获取估值指标失败: {e}")
                fundamental_data['valuation'] = {}
            
            # 4. 业绩预告和业绩快报
            try:
                self.logger.info("正在获取业绩预告...")
                perf_items = []
                def _filter_by_code(df):
                    if df is None or df.empty:
                        return df
                    cols = [c for c in df.columns if str(c) in ('代码','股票代码','证券代码')] or [c for c in df.columns if '码' in str(c)] or [df.columns[0]]
                    col = cols[0]
                    s = df[col].astype(str)
                    mask = s.str.endswith(str(stock_code)) | s.str.contains(str(stock_code), na=False)
                    return df[mask]
                # 先尝试业绩预告
                try:
                    yjyg = ak.stock_yjyg_em()
                    if yjyg is not None and not yjyg.empty:
                        df = _filter_by_code(yjyg)
                        # 按公告日期倒序
                        date_col = '公告日期' if '公告日期' in df.columns else ('最新公告日期' if '最新公告日期' in df.columns else None)
                        if date_col:
                            df = df.sort_values(by=date_col, ascending=False)
                        for _, r in df.head(10).iterrows():
                            item = {
                                'date': str(r.get('公告日期') or r.get('最新公告日期') or r.get('公告时间') or ''),
                                'type': '业绩预告',
                                'range': str(r.get('预告类型') or r.get('变动幅度') or r.get('预告净利润变动幅度') or ''),
                                'reason': str(r.get('变动原因') or ''),
                                'profit_low': str(r.get('预测净利润下限') or r.get('预告净利润下限') or ''),
                                'profit_high': str(r.get('预测净利润上限') or r.get('预告净利润上限') or ''),
                                'eps': str(r.get('每股收益') or r.get('基本每股收益') or ''),
                            }
                            perf_items.append(item)
                except Exception as e:
                    self.logger.info(f"业绩预告接口不可用: {e}")
                # 再尝试业绩快报
                if not perf_items:
                    try:
                        yjkb = ak.stock_yjkb_em()
                        if yjkb is not None and not yjkb.empty:
                            df = _filter_by_code(yjkb)
                            date_col = '公告日期' if '公告日期' in df.columns else ('最新公告日期' if '最新公告日期' in df.columns else None)
                            if date_col:
                                df = df.sort_values(by=date_col, ascending=False)
                            for _, r in df.head(10).iterrows():
                                item = {
                                    'date': str(r.get('公告日期') or r.get('最新公告日期') or r.get('公告时间') or ''),
                                    'type': '业绩快报',
                                    'revenue': str(r.get('营业总收入') or r.get('营业收入') or ''),
                                    'net_profit': str(r.get('净利润') or r.get('归母净利润') or ''),
                                    'eps': str(r.get('每股收益') or r.get('基本每股收益') or ''),
                                    'yoy_revenue': str(r.get('营业总收入同比增长') or r.get('营业收入同比增长') or ''),
                                    'yoy_netprofit': str(r.get('净利润同比增长') or r.get('归母净利润同比增长') or ''),
                                }
                                perf_items.append(item)
                    except Exception as e:
                        self.logger.info(f"业绩快报接口不可用: {e}")
                fundamental_data['performance_forecast'] = perf_items[:10]
                if perf_items:
                    self.logger.info("✓ 业绩预告/快报获取成功")
                else:
                    self.logger.info("ℹ️ 业绩预告暂时不可用")
            except Exception as e:
                self.logger.warning(f"获取业绩预告失败: {e}")
                fundamental_data['performance_forecast'] = []
            
            # 5. 分红配股信息 - 优先使用BaoStock
            try:
                self.logger.info("正在获取分红配股信息...")
                dividend_info_list = []
                
                # 首先尝试BaoStock分红数据（推荐）
                if self.baostock_connected:
                    try:
                        formatted_code = self._format_stock_code_for_baostock(stock_code)
                        
                        # 获取最近3年的分红数据
                        current_year = datetime.now().year
                        for year in [current_year, current_year-1, current_year-2]:
                            rs_dividend = bs.query_dividend_data(
                                code=formatted_code,
                                year=year,
                                yearType="report"
                            )
                            
                            if rs_dividend.error_code == '0':
                                dividend_data = []
                                while (rs_dividend.error_code == '0') & rs_dividend.next():
                                    dividend_data.append(rs_dividend.get_row_data())
                                
                                if dividend_data:
                                    df_dividend = pd.DataFrame(dividend_data, columns=rs_dividend.fields)
                                    # 转换为标准格式，使用正确的字段名
                                    for _, row in df_dividend.iterrows():
                                        dividend_item = {
                                            'year': str(year),  # 使用查询的年份
                                            'dividend_per_share': str(row.get('dividCashPsBeforeTax', '0')),
                                            'dividend_after_tax': str(row.get('dividCashPsAfterTax', '0')),
                                            'dividend_type': '现金分红',
                                            'announcement_date': str(row.get('dividPlanAnnounceDate', '')),
                                            'record_date': str(row.get('dividRegistDate', '')),
                                            'ex_dividend_date': str(row.get('dividOperateDate', '')),
                                            'pay_date': str(row.get('dividPayDate', '')),
                                            'dividend_description': str(row.get('dividCashStock', '')),
                                            'source': 'BaoStock'
                                        }
                                        dividend_info_list.append(dividend_item)
                        
                        if dividend_info_list:
                            dividend_info_list = self._normalize_dividend_records(dividend_info_list)
                            dividend_info_list = sorted(dividend_info_list, key=lambda x: str(x.get('year','')), reverse=True)
                            fundamental_data['dividend_info'] = dividend_info_list
                            self.logger.info(f"✓ BaoStock获取分红配股信息成功，共{len(dividend_info_list)}条")
                        else:
                            self.logger.info("✓ BaoStock查询完成，该股票暂无分红记录")
                            fundamental_data['dividend_info'] = []
                    
                    except Exception as e:
                        self.logger.warning(f"BaoStock获取分红数据失败: {e}")
                        dividend_info_list = []
                
                # 如果BaoStock没有获取到数据，尝试akshare作为备用
                if not dividend_info_list:
                    try:
                        # 尝试akshare的分红数据API
                        dividend_apis = [
                            ('stock_dividend_cninfo', lambda: ak.stock_dividend_cninfo(symbol=stock_code)),
                            ('stock_history_dividend_detail', lambda: ak.stock_history_dividend_detail(symbol=stock_code)),
                        ]
                        
                        for api_name, api_func in dividend_apis:
                            try:
                                dividend_data = api_func()
                                if dividend_data is not None and not dividend_data.empty:
                                    # 转换akshare数据格式
                                    for _, row in dividend_data.head(10).iterrows():
                                        dividend_item = {
                                            'year': str(row.iloc[0] if len(row) > 0 else ''),
                                            'dividend_per_share': str(row.iloc[1] if len(row) > 1 else '0'),
                                            'dividend_type': str(row.iloc[2] if len(row) > 2 else '分红'),
                                            'announcement_date': str(row.iloc[3] if len(row) > 3 else ''),
                                            'record_date': str(row.iloc[4] if len(row) > 4 else ''),
                                            'ex_dividend_date': str(row.iloc[5] if len(row) > 5 else ''),
                                            'source': f'akshare_{api_name}'
                                        }
                                        dividend_info_list.append(dividend_item)
                                    # 标准化与排序
                                    dividend_info_list = self._normalize_dividend_records(dividend_info_list)
                                    dividend_info_list = sorted(dividend_info_list, key=lambda x: str(x.get('year','')), reverse=True)
                                    fundamental_data['dividend_info'] = dividend_info_list
                                    self.logger.info(f"✓ akshare({api_name})获取分红配股信息成功，共{len(dividend_info_list)}条")
                                    break
                            except Exception as e:
                                self.logger.warning(f"akshare {api_name} 获取分红数据失败: {e}")
                                continue
                        
                        if not dividend_info_list:
                            fundamental_data['dividend_info'] = []
                            self.logger.info("ℹ️ 分红配股信息暂时不可用")
                    
                    except Exception as e:
                        self.logger.warning(f"akshare获取分红配股信息失败: {e}")
                        fundamental_data['dividend_info'] = []
                
            except Exception as e:
                self.logger.warning(f"获取分红配股信息失败: {e}")
                fundamental_data['dividend_info'] = []
            
            # 6. 行业分析数据
            try:
                self.logger.info("正在获取行业分析数据...")
                industry_data = self._get_industry_analysis(stock_code)
                fundamental_data['industry_analysis'] = industry_data
                self.logger.info("✓ 行业分析数据获取成功")
            except Exception as e:
                self.logger.warning(f"获取行业分析失败: {e}")
                fundamental_data['industry_analysis'] = {}
            
            # 其他数据项初始化
            fundamental_data.setdefault('shareholders', [])
            fundamental_data.setdefault('institutional_holdings', [])
            
            return fundamental_data
            
        except Exception as e:
            self.logger.error(f"akshare获取综合基本面数据失败: {str(e)}")
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
        """获取行业分析数据（含同业对比与排名）"""
        try:
            import akshare as ak
            import difflib
            industry_data = {}

            # 获取基本行业归属
            try:
                info = ak.stock_individual_info_em(symbol=stock_code)
                industry_name = None
                if info is not None and not info.empty:
                    m = dict(zip(info['item'], info['value']))
                    industry_name = m.get('所属行业') or m.get('行业')
                # 主口径（东财）的行业名
                primary = industry_name or ''
                industry_data['industry_name'] = primary  # 兼容旧字段
                industry_data['industry_name_primary'] = primary
                # 预置多口径信息占位
                industry_data['baostock_industry_name'] = ''
                industry_data['industry_source'] = ''
                industry_data['resolved_em_name'] = ''
                industry_data['industry_tags'] = []
            except Exception as e:
                self.logger.warning(f"获取行业归属失败: {e}")
                industry_data['industry_name'] = ''
                industry_data['industry_name_primary'] = ''
                industry_data['baostock_industry_name'] = ''
                industry_data['industry_source'] = ''
                industry_data['resolved_em_name'] = ''
                industry_data['industry_tags'] = []

            # 获取行业成份并做同业对比
            peers = []
            try:
                # 根据行业名称解析为Eastmoney可识别名称
                def _resolve_em_industry_name(name: str) -> str:
                    if not isinstance(name, str) or not name.strip():
                        return ''
                    try:
                        df_names = ak.stock_board_industry_name_em()
                    except Exception:
                        df_names = pd.DataFrame()
                    if df_names is None or df_names.empty:
                        return name
                    # 取可能的名称列
                    cand_cols = [c for c in ['板块名称','名称','行业名称','行业'] if c in df_names.columns]
                    name_col = cand_cols[0] if cand_cols else df_names.columns[0]
                    pool = [str(x) for x in df_names[name_col].dropna().unique().tolist()]
                    # 1) 精确匹配
                    if name in pool:
                        return name
                    # 2) 子串匹配（双向）
                    subs = [p for p in pool if (name in p) or (p in name)]
                    if subs:
                        return subs[0]
                    # 3) 相似度匹配
                    m = difflib.get_close_matches(name, pool, n=1, cutoff=0.6)
                    if m:
                        return m[0]
                    return name

                if industry_data['industry_name']:
                    em_name = _resolve_em_industry_name(industry_data['industry_name'])
                    industry_data['resolved_em_name'] = em_name
                    cons = pd.DataFrame()
                    # 先尝试EM口径
                    try:
                        cons = ak.stock_board_industry_cons_em(symbol=em_name)
                        if cons is not None and not cons.empty:
                            industry_data['industry_source'] = 'EM'
                    except Exception as e_em:
                        self.logger.info(f"ℹ️ EM行业成份获取失败({e_em})，尝试THS口径")
                    # 兜底：尝试同花顺口径
                    if (cons is None or cons.empty):
                        try:
                            cons = ak.stock_board_industry_cons_ths(symbol=em_name)
                            if cons is not None and not cons.empty and not industry_data.get('industry_source'):
                                industry_data['industry_source'] = 'THS'
                        except Exception as e_ths:
                            self.logger.info(f"ℹ️ THS行业成份获取失败({e_ths})")
                    # 若仍为空，尝试从缓存读取
                    if (cons is None or cons.empty):
                        cache_df = self._load_industry_cons_cache(em_name)
                        if cache_df is not None and not cache_df.empty:
                            cons = cache_df
                            self.logger.info("ℹ️ 使用本地行业成份缓存")
                            if not industry_data.get('industry_source'):
                                industry_data['industry_source'] = 'Cache'
                else:
                    cons = pd.DataFrame()
                if cons is not None and not cons.empty:
                    # 成功则写入缓存
                    try:
                        self._save_industry_cons_cache(em_name, cons)
                    except Exception:
                        pass
                # 进一步兜底：使用BaoStock行业分类构造成份列表
                if (cons is None or cons.empty) and getattr(self, 'baostock_connected', False):
                    try:
                        # 获取全市场行业分类
                        dt = datetime.now().strftime('%Y-%m-%d')
                        df_ind = self._query_baostock_data(bs.query_stock_industry, date=dt)
                        if (df_ind is None or df_ind.empty):
                            df_ind = self._query_baostock_data(bs.query_stock_industry)
                        if df_ind is not None and not df_ind.empty:
                            # 找到目标股票所在行业
                            formatted = self._format_stock_code_for_baostock(stock_code)
                            # 兼容无前缀匹配
                            row_self = df_ind[(df_ind['code'] == formatted) | (df_ind['code'].str.endswith(str(stock_code)))]
                            if not row_self.empty:
                                bs_ind_name = row_self.iloc[0].get('industry') or ''
                                industry_data['baostock_industry_name'] = str(bs_ind_name)
                                peers_df = df_ind[df_ind['industry'] == bs_ind_name].copy()
                                if not peers_df.empty:
                                    # 构造与EM类似的成份表结构
                                    cons = pd.DataFrame({
                                        '代码': peers_df['code'].astype(str).str[-6:],
                                        '名称': peers_df['code_name'] if 'code_name' in peers_df.columns else peers_df.get('codeName', '')
                                    })
                                    # 设置行业名称（若原本为空）
                                    if not industry_data.get('industry_name'):
                                        industry_data['industry_name'] = str(bs_ind_name)
                                        industry_data['industry_name_primary'] = industry_data.get('industry_name_primary','')
                                    if not industry_data.get('industry_source'):
                                        industry_data['industry_source'] = 'BaoStock'
                                    self.logger.info("✓ 使用BaoStock行业分类构造行业成份")
                    except Exception as e_bs:
                        self.logger.info(f"ℹ️ BaoStock行业分类兜底失败: {e_bs}")
                    # 统一列名
                    code_col = '代码' if '代码' in cons.columns else cons.columns[0]
                    name_col = '名称' if '名称' in cons.columns else (cons.columns[1] if len(cons.columns)>1 else code_col)
                    # 取快照用于涨跌幅与市值（在线失败则回退缓存）
                    try:
                        spot = ak.stock_zh_a_spot_em()
                        if spot is not None and not spot.empty:
                            self._save_spot_snapshot_cache(spot)
                    except Exception:
                        spot = self._load_spot_snapshot_cache()
                    if spot is not None and not spot.empty:
                        spot = spot[[c for c in spot.columns if c in ['代码','名称','涨跌幅','最新价','总市值','总市值(亿)']]]
                        df = cons.merge(spot, left_on=code_col, right_on='代码', how='left')
                    else:
                        df = cons.copy()
                    # 合并后，统一选择可用的代码/名称列（避免 _x/_y 冲突）
                    def pick_col(df_cols, candidates):
                        for c in candidates:
                            if c in df_cols:
                                return c
                        return None
                    code_candidates = [code_col, '代码', f'{code_col}_x', '代码_x', f'{code_col}_y', '代码_y']
                    name_candidates = [name_col, '名称', f'{name_col}_x', '名称_x', f'{name_col}_y', '名称_y']
                    code_eff = pick_col(df.columns, code_candidates) or code_col
                    name_eff = pick_col(df.columns, name_candidates) or name_col
                    # 计算排名
                    def to_float(x):
                        try:
                            if isinstance(x,str) and x.endswith('%'):
                                return float(x.strip('%'))
                            return float(x)
                        except Exception:
                            return np.nan
                    df['涨跌幅_val'] = df['涨跌幅'].apply(to_float) if '涨跌幅' in df.columns else np.nan
                    cap_col = '总市值' if '总市值' in df.columns else ('总市值(亿)' if '总市值(亿)' in df.columns else None)
                    if cap_col:
                        df['总市值_val'] = df[cap_col].apply(to_float)
                    else:
                        df['总市值_val'] = np.nan
                    # 排名与百分位
                    df['rank_return'] = df['涨跌幅_val'].rank(ascending=False, method='min')
                    df['pct_return'] = df['rank_return'] / df['rank_return'].max()
                    df['rank_mktcap'] = df['总市值_val'].rank(ascending=False, method='min')
                    df['pct_mktcap'] = df['rank_mktcap'] / df['rank_mktcap'].max()
                    # 当前个股行（兼容不同代码列名）
                    code_match_col = '代码' if '代码' in df.columns else code_eff
                    try:
                        cur = df[df[code_match_col].astype(str) == str(stock_code)]
                    except Exception:
                        cur = pd.DataFrame()
                    if not cur.empty:
                        row = cur.iloc[0]
                        industry_data['industry_rank'] = {
                            'peers_count': int(len(df)),
                            'return_rank': float(row.get('rank_return', np.nan)),
                            'return_percentile': float(row.get('pct_return', np.nan)),
                            'mktcap_rank': float(row.get('rank_mktcap', np.nan)),
                            'mktcap_percentile': float(row.get('pct_mktcap', np.nan)),
                        }
                        industry_data['in_constituents'] = True
                    else:
                        industry_data['industry_rank'] = {}
                        industry_data['in_constituents'] = False
                    # 附带少量同业摘要
                    try:
                        cols = []
                        for c in [code_eff, name_eff]:
                            if c and c in df.columns and c not in cols:
                                cols.append(c)
                        if '涨跌幅' in df.columns:
                            cols.append('涨跌幅')
                        if '总市值' in df.columns:
                            cols.append('总市值')
                        elif '总市值(亿)' in df.columns:
                            cols.append('总市值(亿)')
                        if cols:
                            peers = df[cols].head(20).to_dict('records')
                        else:
                            peers = cons.head(20).to_dict('records')
                    except Exception:
                        peers = []
                else:
                    industry_data['industry_rank'] = {}
                    industry_data['in_constituents'] = False
            except Exception as e:
                self.logger.warning(f"同业对比失败: {e}")
                industry_data['industry_rank'] = {}
                industry_data['in_constituents'] = False
            industry_data['peers_sample'] = peers

            # 汇总行业标签
            try:
                tags = []
                for t in [industry_data.get('industry_name_primary',''), industry_data.get('baostock_industry_name','')]:
                    if isinstance(t, str) and t.strip() and t not in tags:
                        tags.append(t)
                industry_data['industry_tags'] = tags
            except Exception:
                pass

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
                            'title': str(row.get('新闻标题', '')),
                            'content': str(row.get('新闻内容', '')),
                            'date': str(row.get('发布时间', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))),
                            'source': str(row.get('文章来源', 'eastmoney')),
                            'url': str(row.get('新闻链接', '')),
                            'keyword': str(row.get('关键词', stock_code)),
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
                announcements = ak.stock_notice_report()
                if not announcements.empty:
                    # 筛选当前股票的公告（兼容不同列名）
                    code_cols = [c for c in ['代码','股票代码','证券代码','证券代码(6位)'] if c in announcements.columns]
                    if not code_cols:
                        # 找一个看起来像代码的列
                        code_cols = [c for c in announcements.columns if '代码' in str(c)] or [announcements.columns[0]]
                    code_col = code_cols[0]
                    stock_announcements = announcements[announcements[code_col].astype(str) == str(stock_code)]
                    # 仅保留最近365天（兼容不同日期列名）
                    date_cols = [c for c in ['公告日期','公告时间','日期','发布时间'] if c in stock_announcements.columns]
                    if date_cols:
                        try:
                            cutoff = (datetime.now() - timedelta(days=365)).date()
                            dt = pd.to_datetime(stock_announcements[date_cols[0]], errors='coerce').dt.date
                            stock_announcements = stock_announcements.loc[dt >= cutoff]
                        except Exception:
                            pass
                    processed_announcements = []
                    for _, row in stock_announcements.head(30).iterrows():
                        announcement = {
                            'title': str(row.get('公告标题', '')),
                            'content': str(row.get('公告类型', '')) + ' - ' + str(row.get('公告标题', '')),
                            'date': str(row.get('公告日期') or row.get('公告时间') or row.get('日期') or row.get('发布时间') or datetime.now().strftime('%Y-%m-%d')),
                            'type': str(row.get('公告类型', '公告')),
                            'relevance_score': 1.0
                        }
                        processed_announcements.append(announcement)
                    all_news_data['announcements'] = processed_announcements
                    self.logger.info(f"✓ 获取公司公告 {len(processed_announcements)} 条")
                else:
                    self.logger.info("未获取到公司公告数据")
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
            try:
                # 基于数量的置信度上限曲线（100条以上基本饱和）
                qty_conf = float(min(1.0, np.log1p(n) / np.log1p(100)))
            except Exception:
                qty_conf = float(min(1.0, n / 100.0))

            # 基于时间的平均权重（越新越高）
            try:
                avg_time_w = float(np.mean([it.get('time_weight', 1.0) for it in dedup_items])) if n else 0.0
            except Exception:
                avg_time_w = 0.0

            confidence = float(max(0.0, min(1.0, 0.3 + 0.5 * qty_conf + 0.2 * avg_time_w)))

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
            # 数据质量预检查
            if price_data.empty:
                self.logger.warning("价格数据为空，返回默认技术分析")
                return self._get_default_technical_analysis()
            
            if 'close' not in price_data.columns:
                self.logger.warning("缺少收盘价列，返回默认技术分析")
                return self._get_default_technical_analysis()
            
            # 检查数据长度
            data_length = len(price_data)
            if data_length < 5:
                self.logger.warning(f"数据长度不足({data_length}行)，返回默认技术分析")
                return self._get_default_technical_analysis()
            
            # 检查关键列的数据质量
            required_columns = ['close']
            optional_columns = ['high', 'low', 'volume', 'open']
            
            for col in required_columns:
                if col in price_data.columns:
                    null_count = price_data[col].isnull().sum()
                    if null_count > 0:
                        self.logger.warning(f"'{col}'列有{null_count}个空值")
                    
                    # 尝试转换为数值类型
                    try:
                        price_data[col] = pd.to_numeric(price_data[col], errors='coerce')
                        invalid_count = price_data[col].isnull().sum()
                        if invalid_count > data_length * 0.5:  # 超过50%的数据无效
                            self.logger.error(f"'{col}'列数据质量极差，有效数据不足50%")
                            return self._get_default_technical_analysis()
                    except Exception as e:
                        self.logger.error(f"无法转换'{col}'列为数值类型: {e}")
                        return self._get_default_technical_analysis()
            
            self.logger.info(f"技术指标计算开始：数据行数{data_length}，列数{len(price_data.columns)}")

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
                # 关键均线指标输出
                technical_analysis['ma20'] = ma20
                technical_analysis['ma50'] = ma50
                try:
                    technical_analysis['price_vs_ma20_pct'] = float((latest - ma20) / ma20 * 100.0) if ma20 else 0.0
                except Exception:
                    technical_analysis['price_vs_ma20_pct'] = 0.0
                try:
                    technical_analysis['price_vs_ma50_pct'] = float((latest - ma50) / ma50 * 100.0) if ma50 else 0.0
                except Exception:
                    technical_analysis['price_vs_ma50_pct'] = 0.0
                try:
                    technical_analysis['ma_slope50'] = float(price_data['ma50'].diff().iloc[-1]) if len(price_data) >= 2 else 0.0
                except Exception:
                    technical_analysis['ma_slope50'] = 0.0
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
        # 检查市场状态
        market_open, market_status = check_market_status()
        
        if not market_open:
            return {
                'ma_trend': market_status,
                'rsi': 50.0,
                'macd_signal': market_status,
                'bb_position': 0.5,
                'volume_status': market_status
            }
        else:
            return {
                'ma_trend': '计算中...',
                'rsi': 50.0,
                'macd_signal': '计算中...',
                'bb_position': 0.5,
                'volume_status': '计算中...'
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
            # 缓存最近价格
            try:
                self._last_price = float(current_price)
            except Exception:
                self._last_price = None

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

    def _get_stock_snapshot_with_retry(self, max_retries: int = 3) -> pd.DataFrame:
        """
        获取A股快照 - 腾讯/新浪接口（主），AkShare/东财已停用
        返回 DataFrame 含列：代码, 名称, 最新价, 涨跌幅, 市盈率-动态, 总市值, 换手率
        """
        self.logger.info("📊 正在通过腾讯/新浪接口获取A股快照...")

        # ── 主接口：新浪分页API（完整A股列表，含PE/市值）─────────────────────
        df = self._snapshot_from_sina()
        if df is not None and not df.empty:
            self.logger.info(f"✅ 新浪快照获取成功，共 {len(df)} 只股票")
            try:
                self._save_spot_snapshot_cache(df)
            except Exception:
                pass
            return df

        # ── 备用接口：腾讯批量行情 (qt.gtimg.cn) ─────────────────────────────
        self.logger.warning("⚠️ 新浪API失败，降级到腾讯接口...")
        df = self._snapshot_from_tencent_batch()
        if df is not None and not df.empty:
            self.logger.info(f"✅ 腾讯快照获取成功，共 {len(df)} 只股票")
            try:
                self._save_spot_snapshot_cache(df)
            except Exception:
                pass
            return df

        self.logger.warning("⚠️ 腾讯/新浪接口均失败，尝试使用本地缓存")
        return pd.DataFrame()

    def _snapshot_from_sina(self) -> 'pd.DataFrame':
        """新浪分页接口：获取全量A股快照（含PE、市值）"""
        import urllib.request, json
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'http://finance.sina.com.cn'
        }
        all_stocks = []
        page = 1
        while True:
            url = (f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                   f"Market_Center.getHQNodeData?page={page}&num=100&sort=symbol"
                   f"&asc=1&node=hs_a&_s_r_a=page")
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=8) as r:
                    batch = json.loads(r.read().decode('gbk', 'ignore'))
                if not batch:
                    break
                all_stocks.extend(batch)
                page += 1
                if page > 80:   # 最多80页（8000只，足够覆盖全A股）
                    break
            except Exception as e:
                self.logger.warning(f"[新浪快照] 第{page}页失败: {e}")
                break

        if not all_stocks:
            return pd.DataFrame()

        rows = []
        for s in all_stocks:
            try:
                code = str(s.get('code', ''))
                if len(code) != 6 or not code.isdigit():
                    continue
                price = float(s.get('trade', 0) or 0)
                if price <= 0:
                    continue
                rows.append({
                    '代码':       code,
                    '名称':       s.get('name', ''),
                    '最新价':     price,
                    '涨跌幅':     float(s.get('changepercent', 0) or 0),
                    '市盈率-动态': float(s.get('per', 0) or 0),
                    '总市值':     round(float(s.get('mktcap', 0) or 0) / 10000, 2),   # 万→亿
                    '流通市值':   round(float(s.get('nmc', 0) or 0) / 10000, 2),
                    '换手率':     float(s.get('turnoverratio', 0) or 0),
                })
            except Exception:
                continue
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df[df['市盈率-动态'] > 0]
        return df.reset_index(drop=True)

    def _snapshot_from_tencent_batch(self) -> 'pd.DataFrame':
        """
        腾讯批量接口：qt.gtimg.cn 
        用已知的主板/创业板/科创板代码段构造请求，返回实时行情
        """
        import urllib.request
        # 构造全量代码（0开头→sz，6开头→sh，3开头→sz，688→sh）
        sh_ranges = [range(600000, 605000), range(601000, 606000),
                     range(600000, 610000), range(688000, 688999)]
        sz_ranges = [range(0, 3000), range(300000, 302000)]
        all_codes = []
        for r in sh_ranges:
            for c in r:
                all_codes.append(f"sh{c:06d}")
        for r in sz_ranges:
            for c in r:
                all_codes.append(f"sz{c:06d}")
        # 去重
        all_codes = list(dict.fromkeys(all_codes))

        rows = []
        batch_size = 100
        headers = {'User-Agent': 'Mozilla/5.0'}
        for i in range(0, len(all_codes), batch_size):
            batch = all_codes[i:i+batch_size]
            url = 'http://qt.gtimg.cn/q=' + ','.join(batch)
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=5) as r:
                    text = r.read().decode('gbk', 'ignore')
                for line in text.strip().split('\n'):
                    parts = line.split('~')
                    if len(parts) < 46:
                        continue
                    code = parts[2]
                    name = parts[1]
                    if not code or len(code) != 6 or not code.isdigit() or not name:
                        continue
                    price = float(parts[3]) if parts[3] else 0
                    yclose = float(parts[4]) if parts[4] else 0
                    if price <= 0 or yclose <= 0:
                        continue
                    chg = round((price - yclose) / yclose * 100, 2)
                    pe  = float(parts[39]) if parts[39] else 0
                    mktcap = float(parts[45]) if parts[45] else 0  # 亿元
                    rows.append({
                        '代码': code, '名称': name,
                        '最新价': price, '涨跌幅': chg,
                        '市盈率-动态': pe, '总市值': mktcap,
                        '流通市值': float(parts[44]) if parts[44] else 0,
                        '换手率': 0.0,
                    })
            except Exception:
                continue

        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df[df['最新价'] > 0]
        return df.reset_index(drop=True)

    def get_quick_recommendations(self, top_n: int = 20, min_mktcap_e: float = 30.0, exclude_st: bool = True) -> List[Dict]:
        """基于快照的轻量级推荐列表（不拉取逐股深度数据，秒级返回）。

        返回每项示例：
        {
          'stock_code': '000001', 'stock_name': '平安银行',
          'latest_price': 12.34, 'change_pct': 1.23,
          'pe': 8.9, 'mktcap_e': 1500.0,  # 亿
          'score': 87.5, 'recommendation': '建议买入'
        }
        """
        # 推荐列表优先用BaoStock数据，快照/缓存/akshare仅做兜底
        try:
            print("[推荐列表] 开始生成推荐列表...")
            # 1. 先用BaoStock获取快照（含价格、PE、市值等）
            df_bao = self._get_baostock_snapshot()
            if df_bao is not None and not df_bao.empty:
                print(f"[推荐列表] BaoStock快照获取成功，股票数: {len(df_bao)}，字段: {list(df_bao.columns)}")
                df = df_bao.copy()
            else:
                print("[推荐列表] BaoStock快照为空，尝试快照/缓存...")
                spot = self._get_stock_snapshot_with_retry(max_retries=3)
                if (spot is None) or spot.empty:
                    print("[推荐列表] 在线快照为空，尝试本地缓存...")
                    spot = self._load_spot_snapshot_cache()
                if spot is not None and not spot.empty:
                    print(f"[推荐列表] 本地快照获取成功，股票数: {len(spot)}，字段: {list(spot.columns)}")
                    df = spot.copy()
                else:
                    print("[推荐列表] 快照和缓存均为空，进入离线兜底模式...")
                    fallback_rows = self._quick_recommendations_offline_via_price(top_n=max(5, int(top_n)), exclude_st=exclude_st)
                    if fallback_rows:
                        print(f"[推荐列表] 离线兜底生成 {len(fallback_rows)} 条推荐（基于近端动量/波动）")
                        return fallback_rows
                    print("[推荐列表] 离线兜底也失败，返回空列表")
                    return []

            # 列名选择器
            def pick_col(cands: List[str]) -> Optional[str]:
                for c in cands:
                    if c in df.columns:
                        return c
                return None

            code_col = pick_col(['代码','股票代码','证券代码','A股代码','code']) or df.columns[0]
            name_col = pick_col(['名称','股票简称','简称','name']) or (df.columns[1] if len(df.columns) > 1 else code_col)
            pct_col  = pick_col(['涨跌幅','涨跌幅(%)','涨跌幅%','pct_chg'])
            price_col= pick_col(['最新价','现价','价格','最新'])
            pe_col   = pick_col(['市盈率-动态','市盈率TTM','市盈率'])
            mcap_col = pick_col(['总市值(亿)','总市值'])

            def to_float(x) -> Optional[float]:
                try:
                    if isinstance(x, str):
                        xs = x.strip().replace(',', '')
                        if xs.endswith('%'):
                            return float(xs.rstrip('%'))
                        return float(xs)
                    return float(x)
                except Exception:
                    return None

            # 预处理并过滤
            out_rows = []
            for idx, r in df.iterrows():
                try:
                    print("\n================ 原始数据行 =================")
                    print(f"[推荐列表] 行号: {idx}")
                    for k, v in r.to_dict().items():
                        print(f"    {k}: {v}")
                    print("==========================================\n")
                    code = str(r.get(code_col, '')).strip() if code_col else ''
                    if not code or len(code) < 6:
                        continue
                    code = code[-6:]  # 统一6位
                    name = str(r.get(name_col, code)) if name_col else code
                    if exclude_st and any(x in str(name) for x in ['ST','*ST','退']):
                        continue

                    # 自动回退到最近一个有数据的交易日
                    date_col = None
                    for cand in ['日期','date','交易日期','trade_date','datetime']:
                        if cand in r:
                            date_col = cand
                            break
                    data_date = r.get(date_col) if date_col else None

                    # 市值（亿）
                    mktcap_e = to_float(r.get(mcap_col)) if mcap_col else None
                    if mktcap_e is not None and mcap_col == '总市值' and mktcap_e > 1e9:
                        mktcap_e = mktcap_e / 1e8
                    if (mktcap_e is None) or (mktcap_e < min_mktcap_e):
                        continue

                    # PE
                    pe = to_float(r.get(pe_col)) if pe_col else None
                    if pe is None or pe <= 0 or pe > 1200:
                        continue

                    # 涨跌幅（%）
                    chg = to_float(r.get(pct_col)) if pct_col else None
                    if chg is None:
                        chg = 0.0

                    # 价格
                    price = to_float(r.get(price_col)) if price_col else None
                    if price is None or price <= 0:
                        price = 0.0

                    print(f"[推荐列表] {idx}: 代码={code}, 名称={name}, 价格={price}, 涨跌幅={chg}, PE={pe}, 市值={mktcap_e}, 日期={data_date}")

                    # 评分构造：动量(当日涨跌) + 估值(低PE) + 体量（适中市值）
                    mom = max(0.0, min(1.0, (float(chg) + 5.0) / 10.0))
                    pe_eff = max(0.0, min(120.0, float(pe)))
                    pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
                    x = float(mktcap_e)
                    if x <= 30:
                        mcap_score = 0.2
                    elif x <= 80:
                        mcap_score = 0.6 + (x - 80.0) / 50.0 * 0.4
                    elif x <= 300:
                        mcap_score = 1.0 - abs((x - 190.0) / 110.0) * 0.4
                    elif x <= 800:
                        mcap_score = 0.8 - (x - 300.0) / 500.0 * 0.6
                    else:
                        mcap_score = 0.2
                    mcap_score = max(0.0, min(1.0, mcap_score))

                    score01 = 0.45 * mom + 0.35 * pe_score + 0.20 * mcap_score
                    score = float(round(score01 * 100.0, 2))

                    print(f"[推荐列表-评分] 代码={code}, 价格={price}, 涨跌幅={chg}, PE={pe}, 市值={mktcap_e}, mom={mom}, pe_score={pe_score}, mcap_score={mcap_score}, 综合得分={score}")

                    quick_scores = {
                        'technical': mom * 100.0,
                        'fundamental': pe_score * 100.0,
                        'sentiment': 50.0,
                    }
                    quick_scores['comprehensive'] = self.calculate_comprehensive_score(quick_scores)
                    rec = self.generate_recommendation(quick_scores)

                    out_rows.append({
                        'stock_code': code,
                        'stock_name': name,
                        'latest_price': float(price),
                        'change_pct': float(chg),
                        'pe': float(pe),
                        'mktcap_e': float(mktcap_e),
                        'score': score,
                        'recommendation': rec,
                        'data_date': data_date
                    })
                except Exception as e:
                    print(f"[推荐列表] 行处理异常: {e}")
                    continue

            if not out_rows:
                print("[推荐列表] 结果为空，无有效股票")
                return []
            # 排序并截断
            out_rows.sort(key=lambda x: x.get('score', 0.0), reverse=True)
            print(f"[推荐列表] 最终输出 {len(out_rows)} 条推荐")
            return out_rows[: max(1, int(top_n))]
        except Exception as e:
            print(f"[推荐列表] 推荐生成失败: {e}")
            self.logger.warning(f"快速推荐生成失败: {e}")
            return []
        except Exception as e:
            self.logger.warning(f"快速推荐生成失败: {e}")
            return []

    def _get_candidate_codes_via_baostock(self, exclude_st: bool = True, limit: int = 200) -> List[Dict]:
        """通过BaoStock获取A股候选代码列表（含名称），用于离线兜底。

        返回: [{ 'code': '000001', 'name': '平安银行' }, ...]
        """
        try:
            if not getattr(self, 'baostock_connected', False):
                return []
            # 行业接口通常包含较全成份
            df_ind = self._query_baostock_data(bs.query_stock_industry)
            if df_ind is None or df_ind.empty:
                # 退而求其次：全市场列表（可能无名称）
                rs = bs.query_all_stock()
                rows = []
                while (rs.error_code == '0') and rs.next():
                    rows.append(rs.get_row_data())
                if not rows:
                    return []
                import pandas as pd
                df = pd.DataFrame(rows, columns=rs.fields)
                codes = []
                for _, r in df.iterrows():
                    code = str(r.get('code',''))
                    if '.' in code:
                        code = code.split('.')[-1]
                    code = code[-6:]
                    if len(code) == 6 and code.isdigit():
                        codes.append({'code': code, 'name': ''})
                return codes[:max(1, int(limit))]

            # 规范化
            codes = []
            for _, r in df_ind.iterrows():
                try:
                    code = str(r.get('code',''))
                    name = str(r.get('code_name',''))
                    if '.' in code:
                        code = code.split('.')[-1]
                    code = code[-6:]
                    if len(code) != 6 or (not code.isdigit()):
                        continue
                    if exclude_st and any(x in name for x in ['ST','*ST','退']):
                        continue
                    codes.append({'code': code, 'name': name})
                except Exception:
                    continue
            # 去重保序
            seen = set()
            uniq = []
            for it in codes:
                if it['code'] in seen:
                    continue
                seen.add(it['code'])
                uniq.append(it)
            return uniq[:max(1, int(limit))]
        except Exception:
            return []

    def _quick_recommendations_offline_via_price(self, top_n: int = 20, exclude_st: bool = True) -> List[Dict]:
        """当快照与缓存都不可用时的兜底：
        用BaoStock可用的历史K线，计算近20日动量/波动的简单分数，输出TopN。

        注意：PE/总市值无法保证，置为0。该模式主要为“有结果不空白”。
        """
        try:
            import numpy as np
            import pandas as pd
        except Exception:
            return []

        cands = self._get_candidate_codes_via_baostock(exclude_st=exclude_st, limit=max(120, int(top_n) * 20))
        if not cands:
            return []

        out = []
        scanned = 0
        
        self.logger.info(f"📊 离线模式扫描候选池: {len(cands)} 只股票")
        
        for it in cands:
            if len(out) >= max(10, int(top_n) * 2):  # 收集一定冗余后停止
                break
            code = it['code']
            name = it.get('name') or code
            try:
                df = self.get_stock_data(code)
                if df is None or df.empty or 'close' not in df.columns:
                    continue
                close = pd.to_numeric(df['close'], errors='coerce').dropna()
                if len(close) < 22:
                    continue
                close = close.tail(25)
                
                # 获取最新价格和涨跌幅
                last = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) >= 2 else last
                daily_chg_pct = ((last - prev) / prev) * 100.0 if prev > 1e-8 else 0.0
                
                # 尝试从基本面获取真实PE和市值（多种备用方案）
                pe = 0.0
                mktcap_e = 0.0
                try:
                    # 方案1：从综合基本面数据获取
                    fundamental = self.get_comprehensive_fundamental_data(code)
                    valuation = fundamental.get('valuation', {})
                    
                    if valuation:
                        # 提取PE
                        pe_val = valuation.get('市盈率')
                        if pe_val:
                            try:
                                pe = float(pe_val)
                                if pe <= 0 or pe > 1200:  # 过滤异常值
                                    pe = 0.0
                            except:
                                pass
                        
                        # 提取市值
                        mkt_val = valuation.get('总市值')
                        if mkt_val:
                            try:
                                mktcap_e = float(mkt_val)
                                # 如果是元为单位，转换为亿
                                if mktcap_e > 1e9:
                                    mktcap_e = mktcap_e / 1e8
                            except:
                                pass
                    
                    # 方案2：如果估值为空，尝试从BaoStock基本信息获取
                    if pe == 0.0 or mktcap_e == 0.0:
                        basic_info = fundamental.get('basic_info', {})
                        if basic_info:
                            # 尝试获取流通市值作为总市值的近似
                            if mktcap_e == 0.0:
                                for key in ['流通市值', 'circularMarketValue', 'mktcap']:
                                    if key in basic_info and basic_info[key]:
                                        try:
                                            val = float(basic_info[key])
                                            if val > 1e9:  # 元转亿
                                                mktcap_e = val / 1e8
                                            else:
                                                mktcap_e = val
                                            break
                                        except:
                                            pass
                    
                    #方案3：如果还是没有，尝试简单估算
                    if mktcap_e == 0.0:
                        # 使用价格×总股本估算（如果有的话）
                        if 'total_share' in basic_info or '总股本' in basic_info:
                            try:
                                shares = float(basic_info.get('total_share') or basic_info.get('总股本', 0))
                                if shares > 0:
                                    mktcap_e = (last * shares) / 1e8  # 亿元
                            except:
                                pass
                    
                    if pe > 0 or mktcap_e > 0:
                        self.logger.debug(f"{code} {name} 估值: PE={pe:.2f}, 市值={mktcap_e:.2f}亿")
                    else:
                        self.logger.debug(f"{code} {name} 无法获取估值数据")
                except Exception as e:
                    self.logger.debug(f"{code} 获取估值失败: {e}")
                
                # 近20日动量
                base = float(close.iloc[-21]) if len(close) >= 21 else last
                momentum = (last - base) / base if base > 1e-8 else 0.0
                # 简易波动（近20日对数收益的标准差）
                rets = np.diff(np.log(close.values))
                vol = float(np.std(rets[-20:])) if len(rets) >= 20 else float(np.std(rets))

                # 评分：动量越高越好，波动越低越好
                mom_score = max(0.0, min(1.0, (momentum + 0.20) / 0.40))  # 约 -20%~+20% 映射到 0~1
                vol_score = 1.0 - max(0.0, min(1.0, vol / 0.06))          # 年化约化简，粗略阈值
                
                # 尝试获取情绪分数
                sentiment_score = 50.0  # 默认中性
                try:
                    # 简化情绪分析：基于涨跌幅和成交量
                    if len(close) >= 5:
                        # 近5日涨跌趋势
                        recent_5d_chg = (float(close.iloc[-1]) - float(close.iloc[-6])) / float(close.iloc[-6]) if len(close) >= 6 else 0.0
                        
                        # 成交量变化（如果有volume列）
                        volume_factor = 0.0
                        if 'volume' in df.columns:
                            vol_data = pd.to_numeric(df['volume'], errors='coerce').dropna()
                            if len(vol_data) >= 10:
                                recent_vol_avg = float(vol_data.tail(5).mean())
                                prev_vol_avg = float(vol_data.tail(10).head(5).mean())
                                if prev_vol_avg > 0:
                                    volume_factor = (recent_vol_avg - prev_vol_avg) / prev_vol_avg
                        
                        # 情绪得分：趋势+成交量
                        # 涨跌幅贡献：-10%~+10% 映射到 30~70
                        trend_contrib = max(30.0, min(70.0, 50.0 + recent_5d_chg * 200))
                        # 成交量贡献：-50%~+50% 映射到 -10~+10
                        vol_contrib = max(-10.0, min(10.0, volume_factor * 20))
                        
                        sentiment_score = max(0.0, min(100.0, trend_contrib + vol_contrib))
                        self.logger.debug(f"{code} 情绪分析: 趋势={recent_5d_chg*100:.2f}%, 成交量变化={volume_factor*100:.2f}%, 得分={sentiment_score:.2f}")
                except Exception as e:
                    self.logger.debug(f"{code} 情绪分析失败: {e}")
                
                # 如果有PE数据，加入估值因子
                if pe > 0:
                    pe_eff = max(0.0, min(120.0, float(pe)))
                    pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))  # PE越低越好
                    fundamental_score = pe_score * 100.0
                else:
                    fundamental_score = 50.0

                # 技术面得分（动量+波动）
                technical_score = mom_score * 100.0
                
                # 构建三维评分
                quick_scores = {
                    'technical': technical_score,
                    'fundamental': fundamental_score,
                    'sentiment': sentiment_score,
                }
                # 使用系统的综合评分方法（会自动应用权重）
                quick_scores['comprehensive'] = self.calculate_comprehensive_score(quick_scores)
                rec = self.generate_recommendation(quick_scores)
                
                # 最终得分使用综合得分
                final_score = float(round(quick_scores['comprehensive'], 2))

                out.append({
                    'stock_code': code,
                    'stock_name': name,
                    'latest_price': float(round(last, 3)),
                    'change_pct': float(round(daily_chg_pct, 3)),
                    'pe': float(round(pe, 2)) if pe > 0 else 0.0,
                    'mktcap_e': float(round(mktcap_e, 2)) if mktcap_e > 0 else 0.0,
                    'score': final_score,
                    'recommendation': rec
                })
                
                scanned += 1
                if scanned % 10 == 0:
                    self.logger.info(f"⏳ 已扫描 {scanned}/{len(cands)}，已收集 {len(out)} 只有效股票")
                    
            except Exception as e:
                self.logger.debug(f"处理 {code} 失败: {e}")
                continue

        if not out:
            self.logger.warning("离线模式未找到任何有效数据")
            return []
            
        out.sort(key=lambda x: x.get('score', 0.0), reverse=True)
        self.logger.info(f"✅ 离线模式完成，生成 {len(out)} 条推荐")
        return out[: max(1, int(top_n))]

    # ===== 规则筛选推荐（7日窗口）=====
    def _fetch_recent_ohlcv_light(self, stock_code: str, window_days: int = 25) -> pd.DataFrame:
        """轻量获取近端日K数据，用于规则筛选。优先akshare，失败回退缓存与通用接口。

        参数：
            window_days: 需要的交易日数量（不是自然日），默认25个交易日
        
        返回：DataFrame包含列：open, high, low, close, volume，索引为日期升序。
        
        注意：返回的数据为交易日数据，已自动排除周末和节假日。
        """
        try:
            import akshare as ak
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=max(30, int(window_days) * 3))).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(
                symbol=str(stock_code),
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            if df is None or df.empty:
                raise RuntimeError("akshare返回空")
            # 标准化列
            mapping = {
                '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'
            }
            for c_cn, c_en in mapping.items():
                if c_cn in df.columns:
                    df[c_en] = df[c_cn]
            # 成交量单位转股：ak是手
            if 'volume' in df.columns:
                try:
                    df['volume'] = pd.to_numeric(df['volume'], errors='coerce') * 100.0
                except Exception:
                    pass
            # 索引与数值化
            try:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                else:
                    df.index = pd.to_datetime(df.index)
            except Exception:
                pass
            for c in ['open','high','low','close','volume']:
                if c in df.columns:
                    try:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                    except Exception:
                        pass
            df = df[['open','high','low','close','volume']].dropna()
            df = df.sort_index()
            # 返回最近的N个交易日数据（+5是为了确保有足够的数据进行分析）
            # 注意：akshare返回的日K数据已经是交易日，自动排除了周末和节假日
            result_df = df.tail(int(window_days) + 5)
            self.logger.debug(f"获取到 {len(result_df)} 个交易日的K线数据，用于分析")
            return result_df
        except Exception:
            # 回退：使用通用缓存的get_stock_data
            try:
                base = self.get_stock_data(stock_code)
                if base is None or base.empty:
                    return pd.DataFrame()
                cols = [c for c in ['open','high','low','close','volume'] if c in base.columns]
                if not cols:
                    return pd.DataFrame()
                df = base[cols].copy()
                if not isinstance(df.index, pd.DatetimeIndex):
                    try:
                        df.index = pd.to_datetime(base.get('date', base.index))
                    except Exception:
                        pass
                df = df.sort_index().tail(int(window_days) + 5)
                return df
            except Exception:
                return pd.DataFrame()

    def _check_7day_rule(self, df: pd.DataFrame) -> Tuple[bool, float, dict]:
        """在给定的近端OHLCV数据上检查7日规则，返回(是否满足, 规则得分0-100, 诊断信息)。

        规则口径（结合用户描述，做合理工程化近似）：
        - 取最近7个交易日作为窗口d1..d7（d7为最近日）。
        - 第2日为阴线且第3日为阳线（阴→阳），视作“第3日走阴阳线”的组合信号。
        - 第4日最高价 > 第3日最高价（突破确认，允许最小0.3%容差）。
        - 成交量整体呈“前低后高或波动放大”：
          a) 后三日(5-7)总量 > 前三日(1-3)总量；或
          b) 成交量对时间的回归斜率>0；或
          c) 成交量的变异系数(std/mean) >= 0.28。
        - 附加细则（加分项）：
          • 窗口内最大量当日的涨跌为正；
          • 最小量出现在下跌日；
          • 窗口内有1-2日实体较小(十字/横盘)，但伴随放量。
        """
        diag = {}
        try:
            if df is None or df.empty or len(df) < 7:
                return False, 0.0, {'reason': '数据不足'}
            
            # 确保索引为日期类型，并排序
            if not isinstance(df.index, pd.DatetimeIndex):
                try:
                    df.index = pd.to_datetime(df.index)
                except Exception:
                    return False, 0.0, {'reason': '日期索引转换失败'}
            
            # 排序并取最近7个交易日（K线数据本身就是交易日，无需额外过滤）
            df_sorted = df.sort_index(ascending=True)
            
            # 取最近7个交易日（注意：K线数据本身已是交易日，自动排除了周末和节假日）
            w = df_sorted.tail(7).copy()
            
            # 数据有效性验证：确保有7个不同的交易日
            if len(w) < 7:
                return False, 0.0, {'reason': f'交易日数量不足，仅有{len(w)}个交易日'}
            
            # 记录用于分析的交易日期范围（便于调试）
            try:
                date_range = f"{w.index[0].strftime('%Y-%m-%d')} 至 {w.index[-1].strftime('%Y-%m-%d')}"
                diag['trading_date_range'] = date_range
                diag['trading_days_count'] = len(w)
            except Exception:
                pass
            
            w = w[['open','high','low','close','volume']].astype(float)
            w.index = pd.to_datetime(w.index)
            o = w['open'].values
            h = w['high'].values
            l = w['low'].values
            c = w['close'].values
            v = w['volume'].values

            # 阴→阳（d2阴, d3阳）
            cond_yinyang = (c[1] < o[1]) and (c[2] > o[2])
            # d4 突破 d3 high，允许0.3%容差
            cond_break = h[3] > (h[2] * 1.003)

            # 量能趋势：后三日量 > 前三日量 或 斜率>0 或 变异系数高
            front = float(v[0] + v[1] + v[2])
            back = float(v[4] + v[5] + v[6])
            cond_back_higher = back > front * 1.05
            # 简单回归斜率
            try:
                x = np.arange(len(v))
                slope = np.polyfit(x, v, 1)[0]
            except Exception:
                slope = 0.0
            cond_slope_pos = slope > 0
            # 变异系数
            vmean = float(np.mean(v)) if np.isfinite(np.mean(v)) else 0.0
            vstdev = float(np.std(v)) if np.isfinite(np.std(v)) else 0.0
            cond_cv = (vmean > 0) and ((vstdev / vmean) >= 0.28)

            volume_trend = cond_back_higher or cond_slope_pos or cond_cv

            base_ok = cond_yinyang and cond_break and volume_trend

            # 加分项
            bonus = 0.0
            # 最大/最小量日位置与涨跌
            try:
                max_idx = int(np.argmax(v))
                min_idx = int(np.argmin(v))
            except Exception:
                max_idx = 0
                min_idx = 0
            ret = np.diff(c, prepend=c[0]) / np.maximum(1e-6, np.concatenate([[c[0]], c[:-1]]))
            if max_idx >= 0 and max_idx < len(ret) and ret[max_idx] > 0:
                bonus += 8.0
            if min_idx >= 0 and min_idx < len(ret) and ret[min_idx] < 0:
                bonus += 4.0

            # 小实体放量（日内振幅占收盘价的比例 < 1.2%，但相对量能>窗口均值）
            body = np.abs(c - o)
            body_ratio = body / np.maximum(1e-6, c)
            amp = (h - l) / np.maximum(1e-6, c)
            for i in range(len(w)):
                if body_ratio[i] <= 0.012 and v[i] >= vmean * 1.15 and amp[i] <= 0.035:
                    bonus += 3.0
                    break

            # 规则评分：
            score = 0.0
            if cond_yinyang:
                score += 35.0
            if cond_break:
                score += 30.0
            if volume_trend:
                score += 20.0
            score += bonus
            score = float(max(0.0, min(100.0, score)))

            diag.update({
                'yinyang': bool(cond_yinyang),
                'break_high': bool(cond_break),
                'volume_trend': bool(volume_trend),
                'bonus': float(bonus)
            })
            return bool(base_ok), score, diag
        except Exception as e:
            return False, 0.0, {'error': str(e)}

    def get_rule_based_recommendations(self, top_n: int = 20, min_mktcap_e: float = 30.0, exclude_st: bool = True) -> List[Dict]:
        """按7日价格/量能规则筛选推荐列表。

        流程：
        1) 取A股快照做“候选池”（过滤ST、退市、总市值下限）。
        2) 对候选逐股拉取近端日K（约25天），检测7日规则。
        3) 对满足规则的，结合快照动量/估值做综合排序，返回TopN。
        """
        try:
            import akshare as ak
        except Exception:
            ak = None

        # 候选池（优先使用 BaoStock 快照，与快速推荐一致）
        spot = pd.DataFrame()
        
        # 1. 优先尝试 BaoStock 快照
        try:
            spot = self._get_baostock_snapshot()
            if spot is not None and not spot.empty:
                self.logger.info(f"[规则推荐] 使用 BaoStock 快照，{len(spot)} 只股票")
        except Exception as e:
            self.logger.warning(f"[规则推荐] BaoStock 快照失败: {e}")
        
        # 2. 如果 BaoStock 失败，尝试 akshare
        if (spot is None or spot.empty) and ak is not None:
            try:
                spot = ak.stock_zh_a_spot_em()
                if spot is not None and not spot.empty:
                    self.logger.info(f"[规则推荐] 使用 akshare 快照，{len(spot)} 只股票")
                    try:
                        self._save_spot_snapshot_cache(spot)
                    except Exception:
                        pass
            except Exception:
                spot = pd.DataFrame()
        
        # 3. 如果都失败，尝试本地缓存
        if (spot is None) or spot.empty:
            spot = self._load_spot_snapshot_cache()
            if spot is not None and not spot.empty:
                self.logger.info(f"[规则推荐] 使用本地缓存，{len(spot)} 只股票")
        
        if spot is None or spot.empty:
            # 离线兜底：直接用BaoStock构造候选池（仅代码/名称），后续逐股拉K做规则判定
            self.logger.warning("无法获取A股快照，规则推荐改用BaoStock候选池")
            df = None
            candidates = []
            try:
                pool = self._get_candidate_codes_via_baostock(exclude_st=exclude_st, limit=max(200, int(top_n) * 40))
                for it in pool:
                    candidates.append({
                        'stock_code': it['code'],
                        'stock_name': it.get('name') or it['code'],
                        'mktcap_e': 0.0,
                        'change_pct': 0.0,
                        'latest_price': 0.0,
                        'pe': 60.0  # 合理默认用于排序
                    })
            except Exception:
                pass
            if not candidates:
                return []

            # 直接进入扫描阶段
            passed = []
            scan_cap = min(len(candidates), max(200, int(top_n) * 40))
            pool = candidates[:scan_cap]
            for item in pool:
                code = item['stock_code']
                df_k = self._fetch_recent_ohlcv_light(code, window_days=25)
                if df_k is None or df_k.empty or len(df_k) < 7:
                    continue
                ok, rule_score, diag = self._check_7day_rule(df_k)
                if not ok:
                    continue
                # 快速分（无PE/市值，使用默认）
                chg = float(item.get('change_pct', 0.0))
                mom = max(0.0, min(1.0, (chg + 5.0) / 10.0))
                pe_eff = max(0.0, min(120.0, float(item.get('pe', 60.0))))
                pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
                x = float(item.get('mktcap_e', 100.0))
                if x <= 30:
                    mcap_score = 0.2
                elif x <= 80:
                    mcap_score = 0.6 + (x - 80.0) / 50.0 * 0.4
                elif x <= 300:
                    mcap_score = 1.0 - abs((x - 190.0) / 110.0) * 0.4
                elif x <= 800:
                    mcap_score = 0.8 - (x - 300.0) / 500.0 * 0.6
                else:
                    mcap_score = 0.2
                mcap_score = max(0.0, min(1.0, mcap_score))
                quick01 = 0.45 * mom + 0.35 * pe_score + 0.20 * mcap_score
                quick_score = float(round(quick01 * 100.0, 2))

                combo_score = float(round(0.7 * rule_score + 0.3 * quick_score, 2))
                quick_scores = {
                    'technical': float(rule_score),
                    'fundamental': pe_score * 100.0,
                    'sentiment': 55.0,
                }
                quick_scores['comprehensive'] = self.calculate_comprehensive_score(quick_scores)
                rec = self.generate_recommendation(quick_scores)

                passed.append({
                    'stock_code': item['stock_code'],
                    'stock_name': item['stock_name'],
                    'latest_price': float(item.get('latest_price', 0.0)),
                    'change_pct': float(item.get('change_pct', 0.0)),
                    'pe': float(item.get('pe', 60.0)),
                    'mktcap_e': float(item.get('mktcap_e', 0.0)),
                    'score': combo_score,
                    'recommendation': rec
                })

            if not passed:
                return []
            passed.sort(key=lambda x: x.get('score', 0.0), reverse=True)
            return passed[: max(1, int(top_n))]

        df = spot.copy()

        def pick_col(cands: List[str]) -> Optional[str]:
            for c in cands:
                if c in df.columns:
                    return c
            return None

        code_col = pick_col(['代码','股票代码','证券代码','A股代码','code']) or df.columns[0]
        name_col = pick_col(['名称','股票简称','简称','name']) or (df.columns[1] if len(df.columns) > 1 else code_col)
        pct_col  = pick_col(['涨跌幅','涨跌幅(%)','涨跌幅%','pct_chg'])
        price_col= pick_col(['最新价','现价','价格','最新'])
        pe_col   = pick_col(['市盈率-动态','市盈率TTM','市盈率'])
        mcap_col = pick_col(['总市值(亿)','总市值'])

        def to_float(x) -> Optional[float]:
            try:
                if isinstance(x, str):
                    xs = x.strip().replace(',', '')
                    if xs.endswith('%'):
                        return float(xs.rstrip('%'))
                    return float(xs)
                return float(x)
            except Exception:
                return None

        # 预筛 + 控制规模
        candidates = []
        for _, r in df.iterrows():
            try:
                code = str(r.get(code_col, '')).strip()
                if not code:
                    continue
                code = code[-6:]
                # 过滤异常代码（必须为6位数字）
                if len(code) != 6 or (not code.isdigit()):
                    continue
                name = str(r.get(name_col, code))
                if exclude_st and any(x in str(name) for x in ['ST','*ST','退']):
                    continue
                mktcap_e = to_float(r.get(mcap_col)) if mcap_col else None
                if mktcap_e is not None and mcap_col == '总市值' and mktcap_e > 1e9:
                    mktcap_e = mktcap_e / 1e8
                if (mktcap_e is None) or (mktcap_e < min_mktcap_e):
                    continue
                chg = to_float(r.get(pct_col)) if pct_col else 0.0
                price = to_float(r.get(price_col)) if price_col else 0.0
                pe = to_float(r.get(pe_col)) if pe_col else None
                if pe is None or pe <= 0 or pe > 1200:
                    continue
                candidates.append({
                    'stock_code': code,
                    'stock_name': name,
                    'mktcap_e': float(mktcap_e),
                    'change_pct': float(chg) if chg is not None else 0.0,
                    'latest_price': float(price) if price is not None else 0.0,
                    'pe': float(pe)
                })
            except Exception:
                continue

        if not candidates:
            return []

        # 限制扫描规模：优先动量+中等体量（简单排序后取前K）
        candidates.sort(key=lambda x: (abs(x.get('change_pct', 0.0)) * 0.6 + (1.0 / (1.0 + abs(x.get('pe', 50.0)))) * 0.4), reverse=True)
        scan_cap = min(len(candidates), max(200, int(top_n) * 40))  # 扫描上限
        pool = candidates[:scan_cap]

        passed = []
        for item in pool:
            code = item['stock_code']
            df_k = self._fetch_recent_ohlcv_light(code, window_days=25)
            if df_k is None or df_k.empty or len(df_k) < 7:
                continue
            ok, rule_score, diag = self._check_7day_rule(df_k)
            if not ok:
                continue
            # 快照分（与快速推荐一致）
            chg = float(item.get('change_pct', 0.0))
            mom = max(0.0, min(1.0, (chg + 5.0) / 10.0))
            pe_eff = max(0.0, min(120.0, float(item.get('pe', 60.0))))
            pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
            x = float(item.get('mktcap_e', 100.0))
            if x <= 30:
                mcap_score = 0.2
            elif x <= 80:
                mcap_score = 0.6 + (x - 80.0) / 50.0 * 0.4
            elif x <= 300:
                mcap_score = 1.0 - abs((x - 190.0) / 110.0) * 0.4
            elif x <= 800:
                mcap_score = 0.8 - (x - 300.0) / 500.0 * 0.6
            else:
                mcap_score = 0.2
            mcap_score = max(0.0, min(1.0, mcap_score))
            quick01 = 0.45 * mom + 0.35 * pe_score + 0.20 * mcap_score
            quick_score = float(round(quick01 * 100.0, 2))

            # 综合排序分：规则为主
            combo_score = float(round(0.7 * rule_score + 0.3 * quick_score, 2))

            # 生成建议：将规则分视作技术分
            quick_scores = {
                'technical': float(rule_score),
                'fundamental': pe_score * 100.0,
                'sentiment': 55.0,
            }
            quick_scores['comprehensive'] = self.calculate_comprehensive_score(quick_scores)
            rec = self.generate_recommendation(quick_scores)

            passed.append({
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'latest_price': float(item.get('latest_price', 0.0)),
                'change_pct': float(item.get('change_pct', 0.0)),
                'pe': float(item.get('pe', np.nan)),
                'mktcap_e': float(item.get('mktcap_e', np.nan)),
                'score': combo_score,
                'recommendation': rec
            })

        if not passed:
            return []
        passed.sort(key=lambda x: x.get('score', 0.0), reverse=True)
        return passed[: max(1, int(top_n))]

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

**新闻数据详情：**
- 公司新闻：{len(company_news)}条
- 公司公告：{len(announcements)}条  
- 研究报告：{len(research_reports)}条
- 总新闻数：{news_summary.get('total_news_count', 0)}条

**重要新闻标题（前10条）：**
"""
        
        for i, news in enumerate(company_news[:5], 1):
            prompt += f"{i}. {news.get('title', '未知标题')}\n"
        
        for i, announcement in enumerate(announcements[:5], 1):
            prompt += f"{i+5}. [公告] {announcement.get('title', '未知标题')}\n"
        
        # 提取研究报告信息
        if research_reports:
            prompt += "\n**研究报告摘要：**\n"
            for i, report in enumerate(research_reports[:5], 1):
                prompt += f"{i}. {report.get('institution', '未知机构')}: {report.get('rating', '未知评级')} - {report.get('title', '未知标题')}\n"
        
        # 构建完整的提示词
        prompt += f"""

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

            # 提前计算近月资金流向，并对技术面评分做轻量级修正
            try:
                capital_flow = self.calculate_capital_flow(stock_code, price_data, None, window_days=30)
                status = (capital_flow or {}).get('status', '')
                # 依据主力净流状态微调技术面得分（±6内）
                if isinstance(status, str):
                    if '主力净流入强' in status:
                        technical_score = float(min(100.0, technical_score + 6))
                    elif '主力净流入中' in status:
                        technical_score = float(min(100.0, technical_score + 3))
                    elif '主力净流入弱' in status:
                        technical_score = float(min(100.0, technical_score + 1))
                    elif '主力净流出较强' in status:
                        technical_score = float(max(0.0, technical_score - 6))
                    elif '主力净流出' in status:
                        technical_score = float(max(0.0, technical_score - 3))
            except Exception as e:
                self.logger.info(f"资金流修正跳过: {e}")
                capital_flow = {'status': '数据不足', 'note': str(e)}
            
            # 2. 获取25项财务指标和综合基本面分析
            self.logger.info("正在进行25项财务指标分析...")
            fundamental_data = self.get_comprehensive_fundamental_data(stock_code)
            fundamental_score = self.calculate_fundamental_score(fundamental_data)
            # 2.1 使用基本面市值信息，重新计算资金流（用于报告/AI更精准的强度分级）
            try:
                capital_flow = self.calculate_capital_flow(stock_code, price_data, fundamental_data, window_days=30)
            except Exception as e:
                self.logger.info(f"资金流复算失败: {e}")
            
            # 3. 获取综合新闻数据和高级情绪分析
            self.logger.info("正在进行综合新闻和情绪分析...")
            comprehensive_news_data = self.get_comprehensive_news_data(stock_code, days=30)
            sentiment_analysis = self.calculate_advanced_sentiment_analysis(comprehensive_news_data)
            sentiment_score = self.calculate_sentiment_score(sentiment_analysis)
            
            # 合并新闻数据到情绪分析结果中，方便AI分析使用
            sentiment_analysis.update(comprehensive_news_data)
            
            # 4. 计算综合得分（已包含资金流对技术面的修正）
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
                'capital_flow': capital_flow,
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
                'capital_flow': capital_flow,
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

    def _latest_dt(self, df, date_candidates: List[str]):
        """返回数据框中候选日期列的最新日期，用于新鲜度判断"""
        try:
            for c in date_candidates:
                if c in df.columns:
                    s = pd.to_datetime(df[c], errors='coerce')
                    if s.notna().any():
                        return s.max()
        except Exception:
            pass
        return None

    def _is_data_stale(self, df, date_candidates: List[str], max_days: int, what: str) -> bool:
        """判断数据是否过旧，超过max_days则视为过期"""
        dt = self._latest_dt(df, date_candidates)
        if dt is None:
            self.logger.warning(f"{what} 未找到日期列，无法判断新鲜度")
            return False
        try:
            delta = (datetime.now() - dt.to_pydatetime()).days
        except Exception:
            delta = 99999
        self.logger.info(f"{what} 最新日期: {getattr(dt, 'date', lambda: dt)()} (距今 {delta} 天)")
        if delta > max_days:
            self.logger.warning(f"{what} 数据过旧(>{max_days}天)，忽略该接口结果")
            return True
        return False

    def _normalize_dividend_records(self, records: List[dict]) -> List[dict]:
        """标准化分红记录结构，确保进入模型的键一致且精简"""
        norm: List[dict] = []
        for x in records or []:
            year = str(x.get('year') or x.get('dividYear') or x.get('报告年度') or x.get('报告时间') or '')
            per = x.get('dividend_per_share') or x.get('dividCashPsBeforeTax') or x.get('每股派息(税前)') or x.get('派息比例')
            try:
                per = float(per)
            except Exception:
                per = None
            exd = x.get('ex_dividend_date') or x.get('dividOperateDate') or x.get('除权除息日') or x.get('除权日') or ''
            src = x.get('source') or 'unknown'
            item = {
                'year': year,
                'dividend_per_share': per,
                'ex_dividend_date': str(exd),
                'source': src,
            }
            if per is not None:
                norm.append(item)
        return norm

def main():
    """主函数：打印推荐列表所有关键字段和得分"""
    print("\n=== 快速推荐列表（调试输出） ===")
    try:
        analyzer = EnhancedStockAnalyzer()
        recs = analyzer.get_quick_recommendations()
    except Exception as e:
        print(f"获取推荐列表失败: {e}")
        return
    if not recs:
        print("无推荐数据。")
    else:
        for i, rec in enumerate(recs, 1):
            print(f"[{i}] 股票代码: {rec.get('stock_code', '')}")
            print(f"    名称: {rec.get('stock_name', '')}")
            print(f"    当前价格: {rec.get('latest_price', '')}")
            print(f"    涨跌幅: {rec.get('change_pct', '')}")
            print(f"    PE: {rec.get('pe', '')}")
            print(f"    总市值(亿): {rec.get('mktcap_e', '')}")
            print(f"    得分: {rec.get('score', '')}")
            print(f"    建议: {rec.get('recommendation', '')}")
            print("---------------------------")
    print("=== 推荐列表输出结束 ===\n")

if __name__ == "__main__":
    main()
