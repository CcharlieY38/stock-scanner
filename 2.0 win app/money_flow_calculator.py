"""
自计算资金流模块 - 基于成交量和价格变化
不依赖外部API，使用K线数据直接计算
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class MoneyFlowCalculator:
    """资金流计算器"""
    
    def __init__(self):
        pass
    
    def calculate_money_flow(self, df, window_days=30):
        """
        基于K线数据计算资金流
        
        参数:
            df: K线数据，需包含 date, open, high, low, close, volume
            window_days: 计算窗口（天）
        
        返回:
            dict: 资金流分析结果
        """
        try:
            # 数据准备
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 确保数值类型
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 截取窗口
            cutoff = datetime.now() - timedelta(days=window_days)
            df_window = df[df['date'] >= cutoff].copy()
            
            if len(df_window) < 5:
                return self._empty_result("数据不足")
            
            # 1. 计算每日资金流
            df_window['money_flow'] = self._calculate_daily_flow(df_window)
            
            # 2. 按力度分类（模拟特大单、大单、中单、小单）
            df_window = self._classify_flow_by_strength(df_window)
            
            # 3. 计算主力净流入
            main_flow = self._calculate_main_flow(df_window)
            
            # 4. 统计正负天数
            stats = self._calculate_flow_stats(df_window)
            
            # 5. 生成结果
            result = {
                'main_net_inflow': main_flow['net_inflow'],
                'main_net_inflow_ratio': main_flow['net_ratio'],
                'main_status': main_flow['status'],
                'flow_details': {
                    'super_large': stats['super_large'],
                    'large': stats['large'],
                    'medium': stats['medium'],
                    'small': stats['small'],
                },
                'total_volume': float(df_window['volume'].sum()),
                'total_amount': float(df_window['amount'].sum()) if 'amount' in df_window.columns else 0,
                'positive_days': stats['positive_days'],
                'negative_days': stats['negative_days'],
                'source': 'calculated',
                'note': '基于K线数据计算'
            }
            
            return result
            
        except Exception as e:
            return self._empty_result(f"计算失败: {str(e)}")
    
    def _calculate_daily_flow(self, df):
        """
        计算每日资金流
        算法：成交量 × 平均价 × 价格变化方向
        """
        # 平均价 = (最高 + 最低 + 收盘) / 3
        avg_price = (df['high'] + df['low'] + df['close']) / 3
        
        # 价格变化
        price_change = df['close'] - df['open']
        
        # 成交金额 = 成交量 × 平均价
        df['amount'] = df['volume'] * avg_price
        
        # 资金流方向：涨为正，跌为负
        flow_direction = np.sign(price_change)
        
        # 资金流强度：价格变化幅度 × 成交量放大系数
        change_ratio = price_change / df['open']
        volume_ratio = df['volume'] / df['volume'].rolling(5, min_periods=1).mean()
        strength = np.abs(change_ratio) * volume_ratio
        
        # 资金流 = 成交金额 × 方向 × 强度
        money_flow = df['amount'] * flow_direction * strength
        
        return money_flow
    
    def _classify_flow_by_strength(self, df):
        """
        按资金流强度分类（模拟大单小单）
        特大单：强度 > 2
        大单：强度 1-2
        中单：强度 0.5-1
        小单：强度 < 0.5
        """
        flow_abs = np.abs(df['money_flow'])
        flow_percentile_75 = flow_abs.quantile(0.75)
        flow_percentile_50 = flow_abs.quantile(0.50)
        flow_percentile_25 = flow_abs.quantile(0.25)
        
        # 分类
        df['super_large'] = df['money_flow'].where(flow_abs >= flow_percentile_75, 0)
        df['large'] = df['money_flow'].where(
            (flow_abs >= flow_percentile_50) & (flow_abs < flow_percentile_75), 0
        )
        df['medium'] = df['money_flow'].where(
            (flow_abs >= flow_percentile_25) & (flow_abs < flow_percentile_50), 0
        )
        df['small'] = df['money_flow'].where(flow_abs < flow_percentile_25, 0)
        
        return df
    
    def _calculate_main_flow(self, df):
        """
        计算主力资金流（特大单+大单）
        """
        main_flow = df['super_large'] + df['large']
        net_inflow = float(main_flow.sum())
        
        total_amount = float(df['amount'].sum()) if 'amount' in df.columns else 1
        net_ratio = (net_inflow / total_amount * 100) if total_amount > 0 else 0
        
        # 判断状态
        if net_ratio > 5:
            status = "强势流入"
        elif net_ratio > 1:
            status = "持续流入"
        elif net_ratio > -1:
            status = "平衡"
        elif net_ratio > -5:
            status = "持续流出"
        else:
            status = "强势流出"
        
        return {
            'net_inflow': net_inflow,
            'net_ratio': net_ratio,
            'status': status
        }
    
    def _calculate_flow_stats(self, df):
        """
        统计各档位的详细数据
        """
        def calc_detail(flow_col):
            net = float(df[flow_col].sum())
            total_amount = float(df['amount'].sum()) if 'amount' in df.columns else 1
            ratio = (net / total_amount * 100) if total_amount > 0 else 0
            pos_days = int((df[flow_col] > 0).sum())
            neg_days = int((df[flow_col] < 0).sum())
            
            if ratio > 3:
                status = "强势流入"
            elif ratio > 0:
                status = "流入"
            elif ratio > -3:
                status = "流出"
            else:
                status = "强势流出"
            
            return {
                'net': net,
                'ratio': ratio,
                'positive_days': pos_days,
                'negative_days': neg_days,
                'status': status
            }
        
        return {
            'super_large': calc_detail('super_large'),
            'large': calc_detail('large'),
            'medium': calc_detail('medium'),
            'small': calc_detail('small'),
            'positive_days': int((df['money_flow'] > 0).sum()),
            'negative_days': int((df['money_flow'] < 0).sum())
        }
    
    def _empty_result(self, note):
        """返回空结果"""
        return {
            'main_net_inflow': 0,
            'main_net_inflow_ratio': 0,
            'main_status': '数据不足',
            'flow_details': {
                'super_large': {'net': 0, 'ratio': 0, 'positive_days': 0, 'negative_days': 0, 'status': '-'},
                'large': {'net': 0, 'ratio': 0, 'positive_days': 0, 'negative_days': 0, 'status': '-'},
                'medium': {'net': 0, 'ratio': 0, 'positive_days': 0, 'negative_days': 0, 'status': '-'},
                'small': {'net': 0, 'ratio': 0, 'positive_days': 0, 'negative_days': 0, 'status': '-'},
            },
            'total_volume': 0,
            'total_amount': 0,
            'positive_days': 0,
            'negative_days': 0,
            'source': 'none',
            'note': note
        }


# 测试函数
def test_money_flow_calculator():
    """测试资金流计算"""
    import baostock as bs
    
    print("=" * 70)
    print(" 测试自计算资金流（603626）")
    print("=" * 70)
    
    # 登录BaoStock
    lg = bs.login()
    print(f"\n✅ BaoStock登录: {lg.error_msg}")
    
    # 获取K线数据
    code = "sh.603626"
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,volume",
        start_date='2024-09-01',
        end_date='2025-10-29',
        frequency="d",
        adjustflag="2"
    )
    
    if rs.error_code == '0':
        data = []
        while (rs.error_code == '0') & rs.next():
            data.append(rs.get_row_data())
        df = pd.DataFrame(data, columns=rs.fields)
        
        print(f"✅ 获取 {len(df)} 条K线数据\n")
        
        # 计算资金流
        calculator = MoneyFlowCalculator()
        result = calculator.calculate_money_flow(df, window_days=30)
        
        # 显示结果
        print("【主力资金流向】")
        print(f"  净流入: {result['main_net_inflow']/1e8:.2f} 亿")
        print(f"  净流入/成交额: {result['main_net_inflow_ratio']:.2f}%")
        print(f"  状态: {result['main_status']}")
        print(f"  正天数/负天数: {result['positive_days']}/{result['negative_days']}")
        
        print("\n【分档净流入明细】")
        details = result['flow_details']
        for name, key in [('特大单', 'super_large'), ('大单', 'large'), 
                         ('中单', 'medium'), ('小单', 'small')]:
            d = details[key]
            print(f"  {name}:")
            print(f"    净额: {d['net']/1e8:.2f} 亿")
            print(f"    净/成交额: {d['ratio']:.2f}%")
            print(f"    正天数/负天数: {d['positive_days']}/{d['negative_days']}")
            print(f"    状态: {d['status']}")
    
    bs.logout()
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)


if __name__ == "__main__":
    test_money_flow_calculator()
