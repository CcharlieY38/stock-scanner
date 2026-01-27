"""
简化版603626测试 - 直接测试数据接口
"""
import os
# 清除可能的代理设置
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]
        print(f"已清除环境变量: {key}")

print("=" * 60)
print("简化版测试：股票 603626")
print("=" * 60)

# 测试1：BaoStock
print("\n【测试1】BaoStock K线数据")
try:
    import baostock as bs
    import pandas as pd
    
    lg = bs.login()
    print(f"BaoStock登录: {lg.error_msg}")
    
    # 尝试不同代码格式
    codes_to_test = ["sh.603626", "603626"]
    
    for code in codes_to_test:
        print(f"\n尝试代码: {code}")
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume,turn",
            start_date='2024-10-01',
            end_date='2025-10-29',
            frequency="d",
            adjustflag="2"
        )
        
        if rs.error_code == '0':
            data = []
            while (rs.error_code == '0') & rs.next():
                data.append(rs.get_row_data())
            df = pd.DataFrame(data, columns=rs.fields)
            print(f"✅ 成功获取 {len(df)} 条数据")
            if len(df) > 0:
                print(f"最新数据:")
                print(df.tail(3)[['date', 'close', 'volume']])
                
                # 计算涨跌幅
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                if len(df) >= 2:
                    last = float(df.iloc[-1]['close'])
                    prev = float(df.iloc[-2]['close'])
                    chg_pct = ((last - prev) / prev) * 100.0
                    print(f"\n最新价: {last:.2f} 元")
                    print(f"涨跌幅: {chg_pct:+.2f}%")
                break
        else:
            print(f"❌ BaoStock错误: {rs.error_msg}")
    
    bs.logout()
except Exception as e:
    print(f"❌ BaoStock异常: {e}")

# 测试2：akshare
print("\n【测试2】akshare 实时行情")
try:
    import akshare as ak
    
    # 获取沪深A股实时行情
    print("获取A股实时行情...")
    df_realtime = ak.stock_zh_a_spot_em()
    
    # 查找603626
    stock_data = df_realtime[df_realtime['代码'] == '603626']
    
    if not stock_data.empty:
        print(f"✅ 找到603626实时数据:")
        row = stock_data.iloc[0]
        print(f"  名称: {row['名称']}")
        print(f"  最新价: {row['最新价']:.2f} 元")
        print(f"  涨跌幅: {row['涨跌幅']:.2f}%")
        print(f"  成交量: {row['成交量']}")
        print(f"  市盈率: {row.get('市盈率-动态', 'N/A')}")
        print(f"  总市值: {row.get('总市值', 'N/A')}")
    else:
        print("❌ 未找到603626数据")
        
except Exception as e:
    print(f"❌ akshare异常: {e}")
    import traceback
    traceback.print_exc()

# 测试3：akshare历史数据
print("\n【测试3】akshare 历史K线")
try:
    import akshare as ak
    
    df = ak.stock_zh_a_hist(
        symbol="603626",
        period="daily",
        start_date="20241001",
        end_date="20251029",
        adjust="qfq"
    )
    
    if df is not None and not df.empty:
        print(f"✅ 获取到 {len(df)} 条历史数据")
        print(f"最近3天数据:")
        print(df.tail(3)[['日期', '收盘', '涨跌幅']])
    else:
        print("❌ 历史数据为空")
        
except Exception as e:
    print(f"❌ akshare历史数据异常: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
