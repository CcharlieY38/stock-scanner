"""
诊断：为什么多股推荐的数据为0
"""
import os
# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

import baostock as bs
import pandas as pd

print("=" * 80)
print(" 诊断：多股推荐数据缺失问题")
print("=" * 80)

# 登录BaoStock
lg = bs.login()
print(f"\n✅ BaoStock登录: {lg.error_msg}")

# 测试几只股票
test_codes = ["sh.600000", "sz.000001", "sz.000002", "sh.600036"]

print(f"\n【测试1】K线数据获取")
for code in test_codes:
    print(f"\n测试: {code}")
    try:
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume",
            start_date='2024-10-01',
            end_date='2025-10-31',
            frequency="d",
            adjustflag="2"
        )
        
        if rs.error_code == '0':
            data = []
            while (rs.error_code == '0') & rs.next():
                data.append(rs.get_row_data())
            
            if data:
                df = pd.DataFrame(data, columns=rs.fields)
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                
                close = df['close'].dropna()
                if len(close) >= 2:
                    last = float(close.iloc[-1])
                    prev = float(close.iloc[-2])
                    chg_pct = ((last - prev) / prev) * 100.0
                    
                    print(f"  ✅ 数据条数: {len(df)}")
                    print(f"  ✅ 最新价: {last:.2f}")
                    print(f"  ✅ 涨跌幅: {chg_pct:+.2f}%")
                else:
                    print(f"  ⚠️  数据不足: {len(close)}条")
            else:
                print(f"  ❌ 无数据")
        else:
            print(f"  ❌ 查询失败: {rs.error_msg}")
            
    except Exception as e:
        print(f"  ❌ 异常: {e}")

print(f"\n\n【测试2】代码格式转换")
# 测试股票代码的处理
from stock_analyzer import EnhancedStockAnalyzer

analyzer = EnhancedStockAnalyzer()

test_input_codes = ["600000", "000001", "300001"]
for code in test_input_codes:
    print(f"\n输入代码: {code}")
    try:
        # 测试get_stock_data
        df = analyzer.get_stock_data(code)
        if df is not None and not df.empty:
            if 'close' in df.columns:
                close = pd.to_numeric(df['close'], errors='coerce').dropna()
                if len(close) >= 2:
                    last = float(close.iloc[-1])
                    prev = float(close.iloc[-2])
                    chg = ((last - prev) / prev) * 100.0
                    print(f"  ✅ 价格: {last:.2f}, 涨跌: {chg:+.2f}%")
                else:
                    print(f"  ⚠️  数据不足")
            else:
                print(f"  ❌ 无close列")
                print(f"     可用列: {list(df.columns)}")
        else:
            print(f"  ❌ 数据为空")
    except Exception as e:
        print(f"  ❌ 异常: {e}")
        import traceback
        traceback.print_exc()

print(f"\n\n【测试3】基本面数据获取")
test_code = "600000"
print(f"\n测试: {test_code}")
try:
    fundamental = analyzer.get_comprehensive_fundamental_data(test_code)
    valuation = fundamental.get('valuation', {})
    
    print(f"  基本面数据结构: {list(fundamental.keys())}")
    print(f"  估值数据: {valuation}")
    
    if valuation:
        pe = valuation.get('市盈率')
        mkt = valuation.get('总市值')
        print(f"  PE: {pe}")
        print(f"  市值: {mkt}")
    else:
        print(f"  ❌ 估值数据为空")
        
except Exception as e:
    print(f"  ❌ 异常: {e}")
    import traceback
    traceback.print_exc()

bs.logout()

print("\n" + "=" * 80)
print("诊断完成")
print("=" * 80)

print("\n💡 常见问题排查:")
print("1. 如果K线数据正常但价格为0 → 检查数据类型转换")
print("2. 如果K线数据失败 → 检查网络连接和BaoStock状态")
print("3. 如果基本面数据为空 → 检查缓存和网络API")
print("4. 如果代码格式错误 → 检查sh/sz前缀处理")
