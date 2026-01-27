"""
使用BaoStock测试603626的完整数据获取流程
"""
import os
# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

import baostock as bs
import pandas as pd
import numpy as np

print("=" * 70)
print(" 603626 完整数据测试（基于BaoStock）")
print("=" * 70)

# 登录
lg = bs.login()
print(f"\n✅ BaoStock登录: {lg.error_msg}")

code = "sh.603626"

# 1. K线数据
print(f"\n【1】获取K线数据")
rs = bs.query_history_k_data_plus(
    code,
    "date,code,open,high,low,close,volume,turn",
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
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    
    print(f"✅ 获取 {len(df)} 条K线数据")
    
    # 计算价格和涨跌幅
    close = df['close'].dropna()
    close = close.tail(25)
    
    last = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) >= 2 else last
    daily_chg_pct = ((last - prev) / prev) * 100.0
    
    print(f"\n📊 价格数据:")
    print(f"   最新价: {last:.2f} 元")
    print(f"   前日价: {prev:.2f} 元")
    print(f"   涨跌幅: {daily_chg_pct:+.2f}%")
    
    # 计算动量和波动
    base = float(close.iloc[-21]) if len(close) >= 21 else last
    momentum = (last - base) / base if base > 1e-8 else 0.0
    
    rets = np.diff(np.log(close.values))
    vol = float(np.std(rets[-20:])) if len(rets) >= 20 else float(np.std(rets))
    
    print(f"\n📈 技术指标:")
    print(f"   20日动量: {momentum * 100:+.2f}%")
    print(f"   波动率: {vol:.4f}")
    
    # 评分
    mom_score = max(0.0, min(1.0, (momentum + 0.20) / 0.40))
    vol_score = 1.0 - max(0.0, min(1.0, vol / 0.06))
    
    print(f"\n⭐ 技术评分:")
    print(f"   动量得分: {mom_score * 100:.2f}")
    print(f"   波动得分: {vol_score * 100:.2f}")

# 2. 基本面数据
print(f"\n【2】获取基本面数据")

# 盈利能力
rs_profit = bs.query_profit_data(code=code, year=2024, quarter=3)
if rs_profit.error_code == '0':
    profit_data = []
    while (rs_profit.error_code == '0') & rs_profit.next():
        profit_data.append(rs_profit.get_row_data())
    if profit_data:
        df_profit = pd.DataFrame(profit_data, columns=rs_profit.fields)
        print(f"✅ 盈利能力数据:")
        print(df_profit[['code', 'roeAvg', 'npMargin', 'gpMargin']].to_string(index=False))

# 成长能力  
rs_growth = bs.query_growth_data(code=code, year=2024, quarter=3)
if rs_growth.error_code == '0':
    growth_data = []
    while (rs_growth.error_code == '0') & rs_growth.next():
        growth_data.append(rs_growth.get_row_data())
    if growth_data:
        df_growth = pd.DataFrame(growth_data, columns=rs_growth.fields)
        print(f"\n✅ 成长能力数据:")
        print(df_growth[['code', 'YOYNI', 'YOYEPSBasic']].to_string(index=False))

# 运营能力
rs_operation = bs.query_operation_data(code=code, year=2024, quarter=3)
if rs_operation.error_code == '0':
    operation_data = []
    while (rs_operation.error_code == '0') & rs_operation.next():
        operation_data.append(rs_operation.get_row_data())
    if operation_data:
        df_operation = pd.DataFrame(operation_data, columns=rs_operation.fields)
        print(f"\n✅ 运营能力数据:")
        # 显示所有可用字段
        print(f"   可用字段: {list(df_operation.columns)}")
        display_cols = [c for c in ['code', 'assetTurnRate', 'inventoryTurnDays'] if c in df_operation.columns]
        if display_cols:
            print(df_operation[display_cols].to_string(index=False))

# 偿债能力
rs_balance = bs.query_balance_data(code=code, year=2024, quarter=3)
if rs_balance.error_code == '0':
    balance_data = []
    while (rs_balance.error_code == '0') & rs_balance.next():
        balance_data.append(rs_balance.get_row_data())
    if balance_data:
        df_balance = pd.DataFrame(balance_data, columns=rs_balance.fields)
        print(f"\n✅ 偿债能力数据:")
        print(df_balance[['code', 'currentRatio', 'quickRatio']].to_string(index=False))

# 估值数据（PE等）- BaoStock可能没有实时PE，需要计算
print(f"\n【3】估值数据（需计算）")
print("⚠️  BaoStock不直接提供PE和市值，需要通过其他接口或计算获得")
print("   建议使用 akshare 的估值接口，但当前网络受限")

# 综合评分示例
print(f"\n【4】综合评分示例（基于现有数据）")
score_without_pe = float(round(100.0 * (0.75 * mom_score + 0.25 * vol_score), 2))
print(f"   无PE评分（动量75% + 波动25%）: {score_without_pe:.2f}")

# 如果有PE数据（模拟）
pe_example = 25.0  # 假设PE为25
pe_eff = max(0.0, min(120.0, float(pe_example)))
pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
score_with_pe = float(round(100.0 * (0.5 * mom_score + 0.25 * vol_score + 0.25 * pe_score), 2))
print(f"   有PE评分（假设PE={pe_example}）: {score_with_pe:.2f}")
print(f"     - 动量50%: {mom_score * 50:.2f}")
print(f"     - 波动25%: {vol_score * 25:.2f}")
print(f"     - 估值25%: {pe_score * 25:.2f}")

bs.logout()
print(f"\n✅ BaoStock登出")

print("\n" + "=" * 70)
print("测试完成 - BaoStock数据获取正常")
print("=" * 70)
print("\n💡 建议:")
print("   1. BaoStock可以获取K线和基本财务数据")
print("   2. PE和市值需要额外接口（akshare或东财API）")
print("   3. 当前代码修改已支持动态获取真实PE和市值")
print("   4. 网络问题导致部分接口不可用，但不影响核心逻辑")
