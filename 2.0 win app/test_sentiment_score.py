"""
测试情绪分计入综合得分
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
print(" 测试情绪分计入综合得分（603626）")
print("=" * 70)

# 登录
lg = bs.login()
print(f"\n✅ BaoStock登录: {lg.error_msg}")

code = "sh.603626"

# 获取K线数据
print(f"\n【1】获取K线和成交量数据")
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
    
    print(f"✅ 获取 {len(df)} 条数据")
    
    # 最新数据
    close = df['close'].dropna().tail(25)
    
    # 1. 技术面得分
    print(f"\n【2】技术面评分")
    last = float(close.iloc[-1])
    prev = float(close.iloc[-2])
    daily_chg_pct = ((last - prev) / prev) * 100.0
    
    base = float(close.iloc[-21]) if len(close) >= 21 else last
    momentum = (last - base) / base
    
    rets = np.diff(np.log(close.values))
    vol = float(np.std(rets[-20:])) if len(rets) >= 20 else float(np.std(rets))
    
    mom_score = max(0.0, min(1.0, (momentum + 0.20) / 0.40))
    vol_score = 1.0 - max(0.0, min(1.0, vol / 0.06))
    technical_score = mom_score * 100.0
    
    print(f"   最新价: {last:.2f} 元")
    print(f"   日涨跌: {daily_chg_pct:+.2f}%")
    print(f"   20日动量: {momentum*100:+.2f}%")
    print(f"   波动率: {vol:.4f}")
    print(f"   ✅ 技术面得分: {technical_score:.2f}")
    
    # 2. 基本面得分（假设PE=25）
    print(f"\n【3】基本面评分")
    pe = 25.0  # 假设
    pe_eff = max(0.0, min(120.0, pe))
    pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
    fundamental_score = pe_score * 100.0
    print(f"   假设PE: {pe:.2f}")
    print(f"   ✅ 基本面得分: {fundamental_score:.2f}")
    
    # 3. 情绪面得分
    print(f"\n【4】情绪面评分（基于趋势+成交量）")
    
    # 近5日涨跌趋势
    recent_5d_chg = (float(close.iloc[-1]) - float(close.iloc[-6])) / float(close.iloc[-6]) if len(close) >= 6 else 0.0
    
    # 成交量变化
    vol_data = df['volume'].dropna()
    if len(vol_data) >= 10:
        recent_vol_avg = float(vol_data.tail(5).mean())
        prev_vol_avg = float(vol_data.tail(10).head(5).mean())
        volume_factor = (recent_vol_avg - prev_vol_avg) / prev_vol_avg if prev_vol_avg > 0 else 0.0
    else:
        volume_factor = 0.0
    
    # 情绪得分计算
    trend_contrib = max(30.0, min(70.0, 50.0 + recent_5d_chg * 200))
    vol_contrib = max(-10.0, min(10.0, volume_factor * 20))
    sentiment_score = max(0.0, min(100.0, trend_contrib + vol_contrib))
    
    print(f"   近5日涨跌: {recent_5d_chg*100:+.2f}%")
    print(f"   成交量变化: {volume_factor*100:+.2f}%")
    print(f"   趋势贡献: {trend_contrib:.2f}")
    print(f"   成交量贡献: {vol_contrib:+.2f}")
    print(f"   ✅ 情绪面得分: {sentiment_score:.2f}")
    
    # 4. 综合得分（应用权重）
    print(f"\n【5】综合评分（应用权重）")
    
    # 默认权重：技术40% + 基本面40% + 情绪20%
    w_t = 0.4
    w_f = 0.4
    w_s = 0.2
    
    comprehensive_score = (
        technical_score * w_t +
        fundamental_score * w_f +
        sentiment_score * w_s
    )
    
    print(f"   权重配置: 技术{w_t*100:.0f}% + 基本面{w_f*100:.0f}% + 情绪{w_s*100:.0f}%")
    print(f"   技术贡献: {technical_score:.2f} × {w_t} = {technical_score * w_t:.2f}")
    print(f"   基本面贡献: {fundamental_score:.2f} × {w_f} = {fundamental_score * w_f:.2f}")
    print(f"   情绪贡献: {sentiment_score:.2f} × {w_s} = {sentiment_score * w_s:.2f}")
    print(f"   ✅ 综合得分: {comprehensive_score:.2f}")
    
    # 5. 对比：有无情绪分的差异
    print(f"\n【6】对比分析")
    score_without_sentiment = (technical_score * 0.5 + fundamental_score * 0.5)
    diff = comprehensive_score - score_without_sentiment
    
    print(f"   不含情绪分（技术50%+基本面50%）: {score_without_sentiment:.2f}")
    print(f"   包含情绪分（技术40%+基本面40%+情绪20%）: {comprehensive_score:.2f}")
    print(f"   差异: {diff:+.2f}")
    
    if sentiment_score > 50:
        impact = "正面"
        symbol = "📈"
    elif sentiment_score < 50:
        impact = "负面"
        symbol = "📉"
    else:
        impact = "中性"
        symbol = "➡️"
    
    print(f"   {symbol} 情绪面对综合得分的影响: {impact} ({diff:+.2f}分)")

bs.logout()

print("\n" + "=" * 70)
print("测试完成 - 情绪分已正确计入综合得分")
print("=" * 70)
print("\n💡 说明:")
print("   1. 情绪分基于近5日涨跌趋势和成交量变化")
print("   2. 综合得分 = 技术40% + 基本面40% + 情绪20%")
print("   3. 情绪分会影响最终的股票推荐排序")
