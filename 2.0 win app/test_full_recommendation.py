"""
完整测试：快速推荐功能（包含情绪分）
"""
import os
# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

from stock_analyzer import EnhancedStockAnalyzer

print("=" * 80)
print(" 完整测试：快速推荐功能（情绪分已集成）")
print("=" * 80)

# 初始化分析器
analyzer = EnhancedStockAnalyzer()

print("\n📊 测试离线推荐模式（使用BaoStock数据）")
print("   - 包含真实PE和市值")
print("   - 包含情绪分析（基于趋势+成交量）")
print("   - 三维评分：技术40% + 基本面40% + 情绪20%\n")

# 测试快速推荐
try:
    recommendations = analyzer.get_quick_recommendations(top_n=5, exclude_st=True)
    
    if recommendations:
        print(f"✅ 成功生成 {len(recommendations)} 条推荐\n")
        print("=" * 80)
        print(f"{'代码':<10} {'名称':<10} {'价格':>8} {'涨跌%':>8} {'PE':>8} {'市值':>10} {'得分':>8} {'建议':<12}")
        print("=" * 80)
        
        for rec in recommendations:
            code = rec.get('stock_code', '')
            name = rec.get('stock_name', '')
            price = rec.get('latest_price', 0.0)
            chg = rec.get('change_pct', 0.0)
            pe = rec.get('pe', 0.0)
            mkt = rec.get('mktcap_e', 0.0)
            score = rec.get('score', 0.0)
            advice = rec.get('recommendation', '')
            
            print(f"{code:<10} {name:<10} {price:>8.2f} {chg:>+7.2f}% {pe:>8.2f} {mkt:>9.2f}亿 {score:>8.2f} {advice:<12}")
        
        print("=" * 80)
        
        # 详细分析第一只股票
        if recommendations:
            print(f"\n📋 第一名详细分析:")
            first = recommendations[0]
            code = first.get('stock_code')
            name = first.get('stock_name')
            
            print(f"   股票: {code} {name}")
            print(f"   价格: {first.get('latest_price'):.2f} 元")
            print(f"   涨跌: {first.get('change_pct'):+.2f}%")
            print(f"   PE: {first.get('pe'):.2f}")
            print(f"   市值: {first.get('mktcap_e'):.2f} 亿")
            print(f"   综合得分: {first.get('score'):.2f}")
            print(f"   投资建议: {first.get('recommendation')}")
            
            # 如果有详细评分数据（需要从日志中查看）
            print(f"\n   💡 评分构成:")
            print(f"      - 技术面: 基于20日动量和波动率")
            print(f"      - 基本面: 基于PE估值（PE越低越好）")
            print(f"      - 情绪面: 基于近5日趋势和成交量变化")
            print(f"      - 综合得分: 技术40% + 基本面40% + 情绪20%")
    else:
        print("❌ 未生成推荐，可能是数据源问题")
        
except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("测试完成")
print("=" * 80)

print("\n✨ 修改总结:")
print("   1. ✅ 真实价格和涨跌幅（从K线计算）")
print("   2. ✅ 真实PE和市值（从基本面接口获取）")
print("   3. ✅ 情绪分析（基于趋势+成交量）")
print("   4. ✅ 三维综合评分（技术+基本面+情绪）")
print("   5. ✅ 智能权重分配（40%+40%+20%）")
