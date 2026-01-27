"""
测试快速推荐功能
"""
import os
# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

from stock_analyzer import EnhancedStockAnalyzer

print("=" * 80)
print(" 测试快速推荐功能")
print("=" * 80)

# 初始化分析器
print("\n📊 正在初始化分析器...")
analyzer = EnhancedStockAnalyzer()

print("\n🚀 开始生成快速推荐（Top 10）...\n")

try:
    recommendations = analyzer.get_quick_recommendations(top_n=10, exclude_st=True)
    
    if recommendations and len(recommendations) > 0:
        print(f"✅ 成功生成 {len(recommendations)} 条推荐\n")
        print("=" * 100)
        print(f"{'代码':<10} {'名称':<12} {'价格':>10} {'涨跌%':>8} {'PE':>8} {'市值(亿)':>12} {'得分':>8} {'建议':<15}")
        print("=" * 100)
        
        for i, rec in enumerate(recommendations, 1):
            code = rec.get('stock_code', '')
            name = rec.get('stock_name', '')
            price = rec.get('latest_price', 0.0)
            chg = rec.get('change_pct', 0.0)
            pe = rec.get('pe', 0.0)
            mkt = rec.get('mktcap_e', 0.0)
            score = rec.get('score', 0.0)
            advice = rec.get('recommendation', '')
            
            print(f"{code:<10} {name:<12} {price:>10.2f} {chg:>+7.2f}% {pe:>8.2f} {mkt:>12.2f} {score:>8.2f} {advice:<15}")
        
        print("=" * 100)
        
        # 检查数据完整性
        print(f"\n🔍 数据完整性检查:")
        missing_price = sum(1 for r in recommendations if r.get('latest_price', 0) == 0)
        missing_pe = sum(1 for r in recommendations if r.get('pe', 0) == 0)
        missing_mkt = sum(1 for r in recommendations if r.get('mktcap_e', 0) == 0)
        
        print(f"   缺失价格: {missing_price}/{len(recommendations)}")
        print(f"   缺失PE: {missing_pe}/{len(recommendations)}")
        print(f"   缺失市值: {missing_mkt}/{len(recommendations)}")
        
        if missing_price > 0 or missing_pe > 0 or missing_mkt > 0:
            print(f"\n⚠️  部分数据缺失，可能原因:")
            print(f"   1. 网络连接问题")
            print(f"   2. 数据源API限流")
            print(f"   3. 股票代码格式问题")
        else:
            print(f"\n✅ 所有数据完整！")
            
    else:
        print("❌ 未生成任何推荐")
        print("\n可能的原因:")
        print("1. BaoStock连接失败")
        print("2. 候选股票池为空")
        print("3. 数据筛选过于严格")
        
except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("测试完成")
print("=" * 80)
