"""
直接测试快速推荐功能的输出
"""
import os
# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

from stock_analyzer import EnhancedStockAnalyzer
import json

print("=" * 80)
print(" 测试快速推荐功能 - 输出数据检查")
print("=" * 80)

analyzer = EnhancedStockAnalyzer()

print("\n🚀 生成快速推荐（Top 5）...\n")

recommendations = analyzer.get_quick_recommendations(top_n=5, exclude_st=True)

if recommendations:
    print(f"✅ 生成了 {len(recommendations)} 条推荐\n")
    
    print("=" * 100)
    print(f"{'序号':<6} {'代码':<10} {'名称':<12} {'价格':>10} {'涨跌%':>8} {'PE':>8} {'市值(亿)':>12} {'得分':>8}")
    print("=" * 100)
    
    for i, rec in enumerate(recommendations, 1):
        code = rec.get('stock_code', 'N/A')
        name = rec.get('stock_name', 'N/A')
        price = rec.get('latest_price', 0.0)
        chg = rec.get('change_pct', 0.0)
        pe = rec.get('pe', 0.0)
        mkt = rec.get('mktcap_e', 0.0)
        score = rec.get('score', 0.0)
        
        print(f"{i:<6} {code:<10} {name:<12} {price:>10.2f} {chg:>+7.2f}% {pe:>8.2f} {mkt:>12.2f} {score:>8.2f}")
    
    print("=" * 100)
    
    # 详细分析第一条
    print(f"\n📋 第一条推荐详情（JSON格式）:")
    print(json.dumps(recommendations[0], indent=2, ensure_ascii=False))
    
    # 统计数据完整性
    print(f"\n📊 数据完整性统计:")
    total = len(recommendations)
    
    has_price = sum(1 for r in recommendations if r.get('latest_price', 0) != 0)
    has_chg = sum(1 for r in recommendations if r.get('change_pct', 0) != 0)
    has_pe = sum(1 for r in recommendations if r.get('pe', 0) != 0)
    has_mkt = sum(1 for r in recommendations if r.get('mktcap_e', 0) != 0)
    has_score = sum(1 for r in recommendations if r.get('score', 0) != 0)
    
    print(f"  有价格数据: {has_price}/{total} ({has_price/total*100:.1f}%)")
    print(f"  有涨跌幅数据: {has_chg}/{total} ({has_chg/total*100:.1f}%)")
    print(f"  有PE数据: {has_pe}/{total} ({has_pe/total*100:.1f}%)")
    print(f"  有市值数据: {has_mkt}/{total} ({has_mkt/total*100:.1f}%)")
    print(f"  有得分数据: {has_score}/{total} ({has_score/total*100:.1f}%)")
    
    if has_price == 0:
        print(f"\n❌ 严重问题：所有股票价格都为0！")
        print(f"   可能原因：")
        print(f"   1. get_stock_data返回的数据格式有问题")
        print(f"   2. close列名映射错误")
        print(f"   3. 数据类型转换失败")
    elif has_price < total:
        print(f"\n⚠️  部分股票价格缺失")
    else:
        print(f"\n✅ 价格数据完整")
        
else:
    print("❌ 未生成任何推荐")

print("\n" + "=" * 80)
