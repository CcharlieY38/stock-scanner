"""
诊断推荐列表数据获取差异
对比昨晚成功和今天失败的原因
"""
import os
import sys
import json
from datetime import datetime

# 清除代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

print("=" * 70)
print(" 推荐列表数据获取差异诊断")
print("=" * 70)

# 测试相同的股票代码
test_codes = ['002539', '000686', '600909']  # 昨晚成功的前3只

print(f"\n📋 测试股票代码: {', '.join(test_codes)}")
print(f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# 导入分析器
try:
    from stock_analyzer import EnhancedStockAnalyzer
    print("\n✅ 成功导入分析器")
    
    analyzer = EnhancedStockAnalyzer()
    print("✅ 分析器初始化成功")
    
except Exception as e:
    print(f"\n❌ 分析器加载失败: {e}")
    sys.exit(1)

print("\n" + "=" * 70)
print("开始逐项测试数据获取")
print("=" * 70)

results = {}

for code in test_codes:
    print(f"\n{'='*70}")
    print(f"测试股票: {code}")
    print(f"{'='*70}")
    
    result = {
        'code': code,
        'price_ok': False,
        'pe_ok': False,
        'mktcap_ok': False,
        'price_data': {},
        'valuation_data': {},
        'errors': []
    }
    
    # 1. 测试K线数据（价格和涨跌幅）
    print(f"\n【1】测试K线数据获取")
    try:
        df = analyzer.get_stock_data(code)
        if df is not None and not df.empty and 'close' in df.columns:
            import pandas as pd
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            if len(close) >= 2:
                last = float(close.iloc[-1])
                prev = float(close.iloc[-2])
                chg_pct = ((last - prev) / prev) * 100.0
                
                result['price_ok'] = True
                result['price_data'] = {
                    'latest_price': last,
                    'change_pct': chg_pct,
                    'data_points': len(close)
                }
                
                print(f"✅ K线数据获取成功")
                print(f"   最新价: {last:.2f}")
                print(f"   涨跌幅: {chg_pct:+.2f}%")
                print(f"   数据点: {len(close)}条")
            else:
                result['errors'].append('K线数据点不足')
                print(f"⚠️ K线数据点不足: {len(close)}条")
        else:
            result['errors'].append('K线数据为空')
            print(f"❌ K线数据为空")
    except Exception as e:
        result['errors'].append(f'K线获取失败: {str(e)}')
        print(f"❌ K线获取失败: {e}")
    
    # 2. 测试估值数据（PE和市值）
    print(f"\n【2】测试估值数据获取")
    try:
        fundamental = analyzer.get_comprehensive_fundamental_data(code)
        valuation = fundamental.get('valuation', {})
        
        if valuation:
            pe = valuation.get('市盈率')
            mktcap = valuation.get('总市值')
            
            if pe:
                try:
                    pe_val = float(pe)
                    if 0 < pe_val <= 1200:
                        result['pe_ok'] = True
                        result['valuation_data']['pe'] = pe_val
                        print(f"✅ PE获取成功: {pe_val:.2f}")
                    else:
                        result['errors'].append(f'PE值异常: {pe_val}')
                        print(f"⚠️ PE值异常: {pe_val}")
                except Exception as e:
                    result['errors'].append(f'PE转换失败: {str(e)}')
                    print(f"❌ PE转换失败: {e}")
            else:
                result['errors'].append('PE数据缺失')
                print(f"⚠️ PE数据缺失")
            
            if mktcap:
                try:
                    mktcap_val = float(mktcap)
                    # 单位转换
                    if mktcap_val > 1e9:
                        mktcap_e = mktcap_val / 1e8
                    else:
                        mktcap_e = mktcap_val
                    
                    result['mktcap_ok'] = True
                    result['valuation_data']['mktcap_e'] = mktcap_e
                    print(f"✅ 市值获取成功: {mktcap_e:.2f}亿")
                except Exception as e:
                    result['errors'].append(f'市值转换失败: {str(e)}')
                    print(f"❌ 市值转换失败: {e}")
            else:
                result['errors'].append('市值数据缺失')
                print(f"⚠️ 市值数据缺失")
        else:
            result['errors'].append('估值数据为空')
            print(f"❌ 估值数据为空")
            
            # 尝试查看原始fundamental数据结构
            print(f"\n🔍 fundamental数据结构:")
            for key in fundamental.keys():
                print(f"   - {key}: {type(fundamental[key])}")
                
    except Exception as e:
        result['errors'].append(f'估值获取失败: {str(e)}')
        print(f"❌ 估值获取失败: {e}")
    
    # 3. 汇总结果
    print(f"\n【3】数据获取汇总")
    print(f"   价格数据: {'✅ 正常' if result['price_ok'] else '❌ 失败'}")
    print(f"   PE数据: {'✅ 正常' if result['pe_ok'] else '❌ 失败'}")
    print(f"   市值数据: {'✅ 正常' if result['mktcap_ok'] else '❌ 失败'}")
    
    if result['errors']:
        print(f"\n⚠️ 错误列表:")
        for err in result['errors']:
            print(f"   - {err}")
    
    results[code] = result

# 生成诊断报告
print("\n" + "=" * 70)
print("诊断报告汇总")
print("=" * 70)

successful = sum(1 for r in results.values() if r['price_ok'] and r['pe_ok'] and r['mktcap_ok'])
print(f"\n✅ 完全成功: {successful}/{len(test_codes)}")
print(f"⚠️ 部分失败: {len(test_codes) - successful}/{len(test_codes)}")

# 对比分析
print(f"\n📊 昨晚 vs 今天对比:")
print(f"   昨晚(2025-10-29 23:08): 所有数据正常 ✅")
print(f"   今天(2025-10-31): ", end="")

if successful == len(test_codes):
    print("所有数据正常 ✅")
    print("\n💡 结论: 今天也能正常获取数据！")
    print("   可能是之前的测试时机问题，或者网络临时波动")
else:
    print(f"{successful}只正常，{len(test_codes)-successful}只失败 ⚠️")
    print("\n💡 结论: 数据获取不稳定")
    print("   可能原因:")
    print("   1. 网络波动")
    print("   2. API限流")
    print("   3. 数据源接口变更")
    print("   4. 时间段差异（交易时间 vs 非交易时间）")

# 保存诊断结果
try:
    report = {
        'test_time': datetime.now().isoformat(),
        'test_codes': test_codes,
        'results': {k: {
            'code': v['code'],
            'price_ok': v['price_ok'],
            'pe_ok': v['pe_ok'],
            'mktcap_ok': v['mktcap_ok'],
            'price_data': v['price_data'],
            'valuation_data': v['valuation_data'],
            'errors': v['errors']
        } for k, v in results.items()},
        'summary': {
            'total': len(test_codes),
            'successful': successful,
            'failed': len(test_codes) - successful
        }
    }
    
    filename = f"diagnosis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n📄 详细报告已保存: {filename}")
except Exception as e:
    print(f"\n⚠️ 报告保存失败: {e}")

print("\n" + "=" * 70)
