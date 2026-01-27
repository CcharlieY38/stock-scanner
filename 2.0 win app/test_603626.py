"""
测试603626股票的真实数据获取
"""
import sys
from stock_analyzer import EnhancedStockAnalyzer

def test_stock_603626():
    """测试603626的数据获取"""
    print("=" * 60)
    print("开始测试股票 603626 的数据获取")
    print("=" * 60)
    
    # 初始化分析器
    analyzer = EnhancedStockAnalyzer()
    
    # 正确的股票代码格式（BaoStock格式：sh.开头且9位）
    code = "sh.603626"  # 上交所股票
    # 尝试多种格式
    test_codes = ["sh.603626", "603626", "sh603626"]
    
    successful_code = None
    for test_code in test_codes:
        print(f"\n尝试代码格式: {test_code}")
        try:
            df = analyzer.get_stock_data(test_code)
            if df is not None and not df.empty:
                successful_code = test_code
                print(f"✅ 代码格式 {test_code} 可用")
                break
            else:
                print(f"❌ 代码格式 {test_code} 返回空数据")
        except Exception as e:
            print(f"❌ 代码格式 {test_code} 异常: {str(e)[:100]}")
    
    if not successful_code:
        print("\n❌ 所有代码格式都失败，测试终止")
        return
    
    code = successful_code
    print(f"\n📊 使用股票代码: {code}")
    
    # 1. 测试K线数据
    print("\n【1】测试K线数据获取...")
    try:
        df = analyzer.get_stock_data(code)
        if df is not None and not df.empty:
            import pandas as pd
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            if len(close) >= 2:
                last = float(close.iloc[-1])
                prev = float(close.iloc[-2])
                daily_chg_pct = ((last - prev) / prev) * 100.0
                print(f"✅ 最新价格: {last:.2f} 元")
                print(f"✅ 前一日价格: {prev:.2f} 元")
                print(f"✅ 涨跌幅: {daily_chg_pct:+.2f}%")
                print(f"✅ K线数据条数: {len(close)}")
            else:
                print("❌ K线数据不足")
        else:
            print("❌ K线数据获取失败")
    except Exception as e:
        print(f"❌ K线数据异常: {e}")
    
    # 2. 测试基本面数据（PE和市值）
    print("\n【2】测试基本面数据获取...")
    try:
        fundamental = analyzer.get_comprehensive_fundamental_data(code)
        valuation = fundamental.get('valuation', {})
        
        if valuation:
            print(f"✅ 估值数据获取成功")
            print(f"   估值字段: {list(valuation.keys())}")
            
            # PE
            pe_val = valuation.get('市盈率')
            if pe_val:
                try:
                    pe = float(pe_val)
                    if 0 < pe <= 1200:
                        print(f"   ✅ PE(市盈率): {pe:.2f}")
                    else:
                        print(f"   ⚠️ PE异常值: {pe} (已过滤)")
                except:
                    print(f"   ⚠️ PE转换失败: {pe_val}")
            else:
                print(f"   ❌ PE字段不存在")
            
            # 市值
            mkt_val = valuation.get('总市值')
            if mkt_val:
                try:
                    mktcap = float(mkt_val)
                    # 判断单位并转换
                    if mktcap > 1e9:
                        mktcap_e = mktcap / 1e8
                        print(f"   ✅ 总市值: {mktcap_e:.2f} 亿元 (原始: {mktcap:.0f} 元)")
                    else:
                        print(f"   ✅ 总市值: {mktcap:.2f} 亿元")
                except:
                    print(f"   ⚠️ 市值转换失败: {mkt_val}")
            else:
                print(f"   ❌ 市值字段不存在")
            
            # 显示所有估值数据
            print(f"\n   完整估值数据:")
            for k, v in valuation.items():
                print(f"   - {k}: {v}")
        else:
            print("❌ 估值数据为空")
            print(f"   基本面数据结构: {list(fundamental.keys())}")
    except Exception as e:
        print(f"❌ 基本面数据异常: {e}")
        import traceback
        traceback.print_exc()
    
    # 3. 测试完整离线推荐逻辑（模拟单只股票）
    print("\n【3】测试完整离线推荐逻辑...")
    try:
        import pandas as pd
        import numpy as np
        
        df = analyzer.get_stock_data(code)
        if df is not None and not df.empty:
            close = pd.to_numeric(df['close'], errors='coerce').dropna()
            close = close.tail(25)
            
            # 价格和涨跌幅
            last = float(close.iloc[-1])
            prev = float(close.iloc[-2]) if len(close) >= 2 else last
            daily_chg_pct = ((last - prev) / prev) * 100.0 if prev > 1e-8 else 0.0
            
            # PE和市值
            pe = 0.0
            mktcap_e = 0.0
            fundamental = analyzer.get_comprehensive_fundamental_data(code)
            valuation = fundamental.get('valuation', {})
            if valuation:
                pe_val = valuation.get('市盈率')
                if pe_val:
                    try:
                        pe = float(pe_val)
                        if pe <= 0 or pe > 1200:
                            pe = 0.0
                    except:
                        pass
                
                mkt_val = valuation.get('总市值')
                if mkt_val:
                    try:
                        mktcap_e = float(mkt_val)
                        if mktcap_e > 1e9:
                            mktcap_e = mktcap_e / 1e8
                    except:
                        pass
            
            # 动量和波动
            base = float(close.iloc[-21]) if len(close) >= 21 else last
            momentum = (last - base) / base if base > 1e-8 else 0.0
            rets = np.diff(np.log(close.values))
            vol = float(np.std(rets[-20:])) if len(rets) >= 20 else float(np.std(rets))
            
            # 评分
            mom_score = max(0.0, min(1.0, (momentum + 0.20) / 0.40))
            vol_score = 1.0 - max(0.0, min(1.0, vol / 0.06))
            
            if pe > 0:
                pe_eff = max(0.0, min(120.0, float(pe)))
                pe_score = max(0.0, min(1.0, (80.0 - pe_eff) / 80.0))
                score = float(round(100.0 * (0.5 * mom_score + 0.25 * vol_score + 0.25 * pe_score), 2))
                fundamental_score = pe_score * 100.0
            else:
                score = float(round(100.0 * (0.75 * mom_score + 0.25 * vol_score), 2))
                fundamental_score = 50.0
            
            print(f"✅ 推荐数据生成成功:")
            print(f"   - 最新价格: {last:.2f} 元")
            print(f"   - 涨跌幅: {daily_chg_pct:+.2f}%")
            print(f"   - PE: {pe:.2f}")
            print(f"   - 市值: {mktcap_e:.2f} 亿")
            print(f"   - 动量得分: {mom_score * 100:.2f}")
            print(f"   - 波动得分: {vol_score * 100:.2f}")
            print(f"   - 基本面得分: {fundamental_score:.2f}")
            print(f"   - 综合得分: {score:.2f}")
        else:
            print("❌ K线数据获取失败")
    except Exception as e:
        print(f"❌ 推荐逻辑异常: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == "__main__":
    test_stock_603626()
