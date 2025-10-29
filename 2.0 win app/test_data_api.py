#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试数据接口 - 查看BaoStock和akshare数据结构
"""

import baostock as bs
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

def test_baostock_data():
    """测试BaoStock数据接口"""
    print("=" * 60)
    print("🔍 测试 BaoStock 数据接口")
    print("=" * 60)
    
    # 登录BaoStock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ BaoStock登录失败: {lg.error_msg}")
        return None
    
    print("✅ BaoStock登录成功")
    
    # 测试获取股票基本信息
    print("\n📋 获取股票列表（前10个）:")
    try:
        rs = bs.query_all_stock(day='2025-09-20')
        stock_list = []
        while (rs.error_code == '0') & rs.next():
            stock_list.append(rs.get_row_data())
        
        if stock_list:
            df_stocks = pd.DataFrame(stock_list, columns=rs.fields)
            print(f"📊 股票总数: {len(df_stocks)}")
            print(f"📝 列名: {list(df_stocks.columns)}")
            print("前5个股票:")
            print(df_stocks.head())
        else:
            print("❌ 未获取到股票列表")
            
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
    
    # 测试获取价格数据
    print("\n📈 获取平安银行(000001.SZ)价格数据:")
    try:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            "sz.000001",
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list:
            df_price = pd.DataFrame(data_list, columns=rs.fields)
            print(f"📊 数据行数: {len(df_price)}")
            print(f"📝 列名: {list(df_price.columns)}")
            print("数据类型:")
            print(df_price.dtypes)
            print("\n前5行数据:")
            print(df_price.head())
            print("\n后5行数据:")
            print(df_price.tail())
            
            # 检查数据质量
            print("\n🔍 数据质量检查:")
            print(f"空值统计:\n{df_price.isnull().sum()}")
            
            # 尝试转换数值类型
            print("\n🔢 数值转换测试:")
            try:
                df_price['close'] = pd.to_numeric(df_price['close'], errors='coerce')
                df_price['volume'] = pd.to_numeric(df_price['volume'], errors='coerce')
                print(f"收盘价范围: {df_price['close'].min()} - {df_price['close'].max()}")
                print(f"成交量范围: {df_price['volume'].min()} - {df_price['volume'].max()}")
            except Exception as e:
                print(f"❌ 数值转换失败: {e}")
                
        else:
            print("❌ 未获取到价格数据")
            
    except Exception as e:
        print(f"❌ 获取价格数据失败: {e}")
    
    # 登出BaoStock
    bs.logout()
    print("\n✅ BaoStock登出成功")
    
    return df_price if 'df_price' in locals() else None

def test_akshare_data():
    """测试akshare数据接口"""
    print("\n" + "=" * 60)
    print("🔍 测试 akshare 数据接口")
    print("=" * 60)
    
    # 测试获取股票基本信息
    print("\n📋 获取股票基本信息:")
    try:
        stock_info = ak.stock_info_a_code_name()
        print(f"📊 股票总数: {len(stock_info)}")
        print(f"📝 列名: {list(stock_info.columns)}")
        print("前5个股票:")
        print(stock_info.head())
    except Exception as e:
        print(f"❌ 获取股票基本信息失败: {e}")
    
    # 测试获取价格数据
    print("\n📈 获取平安银行(000001)价格数据:")
    try:
        # 获取历史价格数据
        df_price = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20250820", end_date="20250920", adjust="")
        
        if df_price is not None and len(df_price) > 0:
            print(f"📊 数据行数: {len(df_price)}")
            print(f"📝 列名: {list(df_price.columns)}")
            print("数据类型:")
            print(df_price.dtypes)
            print("\n前5行数据:")
            print(df_price.head())
            print("\n后5行数据:")
            print(df_price.tail())
            
            # 检查数据质量
            print("\n🔍 数据质量检查:")
            print(f"空值统计:\n{df_price.isnull().sum()}")
            
            # 检查数值范围
            if '收盘' in df_price.columns:
                print(f"收盘价范围: {df_price['收盘'].min()} - {df_price['收盘'].max()}")
            if '成交量' in df_price.columns:
                print(f"成交量范围: {df_price['成交量'].min()} - {df_price['成交量'].max()}")
                
        else:
            print("❌ 未获取到价格数据")
            
    except Exception as e:
        print(f"❌ 获取价格数据失败: {e}")
        df_price = None
    
    return df_price

def compare_data_sources():
    """对比两个数据源的差异"""
    print("\n" + "=" * 60)
    print("🔄 对比数据源差异")
    print("=" * 60)
    
    # 获取BaoStock数据
    bs_data = test_baostock_data()
    
    # 获取akshare数据
    ak_data = test_akshare_data()
    
    # 对比分析
    print("\n📊 数据源对比总结:")
    print("-" * 40)
    
    if bs_data is not None:
        print(f"🔸 BaoStock - 数据行数: {len(bs_data)}, 列数: {len(bs_data.columns)}")
        print(f"   主要列名: {list(bs_data.columns)[:8]}...")
    else:
        print("🔸 BaoStock - 数据获取失败")
    
    if ak_data is not None:
        print(f"🔸 akshare - 数据行数: {len(ak_data)}, 列数: {len(ak_data.columns)}")
        print(f"   主要列名: {list(ak_data.columns)[:8]}...")
    else:
        print("🔸 akshare - 数据获取失败")

def test_announcement_apis():
    """测试公告获取API的可用性"""
    print("\n" + "=" * 60)
    print("📢 测试公告获取API")
    print("=" * 60)
    
    test_codes = ['002050']  # 使用002050作为测试股票
    
    for test_code in test_codes:
        print(f"\n📊 测试股票: {test_code}")
        print("-" * 40)
        
        # 1. 测试原代码使用的API
        print("1️⃣ 测试 ak.stock_zh_a_alerts_cls")
        try:
            result = ak.stock_zh_a_alerts_cls(symbol=test_code)
            if result is not None and not result.empty:
                print(f"  ✅ 成功获取 {len(result)} 条公告")
                print(f"  📋 列名: {list(result.columns)}")
                print("  📰 前3条公告:")
                for i, row in result.head(3).iterrows():
                    print(f"    - {row.iloc[0] if len(row) > 0 else 'N/A'}")
            else:
                print("  ❌ 返回空数据")
        except AttributeError as e:
            print(f"  ❌ API不存在: {e}")
        except Exception as e:
            print(f"  ❌ 失败: {e}")
        
        # 2. 测试替代API方案（仅使用当前可用且稳定的接口）
        alternative_apis = [
            ('stock_zh_a_alerts', lambda: ak.stock_zh_a_alerts()),  # 全市场公告
            ('stock_notice_report', lambda: ak.stock_notice_report()),  # 全市场公告（按报告）
        ]
        
        for api_name, api_func in alternative_apis:
            print(f"\n2️⃣ 测试 ak.{api_name}")
            try:
                if api_name == 'stock_zh_a_alerts':
                    # 全市场公告，需要筛选
                    all_data = api_func()
                    if all_data is not None and not all_data.empty:
                        # 尝试筛选目标股票
                        filtered_data = all_data[
                            all_data.iloc[:, 0].astype(str).str.contains(test_code, na=False)
                        ] if len(all_data.columns) > 0 else pd.DataFrame()
                        
                        print(f"  ✅ 全市场公告总数: {len(all_data)}")
                        if not filtered_data.empty:
                            print(f"  ✅ 筛选出 {test_code} 相关: {len(filtered_data)} 条")
                            print(f"  📋 列名: {list(all_data.columns)}")
                        else:
                            print(f"  ⚠️ 未找到 {test_code} 相关公告")
                    else:
                        print("  ❌ 返回空数据")
                else:
                    # 全市场公告，需筛选指定股票
                    result = api_func()
                    if result is not None and not result.empty:
                        # 识别代码列并筛选
                        code_cols = [c for c in ['代码','股票代码','证券代码','证券代码(6位)'] if c in result.columns]
                        if not code_cols:
                            code_cols = [c for c in result.columns if '码' in str(c)] or [result.columns[0]]
                        code_col = code_cols[0]
                        filtered = result[result[code_col].astype(str) == str(test_code)]
                        print(f"  ✅ 全市场公告总数: {len(result)}，{test_code} 相关: {len(filtered)} 条")
                        print(f"  📋 列名: {list(result.columns)}")
                        if not filtered.empty:
                            print("  📰 前3条标题:")
                            title_col = '公告标题' if '公告标题' in filtered.columns else filtered.columns[0]
                            for _, row in filtered.head(3).iterrows():
                                print(f"    - {str(row.get(title_col, 'N/A'))[:50]}...")
                    else:
                        print("  ❌ 返回空数据")
                        
            except AttributeError as e:
                print(f"  ❌ API不存在: {e}")
            except Exception as e:
                print(f"  ❌ 失败: {e}")
        
        # 只测试第一个股票，避免过多输出
        break
    
    print("\n" + "=" * 60)
    print("🔍 公告API测试总结:")
    print("1. 检查哪些API可用")
    print("2. 确认数据格式和结构") 
    print("3. 验证是否需要特殊参数")
    print("4. 评估替代方案的可行性")
    print("=" * 60)

def test_dividend_apis():
    """测试分红数据获取API的可用性"""
    print("\n" + "=" * 60)
    print("💰 测试分红数据获取API")
    print("=" * 60)
    
    test_codes = ['002050']  # 使用002050作为测试股票
    
    for test_code in test_codes:
        print(f"\n📊 测试股票: {test_code}")
        print("-" * 40)
        
        # 1. 测试BaoStock分红数据
        print("1️⃣ 测试 BaoStock 分红数据")
        try:
            import baostock as bs
            
            # 登录BaoStock
            lg = bs.login()
            if lg.error_code == '0':
                print("  ✅ BaoStock登录成功")
                
                # 获取最近几年的分红数据
                years = ['2024', '2023', '2022', '2021', '2020']
                all_dividend_data = []
                
                for year in years:
                    rs = bs.query_dividend_data(code=f'sz.{test_code}', year=year, yearType='report')
                    if rs.error_code == '0':
                        year_data = rs.get_data()
                        if not year_data.empty:
                            all_dividend_data.append((year, year_data))
                
                print(f"  ✅ BaoStock 获取到 {len(all_dividend_data)} 年的分红数据")
                
                for year, data in all_dividend_data:
                    print(f"    📅 {year}年分红:")
                    for _, row in data.iterrows():
                        operate_date = row.get('dividOperateDate', '')
                        cash_before = row.get('dividCashPsBeforeTax', '0')
                        cash_after = row.get('dividCashPsAfterTax', '0') 
                        stock_bonus = row.get('dividStocksPs', '0')
                        
                        print(f"      除权日: {operate_date}")
                        if cash_before and cash_before != '0':
                            print(f"      现金分红(税前): {cash_before}元/股")
                        if cash_after and cash_after != '0':
                            print(f"      现金分红(税后): {cash_after}元/股")
                        if stock_bonus and stock_bonus != '0':
                            print(f"      送股: {stock_bonus}股/股")
                        print()
                
                bs.logout()
            else:
                print(f"  ❌ BaoStock登录失败: {lg.error_msg}")
                
        except Exception as e:
            print(f"  ❌ BaoStock分红数据获取失败: {e}")
        
        # 2. 测试akshare分红数据API
        print(f"\n2️⃣ 测试 akshare 分红数据")
        akshare_dividend_apis = [
            'stock_dividend_cninfo',
            'stock_fhpg_detail_em', 
            'stock_fhpg_em',
            'stock_history_dividend_detail'
        ]
        
        for api_name in akshare_dividend_apis:
            print(f"  测试 ak.{api_name}")
            try:
                if hasattr(ak, api_name):
                    api_func = getattr(ak, api_name)
                    result = api_func(symbol=test_code)
                    
                    if result is not None and not result.empty:
                        print(f"    ✅ 成功获取 {len(result)} 条分红数据")
                        print(f"    📋 列名: {list(result.columns)}")
                        
                        # 显示前几条分红信息
                        if len(result) > 0:
                            print("    💰 最近分红记录:")
                            for i, (_, row) in enumerate(result.head(3).iterrows()):
                                # 尝试找到分红相关的列
                                dividend_cols = [col for col in result.columns if any(keyword in col 
                                               for keyword in ['分红', 'dividend', '股息', '派息', '现金', '比例'])]
                                date_cols = [col for col in result.columns if any(keyword in col 
                                           for keyword in ['日期', 'date', '时间'])]
                                
                                date_info = str(row[date_cols[0]]) if date_cols else '未知日期'
                                dividend_info = str(row[dividend_cols[0]]) if dividend_cols else '分红信息未知'
                                
                                print(f"      {i+1}. {date_info}: {dividend_info}")
                    else:
                        print("    ❌ 返回空数据")
                else:
                    print(f"    ❌ API不存在")
                    
            except Exception as e:
                print(f"    ❌ 失败: {e}")
        
        # 只测试第一个股票
        break
    
    print("\n" + "=" * 60)
    print("🔍 分红数据API测试总结:")
    print("1. BaoStock: 提供详细的历史分红数据")
    print("2. akshare: 多个分红API，数据来源不同")
    print("3. 建议: 优先使用BaoStock，akshare作为补充")
    print("=" * 60)

def test_performance_apis(symbol: str = '002050'):
    """测试业绩预告/业绩快报接口，按代码筛选"""
    print("\n" + "=" * 60)
    print("📈 测试业绩预告/快报 API")
    print("=" * 60)

    import akshare as ak
    import pandas as pd

    # 1) 业绩预告（全市场，需筛代码）
    try:
        yjyg = ak.stock_yjyg_em()
        if yjyg is not None and not yjyg.empty:
            code_cols = [c for c in ['代码','股票代码','证券代码'] if c in yjyg.columns]
            code_col = code_cols[0] if code_cols else yjyg.columns[0]
            df = yjyg[yjyg[code_col].astype(str) == str(symbol)]
            print(f"✅ 业绩预告总数: {len(yjyg)}，{symbol} 相关: {len(df)} 条")
            if not df.empty:
                show_cols = [c for c in ['公告日期','预告类型','预告净利润变动幅度','变动原因'] if c in df.columns]
                print(df[show_cols].head(5)) if show_cols else print(df.head(5))
        else:
            print("⚠️ 业绩预告返回空")
    except Exception as e:
        print(f"❌ 业绩预告失败: {e}")

    # 2) 业绩快报（全市场，需筛代码）
    try:
        yjkb = ak.stock_yjkb_em()
        if yjkb is not None and not yjkb.empty:
            code_cols = [c for c in ['代码','股票代码','证券代码'] if c in yjkb.columns]
            code_col = code_cols[0] if code_cols else yjkb.columns[0]
            df = yjkb[yjkb[code_col].astype(str) == str(symbol)]
            print(f"✅ 业绩快报总数: {len(yjkb)}，{symbol} 相关: {len(df)} 条")
            if not df.empty:
                show_cols = [c for c in ['公告日期','营业收入','归母净利润','每股收益','营业收入同比增长','归母净利润同比增长'] if c in df.columns]
                print(df[show_cols].head(5)) if show_cols else print(df.head(5))
        else:
            print("⚠️ 业绩快报返回空")
    except Exception as e:
        print(f"❌ 业绩快报失败: {e}")

def test_industry_analysis(symbol: str = '002050'):
    """测试行业分析：调用 Analyzer 的稳健实现（含缓存/兜底），避免网络波动导致失败"""
    print("\n" + "=" * 60)
    print("🏭 测试行业分析 API")
    print("=" * 60)
    try:
        # 保证同目录可导入
        import os, sys
        sys.path.append(os.path.dirname(__file__))
        from stock_analyzer import EnhancedStockAnalyzer
        an = EnhancedStockAnalyzer()
        ind = an._get_industry_analysis(symbol)
        name = ind.get('industry_name') or '未知'
        name_primary = ind.get('industry_name_primary') or name
        bs_name = ind.get('baostock_industry_name') or '-'
        source = ind.get('industry_source') or '-'
        tags = ind.get('industry_tags') or []
        print(f"行业名称(显示): {name}")
        print(f"主口径(东财): {name_primary}")
        print(f"BaoStock口径: {bs_name}")
        print(f"标签: {', '.join(tags) if tags else '-'}")
        print(f"成份来源: {source}")
        rank = ind.get('industry_rank') or {}
        peers = ind.get('peers_sample') or []
        print(f"是否成份内: {ind.get('in_constituents', False)}")
        print(f"同行样本数: {len(peers)}")
        if rank:
            print(f"排名信息: {rank}")
        if peers:
            # 打印前5条
            for i, p in enumerate(peers[:5]):
                print(f"  - 同行{i+1}: {p}")
    except Exception as e:
        print(f"❌ 行业分析失败(Analyzer): {e}")

def prefer_baostock_price_then_ak(symbol: str = '002050'):
    """优先Bao的价格，Bao无数据时再用akshare"""
    from datetime import datetime, timedelta
    import pandas as pd
    import baostock as bs
    import akshare as ak

    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    code_bao = f"sz.{symbol}" if symbol.startswith(('0','3')) else f"sh.{symbol}"

    # 1) BaoStock
    lg = bs.login()
    if lg.error_code == '0':
        rs = bs.query_history_k_data_plus(
            code_bao,
            "date,code,open,high,low,close,volume,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        bs.logout()
        df_bao = pd.DataFrame(data_list, columns=rs.fields)
        if not df_bao.empty:
            print(f"价格: 使用BaoStock，共{len(df_bao)}行")
            return 'baostock', df_bao

    # 2) akshare 兜底
    try:
        df_ak = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date.replace('-',''), end_date=end_date.replace('-',''), adjust="")
        if df_ak is not None and not df_ak.empty:
            print(f"价格: 使用akshare，共{len(df_ak)}行")
            return 'akshare', df_ak
    except Exception as e:
        print(f"akshare获取价格失败: {e}")
    print("价格: 两个源均失败")
    return None, None


def prefer_baostock_dividend_then_ak(symbol: str = '002050'):
    """优先Bao的分红，Bao无数据时再用akshare"""
    import baostock as bs
    import pandas as pd
    import akshare as ak
    from datetime import datetime

    years = [datetime.now().year, datetime.now().year-1, datetime.now().year-2]
    code_bao = f"sz.{symbol}" if symbol.startswith(('0','3')) else f"sh.{symbol}"

    lg = bs.login()
    ok = (lg.error_code == '0')
    all_rows = []
    if ok:
        for year in years:
            rs = bs.query_dividend_data(code=code_bao, year=year, yearType='report')
            if rs.error_code == '0':
                df = rs.get_data()
                if df is not None and not df.empty:
                    all_rows.append(df)
        bs.logout()
    if all_rows:
        df_bao = pd.concat(all_rows, ignore_index=True)
        print(f"分红: 使用BaoStock，共{len(df_bao)}条")
        return 'baostock', df_bao

    # 兜底 akshare
    ak_funcs = [
        lambda: ak.stock_dividend_cninfo(symbol=symbol),
        lambda: ak.stock_history_dividend_detail(symbol=symbol),
    ]
    for f in ak_funcs:
        try:
            df = f()
            if df is not None and not df.empty:
                print(f"分红: 使用akshare({f.__name__}), 共{len(df)}条")
                return 'akshare', df
        except Exception as e:
            print(f"akshare分红失败: {e}")
    print("分红: 两个源均失败")
    return None, None


def test_capital_flow(symbol: str = '002050'):
    """测试 Analyzer 的近1个月资金流聚合与分级输出，打印关键字段，容错网络波动"""
    print("\n" + "=" * 60)
    print("💵 测试近1个月资金流")
    print("=" * 60)
    try:
        import os, sys, json
        sys.path.append(os.path.dirname(__file__))
        from stock_analyzer import EnhancedStockAnalyzer

        an = EnhancedStockAnalyzer()
        price_df = an.get_stock_data(symbol)
        if price_df is None or price_df.empty:
            print("⚠️ 价格数据为空，跳过资金流测试")
            return None

        # 基本面用于估算市值（可空）
        try:
            funda = an.get_comprehensive_fundamental_data(symbol)
        except Exception:
            funda = {}

        cf = an.calculate_capital_flow(symbol, price_df, funda, window_days=30)
        # 打印摘要
        buckets = cf.get('buckets') or {}
        print(json.dumps({
            'code': symbol,
            'status': cf.get('status'),
            'main_force_net': cf.get('main_force_net'),
            'main_force_ratio': cf.get('main_force_ratio_to_turnover'),
            'sum_amount': cf.get('sum_amount'),
            'buckets_keys': list(buckets.keys()),
            'note': cf.get('note'),
            'source': cf.get('source')
        }, ensure_ascii=False, indent=2))

        # 软断言：结构健全
        required_bucket_keys = {'extra_large', 'large', 'medium', 'small'}
        if buckets and required_bucket_keys.issubset(set(buckets.keys())):
            print("✅ 资金流结构完整（含四档）")
        else:
            print("⚠️ 资金流结构不完整或空，可能为网络/权限问题")

        return cf
    except Exception as e:
        print(f"❌ 资金流测试失败: {e}")
        return None


def test_analyzer_end_to_end(symbol: str = '002050'):
    """端到端运行 analyze_stock，打印分数与资金流状态，便于联调 GUI"""
    print("\n" + "=" * 60)
    print("🧪 端到端 Analyzer 测试")
    print("=" * 60)
    try:
        import os, sys, json
        sys.path.append(os.path.dirname(__file__))
        from stock_analyzer import EnhancedStockAnalyzer
        an = EnhancedStockAnalyzer()
        rep = an.analyze_stock(symbol)
        out = {
            'code': rep.get('stock_code'),
            'name': rep.get('stock_name'),
            'scores': rep.get('scores'),
            'capital_flow_status': (rep.get('capital_flow') or {}).get('status'),
            'capital_flow_main': (rep.get('capital_flow') or {}).get('main_force_net'),
            'capital_flow_note': (rep.get('capital_flow') or {}).get('note'),
            'capital_flow_source': (rep.get('capital_flow') or {}).get('source'),
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return rep
    except Exception as e:
        print(f"❌ 端到端测试失败: {e}")
        return None


if __name__ == "__main__":
    print("🚀 开始测试股票数据接口...")
    print(f"📅 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 测试数据源对比
        compare_data_sources()

        # 测试公告API
        test_announcement_apis()

        # 测试分红数据API
        test_dividend_apis()

        # 测试业绩预告/快报API
        test_performance_apis('002050')

        # 测试行业分析API
        test_industry_analysis('002050')

        # 测试资金流（近1个月）
        test_capital_flow('002050')

        # 端到端（含资金流、评分修正、GUI可读取字段）
        test_analyzer_end_to_end('002050')

    except Exception as e:
        print(f"❌ 测试过程出现错误: {e}")
        import traceback
        traceback.print_exc()

    print("\n✅ 测试完成！")
    print("\n====== Bao优先兜底测试 ======")
    src_p, dfp = prefer_baostock_price_then_ak('002050')
    src_d, dfd = prefer_baostock_dividend_then_ak('002050')
    print(f"价格源: {src_p}, 分红源: {src_d}")
