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

if __name__ == "__main__":
    print("🚀 开始测试股票数据接口...")
    print(f"📅 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        compare_data_sources()
    except Exception as e:
        print(f"❌ 测试过程出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ 测试完成！")
