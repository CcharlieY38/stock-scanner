"""
提示文件：如何完成test_002490.py

提示1 - 登录BaoStock：
-------------------
lg = bs.login()
if lg.error_code == '0':
    print("✅ 登录成功")
else:
    print(f"❌ 登录失败: {lg.error_msg}")

提示2 - 获取K线数据：
-------------------
rs = bs.query_history_k_data_plus(
    code,  # 股票代码
    "date,code,close,volume",  # 需要的字段
    start_date='2024-10-01',  # 开始日期
    end_date='2025-10-29',    # 结束日期
    frequency="d",            # 日线
    adjustflag="2"           # 前复权
)

# 检查是否成功
if rs.error_code == '0':
    # 读取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if data_list:
        df = pd.DataFrame(data_list, columns=rs.fields)
        print(f"✅ 获取到 {len(df)} 条数据")
    else:
        print("❌ 数据为空")
else:
    print(f"❌ 查询失败: {rs.error_msg}")

提示3 - 数据处理：
-------------------
# 转换数据类型
df['close'] = pd.to_numeric(df['close'], errors='coerce')

# 获取最新价
latest_price = df['close'].iloc[-1]

# 计算涨跌幅
prev_price = df['close'].iloc[-2]
change_pct = (latest_price - prev_price) / prev_price * 100

# 5日均价
ma5 = df['close'].tail(5).mean()

提示4 - 判断趋势：
-------------------
if latest_price > ma5:
    trend = "上涨趋势"
else:
    trend = "下跌趋势"
"""
