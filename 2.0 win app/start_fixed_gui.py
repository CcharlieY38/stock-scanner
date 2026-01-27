"""
修复GUI网络问题 - 临时禁用有问题的数据源
"""
import sys
import os

# 清除可能的代理设置
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        print(f"清除环境变量: {key}")
        del os.environ[key]

print("=" * 70)
print(" 修复网络问题并启动GUI")
print("=" * 70)

# 设置更宽松的超时
os.environ['STOCK_ANALYZER_TIMEOUT'] = '3'  # 减少超时时间
os.environ['STOCK_ANALYZER_SKIP_FUND_FLOW'] = '1'  # 跳过资金流

print("\n✅ 已应用修复:")
print("   1. 清除HTTP代理设置")
print("   2. 减少请求超时时间")
print("   3. 跳过资金流数据（避免卡顿）")
print("\n🚀 正在启动GUI...")
print("=" * 70)

# 启动GUI
from gui2 import main
main()
