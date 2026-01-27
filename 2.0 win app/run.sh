#!/bin/bash
# 股票分析系统启动脚本

cd "$(dirname "$0")"

echo "=================================="
echo "  股票分析系统 v3.0 (预测型)"
echo "=================================="
echo ""

# 检查Python版本
PYTHON_CMD="python3"

if ! command -v $PYTHON_CMD &> /dev/null; then
    echo "❌ 错误: 未找到 python3 命令"
    echo "请安装 Python 3.8 或更高版本"
    exit 1
fi

echo "✅ Python: $($PYTHON_CMD --version)"
echo ""

# 检查依赖
echo "🔍 检查依赖..."
$PYTHON_CMD -c "
import sys
try:
    import PyQt6
    import pandas
    import numpy
    import akshare
    import markdown2
    print('✅ 所有依赖已就绪')
except ImportError as e:
    print(f'❌ 缺少依赖: {e}')
    print('')
    print('请运行以下命令安装:')
    print('  pip3 install PyQt6 pandas numpy akshare markdown2 baostock jieba')
    sys.exit(1)
" || exit 1

echo ""
echo "🚀 正在启动GUI..."
echo ""

# 启动GUI
$PYTHON_CMD gui2.py

echo ""
echo "程序已退出"
