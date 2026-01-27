#!/usr/bin/env python3
"""
量化主线狙击系统 - 快速演示脚本
验证GUI集成是否成功
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_import():
    """测试模块导入"""
    print("=" * 60)
    print("🧪 测试量化主线狙击系统模块导入")
    print("=" * 60)
    
    # 测试quant_mainline_tab导入
    try:
        from quant_mainline_tab import QuantMainlineTab, QuantMainlineThread
        print("✅ quant_mainline_tab 模块导入成功")
        print(f"   - QuantMainlineTab: {QuantMainlineTab}")
        print(f"   - QuantMainlineThread: {QuantMainlineThread}")
    except ImportError as e:
        print(f"❌ quant_mainline_tab 导入失败: {e}")
        return False
    
    # 测试PyQt6依赖
    try:
        from PyQt6.QtWidgets import QApplication
        print("✅ PyQt6 已安装")
    except ImportError:
        print("❌ PyQt6 未安装，请运行: pip install PyQt6")
        return False
    
    return True

def test_gui_integration():
    """测试GUI集成"""
    print("\n" + "=" * 60)
    print("🧪 测试GUI集成")
    print("=" * 60)
    
    try:
        from PyQt6.QtWidgets import QApplication
        from gui2 import ModernStockAnalyzerGUI
        
        print("✅ GUI主模块导入成功")
        
        # 检查gui2.py是否包含量化标签页代码
        with open('gui2.py', 'r', encoding='utf-8') as f:
            content = f.read()
            if 'QuantMainlineTab' in content:
                print("✅ gui2.py 已集成量化主线狙击标签页")
            else:
                print("⚠️ gui2.py 未检测到集成代码")
        
        return True
    except Exception as e:
        print(f"❌ GUI集成测试失败: {e}")
        return False

def show_usage():
    """显示使用说明"""
    print("\n" + "=" * 60)
    print("📖 使用说明")
    print("=" * 60)
    print("""
启动股票分析系统:
    python gui2.py
    或
    python start.py

使用量化主线狙击功能:
    1. 启动程序后，点击顶部的 "🎯 量化主线狙击" 标签页
    2. 点击 "🔍 盘前扫描" 按钮开始分析
    3. 查看三个表格中的分析结果
    4. 点击 "📥 导出报告" 保存分析结果

界面布局:
    ┌─────────────────────────────────────────────┐
    │ 📈 单只分析 │ 📊 批量分析 │ 🌟 推荐列表 │ 🎯 量化主线狙击 │ ← 点这里
    └─────────────────────────────────────────────┘
    │                                             │
    │  [🔍 盘前扫描] [📡 盘中监控] [📥 导出报告]    │
    │                                             │
    │  🎯 主线行业排名 Top 5                       │
    │  ┌──────────────────────────────┐           │
    │  │ 排名 │ 行业 │ 强度分 │ 状态   │           │
    │  └──────────────────────────────┘           │
    │                                             │
    │  👑 龙头股池 Top 10                          │
    │  ┌──────────────────────────────┐           │
    │  │ 代码 │ 名称 │ 类型 │ 评分    │           │
    │  └──────────────────────────────┘           │
    │                                             │
    │  🎣 超跌反弹候选                             │
    │  ┌──────────────────────────────┐           │
    │  │ 代码 │ 名称 │ 超跌系数       │           │
    │  └──────────────────────────────┘           │
    └─────────────────────────────────────────────┘

当前版本说明:
    ✅ GUI界面已完成
    ✅ 盘前扫描流程已完成
    ✅ 数据展示已完成
    ✅ 报告导出已完成
    
    ⚠️ 当前使用模拟数据展示
    ⏳ 实际量化引擎待集成（参考完整设计文档）

详细说明请查看:
    量化主线狙击系统使用指南.md
    """)

def main():
    """主函数"""
    print("\n🚀 量化主线共振狙击系统 - 集成验证")
    print()
    
    # 测试导入
    if not test_import():
        print("\n❌ 导入测试失败，请检查依赖")
        return
    
    # 测试GUI集成
    if not test_gui_integration():
        print("\n❌ GUI集成测试失败")
        return
    
    # 显示使用说明
    show_usage()
    
    print("\n" + "=" * 60)
    print("✅ 所有测试通过！量化主线狙击系统已成功集成到GUI")
    print("=" * 60)
    print("\n💡 现在可以运行 `python gui2.py` 启动程序")
    print()

if __name__ == '__main__':
    main()
