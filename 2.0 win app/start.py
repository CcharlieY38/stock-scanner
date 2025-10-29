#!/usr/bin/env python3
"""
股票分析系统启动脚本
包含完整的错误处理和日志记录
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        # 检查Python版本
        if sys.version_info < (3, 8):
            print("❌ 需要 Python 3.8 或更高版本")
            print(f"   当前版本: {sys.version}")
            sys.exit(1)
        
        print("=" * 60)
        print("    现代股票分析系统 v3.0")
        print("=" * 60)
        print()
        
        # 导入配置模块
        try:
            from gui_config import check_dependencies
            deps_status = check_dependencies()
            
            if not deps_status['all_satisfied']:
                print("❌ 缺少必需依赖:")
                for dep in deps_status['missing_required']:
                    print(f"   - {dep}")
                print()
                print("请运行以下命令安装:")
                print(f"   pip install {' '.join(deps_status['missing_required'])}")
                sys.exit(1)
            
            if deps_status['missing_optional']:
                print("ℹ️  以下可选依赖未安装:")
                for dep in deps_status['missing_optional']:
                    print(f"   - {dep}")
                print()
        
        except ImportError as e:
            print(f"⚠️  配置模块导入失败: {e}")
            print("   将使用默认配置继续...")
            print()
        
        # 启动GUI
        print("🚀 正在启动图形界面...")
        print()
        
        from gui2 import main as gui_main
        gui_main()
    
    except KeyboardInterrupt:
        print("\n")
        print("⚠️  用户中断")
        sys.exit(0)
    
    except Exception as e:
        print("\n")
        print("=" * 60)
        print("❌ 启动失败")
        print("=" * 60)
        print(f"错误: {e}")
        print()
        
        import traceback
        print("详细错误信息:")
        print(traceback.format_exc())
        print()
        print("请检查:")
        print("  1. 是否安装了所有必需依赖")
        print("  2. Python 版本是否 >= 3.8")
        print("  3. 是否有足够的磁盘空间")
        print("  4. 网络连接是否正常")
        print()
        sys.exit(1)

if __name__ == '__main__':
    main()
