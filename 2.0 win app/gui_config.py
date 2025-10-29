"""
GUI配置文件
用于管理界面相关的配置和环境变量
"""

import os
import sys
import logging

def setup_environment():
    """设置运行环境变量"""
    # macOS 稳定性增强
    if sys.platform == 'darwin':  # macOS
        os.environ.setdefault('QT_MAC_WANTS_LAYER', '1')
        os.environ.setdefault('QT_ENABLE_HIGHDPI_SCALING', '1')
        os.environ.setdefault('QT_AUTO_SCREEN_SCALE_FACTOR', '1')
        
        # 抑制不必要的Qt警告
        os.environ.setdefault('QT_LOGGING_RULES', '*.debug=false;qt.qpa.*=false')
        
        # 抑制 macOS IMK 相关警告
        os.environ.setdefault('QT_MAC_DISABLE_FOREGROUND_APPLICATION_TRANSFORM', '1')
    
    elif sys.platform == 'win32':  # Windows
        os.environ.setdefault('QT_ENABLE_HIGHDPI_SCALING', '1')
        os.environ.setdefault('QT_AUTO_SCREEN_SCALE_FACTOR', '1')
    
    elif sys.platform.startswith('linux'):  # Linux
        os.environ.setdefault('QT_QPA_PLATFORM', 'xcb')
        os.environ.setdefault('QT_AUTO_SCREEN_SCALE_FACTOR', '1')

def setup_logging():
    """设置日志配置"""
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('stock_analyzer_gui.log', encoding='utf-8')
        ]
    )
    
    # 降低某些库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def get_app_config():
    """获取应用配置"""
    return {
        'app_name': '现代股票分析系统',
        'app_version': '3.0.0',
        'organization': 'Smart Stock Analyzer Team',
        'window_size': {
            'default': (1200, 800),
            'minimum': (800, 600)
        },
        'theme': {
            'style': 'Fusion',
            'primary_color': '#667eea',
            'secondary_color': '#764ba2',
            'success_color': '#56ab2f',
            'warning_color': '#f39c12',
            'error_color': '#e74c3c',
            'info_color': '#3498db'
        },
        'network': {
            'timeout': 10,  # 秒
            'retry_count': 3,
            'retry_delay': 1  # 秒
        },
        'cache': {
            'enable': True,
            'dir': '.cache',
            'ttl': 3600  # 秒
        }
    }

def check_dependencies():
    """检查必要的依赖"""
    missing_deps = []
    
    # 必需依赖
    required_deps = {
        'PyQt6': 'PyQt6',
        'pandas': 'pandas',
        'numpy': 'numpy',
        'akshare': 'akshare',
        'baostock': 'baostock',
        'markdown2': 'markdown2'
    }
    
    # 可选依赖
    optional_deps = {
        'jieba': 'jieba (用于中文分词)',
        'openai': 'openai (用于AI分析)',
        'anthropic': 'anthropic (用于Claude AI)',
        'zhipuai': 'zhipuai (用于智谱AI)'
    }
    
    for package, name in required_deps.items():
        try:
            __import__(package)
        except ImportError:
            missing_deps.append(name)
    
    optional_missing = []
    for package, name in optional_deps.items():
        try:
            __import__(package)
        except ImportError:
            optional_missing.append(name)
    
    return {
        'missing_required': missing_deps,
        'missing_optional': optional_missing,
        'all_satisfied': len(missing_deps) == 0
    }

if __name__ == '__main__':
    # 测试配置
    setup_environment()
    logger = setup_logging()
    
    logger.info("环境配置完成")
    
    deps = check_dependencies()
    if deps['missing_required']:
        logger.error(f"缺少必需依赖: {', '.join(deps['missing_required'])}")
    else:
        logger.info("所有必需依赖已满足")
    
    if deps['missing_optional']:
        logger.info(f"缺少可选依赖: {', '.join(deps['missing_optional'])}")
    
    config = get_app_config()
    logger.info(f"应用配置: {config['app_name']} v{config['app_version']}")
