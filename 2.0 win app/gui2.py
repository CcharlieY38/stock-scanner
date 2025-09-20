import sys
import time
import logging
from io import StringIO
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLineEdit, QPushButton, QTextBrowser,
                           QLabel, QTextEdit, QMessageBox, QProgressBar, 
                           QFrame, QSizePolicy, QTabWidget, QGroupBox, 
                           QGridLayout, QCheckBox, QSlider, QSpinBox,
                           QSplitter, QScrollArea)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QFont, QPalette, QColor, QTextCursor, QIcon
import markdown2
import json
from datetime import datetime

# 导入股票分析器并智能识别版本
try:
    # 导入分析器（支持多种可能的类名）
    from stock_analyzer import *
    
    # 智能识别分析器类和版本
    ANALYZER_CLASS = None
    ANALYZER_VERSION = "Unknown"
    
    # 尝试不同的类名
    possible_classes = [
        ('EnhancedStockAnalyzer', 'Enhanced v3.0'),
        ('ComprehensiveStockAnalyzer', 'Standard v2.0'),
        ('StockAnalyzer', 'Basic v1.0')
    ]
    
    for class_name, version in possible_classes:
        try:
            ANALYZER_CLASS = globals()[class_name]
            ANALYZER_VERSION = version
            print(f"✅ 成功导入分析器: {class_name}")
            break
        except KeyError:
            continue
    
    if ANALYZER_CLASS is None:
        raise ImportError("未找到合适的分析器类")
        
except ImportError:
    print("❌ 无法导入股票分析器！")
    print("📋 请确保 stock_analyzer.py 文件存在于同一目录下")
    print("💡 支持的分析器类名：")
    print("   - EnhancedStockAnalyzer (增强版)")
    print("   - ComprehensiveStockAnalyzer (标准版)")
    print("   - StockAnalyzer (基础版)")
    sys.exit(1)

class LogHandler(logging.Handler):
    """自定义日志处理器，将日志输出到GUI"""
    def __init__(self, log_signal):
        super().__init__()
        self.log_signal = log_signal
        
    def emit(self, record):
        try:
            msg = self.format(record)
            # 根据日志级别确定颜色
            if record.levelno >= logging.ERROR:
                log_type = "error"
            elif record.levelno >= logging.WARNING:
                log_type = "warning"
            elif "✓" in msg:
                log_type = "success"
            elif "正在" in msg or "开始" in msg:
                log_type = "info"
            else:
                log_type = "normal"
            
            self.log_signal.emit(msg, log_type)
        except Exception:
            pass

class ModernFrame(QFrame):
    """现代化的面板组件"""
    def __init__(self, parent=None, elevated=False):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Raised)
        shadow_style = "0 4px 8px rgba(0,0,0,0.1)" if elevated else "0 2px 4px rgba(0,0,0,0.05)"
        self.setStyleSheet(f"""
            ModernFrame {{
                background-color: #ffffff;
                border-radius: 12px;
                border: 1px solid #e3f2fd;
                box-shadow: {shadow_style};
            }}
        """)

class ModernButton(QPushButton):
    """现代化的按钮组件"""
    def __init__(self, text, parent=None, button_type="primary"):
        super().__init__(text, parent)
        self.setMinimumHeight(44)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        if button_type == "primary":
            self.setStyleSheet("""
                QPushButton {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 12px 24px;
                    font-weight: 600;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background: linear-gradient(135deg, #5a6fd8 0%, #6a4190 100%);
                    transform: translateY(-1px);
                }
                QPushButton:pressed {
                    background: linear-gradient(135deg, #4e5bc6 0%, #5e377e 100%);
                    transform: translateY(0px);
                }
                QPushButton:disabled {
                    background: #cccccc;
                    color: black;
                }
            """)
        elif button_type == "secondary":
            self.setStyleSheet("""
                QPushButton {
                    background-color: #f8f9fa;
                    color: black;
                    border: 2px solid #e9ecef;
                    border-radius: 8px;
                    padding: 12px 24px;
                    font-weight: 600;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background-color: #e9ecef;
                    border-color: #adb5bd;
                    color: black;
                }
                QPushButton:pressed {
                    background-color: #dee2e6;
                }
                QPushButton:disabled {
                    color: black;
                    border-color: #dee2e6;
                }
            """)
        elif button_type == "success":
            self.setStyleSheet("""
                QPushButton {
                    background: linear-gradient(135deg, #56ab2f 0%, #a8e6cf 100%);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    padding: 12px 24px;
                    font-weight: 600;
                    font-size: 14px;
                }
                QPushButton:hover {
                    background: linear-gradient(135deg, #4e9a2a 0%, #96d4b5 100%);
                }
            """)

class ModernLineEdit(QLineEdit):
    """现代化的输入框组件"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(44)
        self.setStyleSheet("""
            QLineEdit {
                border: 2px solid #e9ecef;
                border-radius: 8px;
                padding: 12px 16px;
                background-color: white;
                font-size: 14px;
                selection-background-color: #cce0ff;
            }
            QLineEdit:focus {
                border-color: #667eea;
                background-color: #f8f9ff;
            }
            QLineEdit:hover {
                border-color: #adb5bd;
            }
        """)

class ModernTextEdit(QTextEdit):
    """现代化的多行文本输入框组件"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("""
            QTextEdit {
                border: 2px solid #e9ecef;
                border-radius: 8px;
                padding: 12px;
                background-color: white;
                font-size: 14px;
                selection-background-color: #cce0ff;
                line-height: 1.5;
            }
            QTextEdit:focus {
                border-color: #667eea;
                background-color: #f8f9ff;
            }
            QTextEdit:hover {
                border-color: #adb5bd;
            }
        """)

class ModernProgressBar(QProgressBar):
    """现代化的进度条组件"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("""
            QProgressBar {
                border: none;
                border-radius: 6px;
                background-color: #e9ecef;
                height: 12px;
                text-align: center;
                color: black;
                font-weight: 600;
            }
            QProgressBar::chunk {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-radius: 6px;
            }
        """)

class StreamingDisplay(QTextEdit):
    """流式显示组件，支持彩色日志显示"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("""
            QTextEdit {
                border: 2px solid #e9ecef;
                border-radius: 8px;
                padding: 16px;
                background-color: #f8f9fa;
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 13px;
                line-height: 1.4;
            }
        """)
        self.setReadOnly(True)
        
    def append_streaming_text(self, text, text_type="normal"):
        """添加流式文本"""
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        
        # 根据文本类型设置样式
        if text_type == "header":
            cursor.insertHtml(f'<p style="color: #667eea; font-weight: bold; font-size: 14px; margin: 8px 0;">{text}</p>')
        elif text_type == "important":
            cursor.insertHtml(f'<span style="color: #e74c3c; font-weight: bold;">{text}</span>')
        elif text_type == "success":
            cursor.insertHtml(f'<span style="color: #27ae60; font-weight: bold;">{text}</span>')
        elif text_type == "warning":
            cursor.insertHtml(f'<span style="color: #f39c12; font-weight: bold;">⚠️ {text}</span>')
        elif text_type == "error":
            cursor.insertHtml(f'<span style="color: #e74c3c; font-weight: bold;">❌ {text}</span>')
        elif text_type == "info":
            cursor.insertHtml(f'<span style="color: #3498db; font-weight: 500;">📋 {text}</span>')
        else:
            cursor.insertText(text)
        
        # 添加换行
        if not text.endswith('\n'):
            cursor.insertText('\n')
        
        # 自动滚动到底部
        self.ensureCursorVisible()
        
    def clear_log(self):
        """清空日志"""
        self.clear()
        self.append_streaming_text("📋 日志已清空", "info")

class AnalysisWorker(QThread):
    """后台工作线程，用于执行分析任务"""
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)
    log_message = pyqtSignal(str, str)  # 日志消息和类型

    def __init__(self, analyzer, stock_code, enable_streaming=True):
        super().__init__()
        self.analyzer = analyzer
        self.stock_code = stock_code
        self.enable_streaming = enable_streaming

    def run(self):
        try:
            # 设置日志处理器
            log_handler = LogHandler(self.log_message)
            log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            
            # 添加日志处理器到分析器的logger
            self.analyzer.logger.addHandler(log_handler)
            self.analyzer.logger.setLevel(logging.INFO)
            
            # 发送开始分析的信号
            self.log_message.emit(f"🚀 开始全面分析股票: {self.stock_code}", "header")
            self.progress.emit(10)
            
            # 设置流式显示配置（如果分析器支持）
            if hasattr(self.analyzer, 'set_streaming_config'):
                if self.enable_streaming:
                    self.analyzer.set_streaming_config(
                        enabled=True,
                        show_thinking=False  # GUI中不直接打印，而是通过日志显示
                    )
                else:
                    self.analyzer.set_streaming_config(enabled=False)
            
            # 执行实际分析
            try:
                report = self.analyzer.analyze_stock(self.stock_code, enable_streaming=False)
                self.progress.emit(100)
                self.log_message.emit("🎉 股票分析完成！", "success")
                
                # 移除日志处理器
                self.analyzer.logger.removeHandler(log_handler)
                
                self.finished.emit(report)
            except AttributeError as e:
                error_msg = str(e)
                if 'calculate_news_sentiment' in error_msg:
                    self.log_message.emit("⚠️ 检测到方法缺失，正在尝试修复...", "warning")
                    self.error.emit("分析器版本不匹配，缺少情绪分析方法。请更新分析器文件。")
                elif any(method in error_msg for method in ['get_sentiment_analysis', 'get_comprehensive_fundamental_data']):
                    self.log_message.emit("⚠️ 检测到接口版本问题", "warning")
                    self.error.emit("分析器接口不兼容，请确保使用最新版本的分析器。")
                else:
                    self.error.emit(f"分析过程中出现属性错误: {error_msg}")
            except ImportError as e:
                self.log_message.emit(f"❌ 缺少必要依赖: {str(e)}", "error")
                self.error.emit(f"缺少依赖库: {str(e)}。请安装相关依赖后重试。")
            except ValueError as e:
                self.log_message.emit(f"⚠️ 数据获取问题: {str(e)}", "warning")
                self.error.emit(f"数据获取失败: {str(e)}。请检查股票代码是否正确。")
            except Exception as e:
                self.log_message.emit(f"❌ 分析过程中出现错误: {str(e)}", "error")
                self.error.emit(f"分析失败: {str(e)}")
            finally:
                # 确保移除日志处理器
                try:
                    self.analyzer.logger.removeHandler(log_handler)
                except:
                    pass
                    
        except Exception as e:
            self.log_message.emit(f"❌ 工作线程出现严重错误: {str(e)}", "error")
            self.error.emit(f"工作线程错误: {str(e)}")

class BatchAnalysisWorker(QThread):
    """批量分析工作线程"""
    finished = pyqtSignal(list)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)
    current_stock = pyqtSignal(str)
    log_message = pyqtSignal(str, str)  # 日志消息和类型

    def __init__(self, analyzer, stock_list):
        super().__init__()
        self.analyzer = analyzer
        self.stock_list = stock_list

    def run(self):
        try:
            # 设置日志处理器
            log_handler = LogHandler(self.log_message)
            log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.analyzer.logger.addHandler(log_handler)
            self.analyzer.logger.setLevel(logging.INFO)
            
            results = []
            total = len(self.stock_list)
            
            self.log_message.emit(f"📊 开始批量分析 {total} 只股票", "header")
            
            # 设置为非流式模式以提高批量处理效率
            if hasattr(self.analyzer, 'set_streaming_config'):
                self.analyzer.set_streaming_config(enabled=False)
            
            for i, stock_code in enumerate(self.stock_list):
                try:
                    self.current_stock.emit(f"正在分析: {stock_code} ({i+1}/{total})")
                    self.log_message.emit(f"📈 开始分析第 {i+1} 只股票: {stock_code}", "info")
                    
                    report = self.analyzer.analyze_stock(stock_code, enable_streaming=False)
                    results.append(report)
                    
                    self.log_message.emit(f"✓ {stock_code} 分析完成，得分: {report['scores']['comprehensive']:.1f}", "success")
                    self.progress.emit(int((i + 1) / total * 100))
                    
                except Exception as e:
                    self.log_message.emit(f"❌ {stock_code} 分析失败: {str(e)}", "error")
                    # 继续处理下一只股票
                    continue
                
            self.log_message.emit(f"🎉 批量分析完成！成功分析 {len(results)}/{total} 只股票", "success")
            
            # 移除日志处理器
            self.analyzer.logger.removeHandler(log_handler)
            
            self.finished.emit(results)
            
        except Exception as e:
            self.log_message.emit(f"❌ 批量分析出现严重错误: {str(e)}", "error")
            try:
                self.analyzer.logger.removeHandler(log_handler)
            except:
                pass
            self.error.emit(str(e))

class EnhancedScoreCard(QFrame):
    """增强版评分卡片组件"""
    def __init__(self, title, score, max_score=100, additional_info="", parent=None):
        super().__init__(parent)
        self.setFixedSize(140, 120)
        
        # 根据分数设置渐变颜色
        if score >= 80:
            gradient = "linear-gradient(135deg, #56ab2f 0%, #a8e6cf 100%)"  # 绿色 - 优秀
        elif score >= 60:
            gradient = "linear-gradient(135deg, #667eea 0%, #764ba2 100%)"  # 蓝色 - 良好
        elif score >= 40:
            gradient = "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)"  # 粉色 - 一般
        else:
            gradient = "linear-gradient(135deg, #ff4b2b 0%, #ff416c 100%)"  # 红色 - 较差
            
        self.setStyleSheet(f"""
            QFrame {{
                background: {gradient};
                border-radius: 12px;
                border: none;
            }}
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(2)
        
        # 标题
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: white;
                font-size: 12px;
                font-weight: 600;
                background: transparent;
            }
        """)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # 分数
        score_label = QLabel(f"{score:.1f}")
        score_label.setStyleSheet("""
            QLabel {
                color: white;
                font-size: 24px;
                font-weight: bold;
                background: transparent;
            }
        """)
        score_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(score_label)
        
        # 满分标识
        max_label = QLabel(f"/{max_score}")
        max_label.setStyleSheet("""
            QLabel {
                color: rgba(255,255,255,0.8);
                font-size: 10px;
                background: transparent;
            }
        """)
        max_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(max_label)
        
        # 附加信息
        if additional_info:
            info_label = QLabel(additional_info)
            info_label.setStyleSheet("""
                QLabel {
                    color: rgba(255,255,255,0.9);
                    font-size: 9px;
                    background: transparent;
                }
            """)
            info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            info_label.setWordWrap(True)
            layout.addWidget(info_label)

class DataQualityIndicator(QFrame):
    """数据质量指示器"""
    def __init__(self, title, value, unit="", parent=None):
        super().__init__(parent)
        self.setFixedSize(100, 60)
        self.setStyleSheet("""
            QFrame {
                background-color: rgba(255,255,255,0.9);
                border-radius: 8px;
                border: 1px solid #e9ecef;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(2)
        
        # 数值
        value_label = QLabel(f"{value}{unit}")
        value_label.setStyleSheet("""
            QLabel {
                color: #2c3e50;
                font-size: 16px;
                font-weight: bold;
                background: transparent;
            }
        """)
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(value_label)
        
        # 标题
        title_label = QLabel(title)
        title_label.setStyleSheet("""
            QLabel {
                color: black;
                font-size: 10px;
                background: transparent;
            }
        """)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)

class ModernStockAnalyzerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # 初始化日志显示（会在init_ui中创建）
        self.log_display = None
        
        # 初始化UI
        self.init_ui()
        self.adjust_size_and_position()
        
        # 初始化分析器并显示状态
        self.init_analyzer()

    def init_analyzer(self):
        """初始化分析器并智能检测功能"""
        try:
            self.log_display.append_streaming_text(f"🚀 正在初始化股票分析系统...", "info")
            
            # 创建分析器实例
            self.analyzer = ANALYZER_CLASS()
            
            # 智能检测分析器功能水平
            detected_features = self.detect_analyzer_features()
            actual_version = self.determine_actual_version(detected_features)
            
            self.log_display.append_streaming_text(f"✅ {actual_version}分析器初始化成功", "success")
            self.log_display.append_streaming_text(f"🔍 检测到功能: {', '.join(detected_features)}", "info")
            
            # 更新全局版本变量和界面
            global ANALYZER_VERSION
            ANALYZER_VERSION = actual_version
            
            # 更新界面标题
            self.update_title_version(actual_version)
            
            self.log_display.append_streaming_text("📊 正在检查系统环境...", "info")
            
            # 检查依赖
            self.check_dependencies()
            
            # 检查分析器特定功能
            self.check_analyzer_capabilities()
            
            self.log_display.append_streaming_text("🎉 系统初始化完成，可以开始分析！", "success")
            self.log_display.append_streaming_text("💡 支持股票代码：000001, 600036, 300019等", "info")
            
        except Exception as e:
            if self.log_display:
                self.log_display.append_streaming_text(f"❌ 分析器初始化失败: {str(e)}", "error")
            
            error_details = f"""无法初始化股票分析器：

错误信息：{str(e)}

请检查：
1. stock_analyzer.py 文件是否存在并且语法正确
2. 相关依赖是否已安装：
   pip install akshare pandas numpy jieba PyQt6 markdown2
3. 网络连接是否正常
4. Python版本是否兼容 (建议Python 3.8+)

文件应包含以下类之一：
- EnhancedStockAnalyzer (推荐)
- ComprehensiveStockAnalyzer
- StockAnalyzer"""
            
            QMessageBox.critical(self, "初始化错误", error_details)
            sys.exit(1)

    def detect_analyzer_features(self):
        """智能检测分析器功能"""
        features = []
        
        # 检测增强版功能
        enhanced_methods = [
            'get_comprehensive_fundamental_data',
            'get_comprehensive_news_data',
            'calculate_advanced_sentiment_analysis',
            '_calculate_core_financial_indicators'
        ]
        
        # 检测标准版功能
        standard_methods = [
            'get_fundamental_data',
            'get_news_data',
            'calculate_news_sentiment',
            'get_sentiment_analysis'
        ]
        
        # 检测基础功能
        basic_methods = [
            'get_stock_data',
            'calculate_technical_indicators',
            'analyze_stock'
        ]
        
        enhanced_count = sum(1 for method in enhanced_methods if hasattr(self.analyzer, method))
        standard_count = sum(1 for method in standard_methods if hasattr(self.analyzer, method))
        basic_count = sum(1 for method in basic_methods if hasattr(self.analyzer, method))
        
        if enhanced_count >= 3:
            features.append("增强版功能")
        elif standard_count >= 3:
            features.append("标准版功能")
        elif basic_count >= 2:
            features.append("基础功能")
        
        # 检测特定功能
        if hasattr(self.analyzer, 'analysis_params'):
            params = self.analyzer.analysis_params
            if params.get('financial_indicators_count', 0) >= 20:
                features.append("25项财务指标")
            if params.get('max_news_count', 0) >= 100:
                features.append("综合新闻分析")
        
        if hasattr(self.analyzer, 'api_keys'):
            features.append("AI分析支持")
            
        if hasattr(self.analyzer, 'set_streaming_config'):
            features.append("流式推理")
            
        return features

    def determine_actual_version(self, features):
        """根据检测到的功能确定实际版本"""
        if "增强版功能" in features and "25项财务指标" in features:
            return "Enhanced v3.0"
        elif "增强版功能" in features:
            return "Enhanced v2.5"
        elif "标准版功能" in features:
            return "Standard v2.0"
        elif "基础功能" in features:
            return "Basic v1.0"
        else:
            return "Custom Version"

    def check_dependencies(self):
        """检查依赖库"""
        try:
            import akshare
            self.log_display.append_streaming_text(f"✅ akshare {akshare.__version__} 数据源连接正常", "success")
            
            # 测试akshare基本功能
            try:
                test_data = akshare.tool_trade_date_hist_sina()
                if not test_data.empty:
                    self.log_display.append_streaming_text("✅ akshare API测试成功", "success")
                else:
                    self.log_display.append_streaming_text("⚠️ akshare API响应为空，可能网络不稳定", "warning")
            except Exception as e:
                self.log_display.append_streaming_text(f"⚠️ akshare API测试失败: {str(e)[:50]}...", "warning")
                
        except ImportError:
            self.log_display.append_streaming_text("❌ akshare 未安装，数据获取将受限", "error")
            QMessageBox.warning(self, "依赖缺失", "akshare库未安装，请运行：pip install akshare")
        
        try:
            import jieba
            self.log_display.append_streaming_text("✅ jieba 中文分词工具就绪", "success")
        except ImportError:
            self.log_display.append_streaming_text("⚠️ jieba 未安装，情绪分析将使用简化模式", "warning")
        
        try:
            import pandas as pd
            import numpy as np
            self.log_display.append_streaming_text(f"✅ 数据处理库就绪 (pandas {pd.__version__})", "success")
        except ImportError:
            self.log_display.append_streaming_text("❌ pandas/numpy 未安装", "error")

    def check_analyzer_capabilities(self):
        """检查分析器特定功能"""
        # 检查增强版功能
        if hasattr(self.analyzer, 'get_comprehensive_fundamental_data'):
            self.log_display.append_streaming_text("✅ 25项财务指标分析功能就绪", "success")
        
        if hasattr(self.analyzer, 'get_comprehensive_news_data'):
            self.log_display.append_streaming_text("✅ 综合新闻数据获取功能就绪", "success")
        
        if hasattr(self.analyzer, 'calculate_advanced_sentiment_analysis'):
            self.log_display.append_streaming_text("✅ 高级情绪分析功能就绪", "success")
        
        # 检查AI配置
        if hasattr(self.analyzer, 'api_keys'):
            ai_available = any(key.strip() for key in self.analyzer.api_keys.values() if isinstance(key, str))
            if ai_available:
                self.log_display.append_streaming_text("✅ AI分析功能已配置", "success")
            else:
                self.log_display.append_streaming_text("ℹ️ 未配置AI API，将使用内置高级分析", "info")
        
        # 显示配置信息
        if hasattr(self.analyzer, 'analysis_params'):
            params = self.analyzer.analysis_params
            financial_count = params.get('financial_indicators_count', 'N/A')
            news_count = params.get('max_news_count', 'N/A')
            self.log_display.append_streaming_text(f"📊 财务指标数量: {financial_count}", "info")
            self.log_display.append_streaming_text(f"📰 最大新闻数量: {news_count}", "info")
        
        # 检查方法兼容性
        method_compatibility = {
            'analyze_stock': '核心分析',
            'get_stock_data': '股票数据获取',
            'calculate_technical_indicators': '技术指标计算',
            'get_fundamental_data': '基本面数据',
            'get_news_data': '新闻数据',
            'calculate_news_sentiment': '情绪分析'
        }
        
        available_methods = []
        for method, description in method_compatibility.items():
            if hasattr(self.analyzer, method):
                available_methods.append(description)
        
        if available_methods:
            self.log_display.append_streaming_text(f"🔧 可用功能: {', '.join(available_methods)}", "info")

    def adjust_size_and_position(self):
        """调整窗口大小和位置以适应不同分辨率"""
        screen = QApplication.primaryScreen()
        if screen:
            geometry = screen.availableGeometry()
            # 设置窗口大小为屏幕的80%
            width = int(geometry.width() * 0.85)
            height = int(geometry.height() * 0.85)
            self.resize(width, height)
            
            # 居中显示
            center = geometry.center()
            frame = self.frameGeometry()
            frame.moveCenter(center)
            self.move(frame.topLeft())

    def init_ui(self):
        self.setWindowTitle('现代股票分析系统 - 加载中...')
        self.setStyleSheet("""
            QMainWindow {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            }
        """)

        # 创建中央部件和主布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # 创建标题区域
        title_frame = self.create_title_section()
        main_layout.addWidget(title_frame)

        # 创建主要内容区域
        content_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # 左侧：输入和控制区域
        left_widget = self.create_input_section()
        content_splitter.addWidget(left_widget)
        
        # 右侧：结果显示区域
        right_widget = self.create_result_section()
        content_splitter.addWidget(right_widget)
        
        content_splitter.setStretchFactor(0, 1)
        content_splitter.setStretchFactor(1, 2)
        
        main_layout.addWidget(content_splitter)

    def create_title_section(self):
        """创建标题区域"""
        frame = ModernFrame(elevated=True)
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(24, 16, 24, 16)
        
        title_label = QLabel(' ')
        title_label.setStyleSheet("""
            QLabel {
                color: #2c3e50;
                font-size: 26px;
                font-weight: bold;
                background: transparent;
            }
        """)
        layout.addWidget(title_label)
        
        layout.addStretch()
        
        # 版本信息（动态更新）
        self.version_label = QLabel(f"基于{ANALYZER_CLASS.__name__}")
        self.version_label.setStyleSheet("""
            QLabel {
                color: black;
                font-size: 12px;
                background: transparent;
            }
        """)
        layout.addWidget(self.version_label)
        
        # 配置按钮
        config_btn = ModernButton('⚙️ 设置', button_type="secondary")
        config_btn.clicked.connect(self.show_config_dialog)
        layout.addWidget(config_btn)
        
        return frame

    def update_title_version(self, version):
        """更新标题栏版本信息"""
        if hasattr(self, 'version_label'):
            self.version_label.setText(f"{version} | {ANALYZER_CLASS.__name__}")
            self.setWindowTitle(f'现代股票分析系统 - {version}')

    def create_input_section(self):
        """创建输入控制区域"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        
        # 创建标签页
        tab_widget = QTabWidget()
        tab_widget.setStyleSheet("""
            QTabWidget::pane {
                border: none;
                background-color: white;
                border-radius: 8px;
            }
            QTabBar::tab {
                background: #f8f9fa;
                border: 1px solid #dee2e6;
                padding: 12px 24px;
                margin: 2px;
                border-radius: 6px;
                font-weight: 600;
                color: black;
            }
            QTabBar::tab:selected {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border-color: #667eea;
            }
            QTabBar::tab:hover {
                background: #e9ecef;
                color: black;
            }
        """)
        
        # 单只股票分析标签页
        single_tab = self.create_single_stock_tab()
        tab_widget.addTab(single_tab, "📈 单只分析")
        
        # 批量分析标签页
        batch_tab = self.create_batch_stock_tab()
        tab_widget.addTab(batch_tab, "📊 批量分析")
        
        layout.addWidget(tab_widget)
        
        # 日志显示区域
        log_frame = ModernFrame()
        log_layout = QVBoxLayout(log_frame)
        log_layout.setContentsMargins(16, 16, 16, 16)
        
        # 日志标题和控制按钮
        log_header = QHBoxLayout()
        log_label = QLabel('📋 分析日志')
        log_label.setStyleSheet("""
            QLabel {
                color: black;
                font-size: 16px;
                font-weight: bold;
                background: transparent;
            }
        """)
        log_header.addWidget(log_label)
        log_header.addStretch()
        
        # 清空日志按钮
        clear_log_btn = ModernButton('🗑️ 清空', button_type="secondary")
        clear_log_btn.setMaximumWidth(80)
        clear_log_btn.clicked.connect(self.clear_log)
        log_header.addWidget(clear_log_btn)
        
        log_layout.addLayout(log_header)
        
        # 日志显示区域
        self.log_display = StreamingDisplay()
        self.log_display.setMaximumHeight(250)
        self.log_display.append_streaming_text("📋 系统就绪，等待分析任务...", "info")
        log_layout.addWidget(self.log_display)
        
        layout.addWidget(log_frame)
        
        return widget

    def create_single_stock_tab(self):
        """创建单只股票分析标签页"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)

        # 股票代码输入
        input_group = QGroupBox("股票代码")
        input_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
                color: black;
                border: 2px solid #e9ecef;
                border-radius: 8px;
                margin-top: 12px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 8px 0 8px;
                background-color: white;
            }
        """)
        input_layout = QVBoxLayout(input_group)
        
        self.single_stock_input = ModernLineEdit()
        self.single_stock_input.setPlaceholderText('输入股票代码（如：000001、600036、300019）')
        input_layout.addWidget(self.single_stock_input)
        
        layout.addWidget(input_group)

        # 分析选项
        options_group = QGroupBox("分析选项")
        options_group.setStyleSheet(input_group.styleSheet())
        options_layout = QVBoxLayout(options_group)
        
        self.enable_streaming_cb = QCheckBox("启用流式推理显示")
        self.enable_streaming_cb.setChecked(True)
        self.enable_streaming_cb.setStyleSheet("""
            QCheckBox {
                font-size: 14px;
                color: black;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 3px;
                border: 2px solid #adb5bd;
            }
            QCheckBox::indicator:checked {
                background-color: #667eea;
                border-color: #667eea;
            }
        """)
        options_layout.addWidget(self.enable_streaming_cb)
        
        layout.addWidget(options_group)

        # 分析按钮
        self.analyze_btn = ModernButton('🔍 开始深度分析', button_type="primary")
        self.analyze_btn.clicked.connect(self.analyze_single_stock)
        layout.addWidget(self.analyze_btn)

        # 进度条
        self.progress_bar = ModernProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        layout.addStretch()
        return widget

    def create_batch_stock_tab(self):
        """创建批量分析标签页"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)

        # 股票代码输入
        input_group = QGroupBox("股票代码列表")
        input_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
                color: black;
                border: 2px solid #e9ecef;
                border-radius: 8px;
                margin-top: 12px;
                padding-top: 12px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 8px 0 8px;
                background-color: white;
            }
        """)
        input_layout = QVBoxLayout(input_group)
        
        self.batch_stock_input = ModernTextEdit()
        self.batch_stock_input.setPlaceholderText('输入多个股票代码，每行一个\n例如：\n000001\n000002\n600036\n300019')
        self.batch_stock_input.setMaximumHeight(120)
        input_layout.addWidget(self.batch_stock_input)
        
        layout.addWidget(input_group)

        # 批量分析按钮
        self.batch_analyze_btn = ModernButton('📊 批量深度分析', button_type="success")
        self.batch_analyze_btn.clicked.connect(self.analyze_multiple_stocks)
        layout.addWidget(self.batch_analyze_btn)

        # 批量进度条
        self.batch_progress_bar = ModernProgressBar()
        self.batch_progress_bar.setVisible(False)
        layout.addWidget(self.batch_progress_bar)
        
        # 当前分析股票显示
        self.current_stock_label = QLabel()
        self.current_stock_label.setStyleSheet("""
            QLabel {
                color: black;
                font-size: 12px;
                font-style: italic;
                background: transparent;
            }
        """)
        self.current_stock_label.setVisible(False)
        layout.addWidget(self.current_stock_label)

        layout.addStretch()
        return widget

    def create_result_section(self):
        """创建结果显示区域"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        
        # 结果标题
        result_frame = ModernFrame(elevated=True)
        result_title_layout = QHBoxLayout(result_frame)
        result_title_layout.setContentsMargins(20, 12, 20, 12)
        
        result_label = QLabel('📋 分析结果')
        result_label.setStyleSheet("""
            QLabel {
                color: #2c3e50;
                font-size: 20px;
                font-weight: bold;
                background: transparent;
            }
        """)
        result_title_layout.addWidget(result_label)
        result_title_layout.addStretch()
        
        # 导出按钮
        export_btn = ModernButton('📤 导出报告', button_type="secondary")
        export_btn.clicked.connect(self.export_report)
        result_title_layout.addWidget(export_btn)
        
        layout.addWidget(result_frame)
        
        # 评分卡片区域
        self.score_frame = ModernFrame()
        self.score_layout = QHBoxLayout(self.score_frame)
        self.score_layout.setContentsMargins(20, 20, 20, 20)
        self.score_frame.setVisible(False)
        layout.addWidget(self.score_frame)
        
        # 数据质量指示器区域
        self.data_quality_frame = ModernFrame()
        self.data_quality_layout = QHBoxLayout(self.data_quality_frame)
        self.data_quality_layout.setContentsMargins(20, 12, 20, 12)
        self.data_quality_frame.setVisible(False)
        layout.addWidget(self.data_quality_frame)
        
        # 结果显示浏览器
        self.result_browser = QTextBrowser()
        self.result_browser.setOpenExternalLinks(True)
        self.result_browser.setStyleSheet("""
            QTextBrowser {
                border: 2px solid #e9ecef;
                border-radius: 12px;
                padding: 20px;
                background-color: white;
                font-size: 14px;
                line-height: 1.6;
            }
        """)
        layout.addWidget(self.result_browser)
        
        return widget

    def format_enhanced_report(self, report, is_single=True):
        """格式化增强版分析报告"""
        stock_name = report.get('stock_name', report['stock_code'])
        
        # 获取数据质量信息
        data_quality = report.get('data_quality', {})
        financial_count = data_quality.get('financial_indicators_count', 0)
        news_count = report.get('sentiment_analysis', {}).get('total_analyzed', 0)
        
        md = f"""# 📈 股票分析报告 ({ANALYZER_VERSION})

## 🏢 基本信息
| 项目 | 值 |
|------|-----|
| **股票代码** | {report['stock_code']} |
| **股票名称** | {stock_name} |
| **分析时间** | {report['analysis_date']} |
| **当前价格** | ¥{report['price_info']['current_price']:.2f} |
| **价格变动** | {report['price_info']['price_change']:+.2f}% |
| **成交量比率** | {report['price_info']['volume_ratio']:.2f} |
| **波动率** | {report['price_info']['volatility']:.2f}% |

## 📊 综合评分

### 🎯 总体评分：{report['scores']['comprehensive']:.1f}/100

| 维度 | 得分 | 权重 | 评级 |
|------|------|------|------|
| **技术分析** | {report['scores']['technical']:.1f}/100 | {report['analysis_weights']['technical']*100:.0f}% | {'优秀' if report['scores']['technical'] >= 80 else '良好' if report['scores']['technical'] >= 60 else '一般' if report['scores']['technical'] >= 40 else '较差'} |
| **基本面分析** | {report['scores']['fundamental']:.1f}/100 | {report['analysis_weights']['fundamental']*100:.0f}% | {'优秀' if report['scores']['fundamental'] >= 80 else '良好' if report['scores']['fundamental'] >= 60 else '一般' if report['scores']['fundamental'] >= 40 else '较差'} |
| **情绪分析** | {report['scores']['sentiment']:.1f}/100 | {report['analysis_weights']['sentiment']*100:.0f}% | {'优秀' if report['scores']['sentiment'] >= 80 else '良好' if report['scores']['sentiment'] >= 60 else '一般' if report['scores']['sentiment'] >= 40 else '较差'} |

## 📋 数据质量
| 项目 | 数量 | 质量评估 |
|------|------|----------|
| **财务指标** | {financial_count} 项 | {'优秀' if financial_count >= 20 else '良好' if financial_count >= 15 else '一般' if financial_count >= 10 else '需改善'} |
| **新闻数据** | {news_count} 条 | {'丰富' if news_count >= 50 else '充足' if news_count >= 20 else '一般' if news_count >= 10 else '稀少'} |
| **分析完整度** | - | {data_quality.get('analysis_completeness', '部分')} |

## 🔧 技术面分析
| 指标 | 值 | 状态 | 说明 |
|------|-----|------|------|
| **均线趋势** | - | {report['technical_analysis']['ma_trend']} | 多头排列看涨，空头排列看跌 |
| **RSI指标** | {report['technical_analysis']['rsi']:.1f} | {'超买' if report['technical_analysis']['rsi'] > 70 else '超卖' if report['technical_analysis']['rsi'] < 30 else '正常'} | 30-70为正常区间 |
| **MACD信号** | - | {report['technical_analysis']['macd_signal']} | 金叉看涨，死叉看跌 |
| **成交量状态** | - | {report['technical_analysis']['volume_status']} | 放量配合价格变动更有效 |
| **布林带位置** | {report['technical_analysis']['bb_position']:.2f} | {'上轨附近' if report['technical_analysis']['bb_position'] > 0.8 else '下轨附近' if report['technical_analysis']['bb_position'] < 0.2 else '中位运行'} | 上轨阻力，下轨支撑 |

## 💰 基本面分析

**基本面得分：{report['scores']['fundamental']:.1f}/100**

### 📈 核心财务指标
"""

        # 添加财务指标详情（如果有的话）
        fundamental_data = report.get('fundamental_data', {})
        financial_indicators = fundamental_data.get('financial_indicators', {})
        
        if financial_indicators:
            md += "\n| 指标名称 | 数值 |\n|----------|------|\n"
            # 显示前10个重要的财务指标
            count = 0
            for key, value in financial_indicators.items():
                if count >= 10:
                    break
                if isinstance(value, (int, float)) and value != 0:
                    md += f"| {key} | {value} |\n"
                    count += 1
        else:
            md += "\n基本面数据包含了公司的财务状况、估值水平、盈利能力等关键指标的综合评估。\n"

        # 继续添加其他部分
        sentiment_analysis = report.get('sentiment_analysis', {})
        
        md += f"""

## 📰 市场情绪分析

| 项目 | 值 | 备注 |
|------|-----|------|
| **情绪趋势** | {sentiment_analysis.get('sentiment_trend', '中性')} | 基于新闻和公告分析 |
| **情绪得分** | {sentiment_analysis.get('overall_sentiment', 0):.3f} | -1到1之间，0为中性 |
| **新闻总数** | {sentiment_analysis.get('total_analyzed', 0)} | 公司新闻+公告+研报 |
| **正面新闻比例** | {sentiment_analysis.get('positive_ratio', 0):.1%} | 积极情绪新闻占比 |
| **负面新闻比例** | {sentiment_analysis.get('negative_ratio', 0):.1%} | 消极情绪新闻占比 |

### 📊 新闻数据分布
"""
        
        # 添加新闻分布信息
        if 'news_summary' in sentiment_analysis:
            news_summary = sentiment_analysis['news_summary']
            md += f"""
| 新闻类型 | 数量 |
|----------|------|
| 公司新闻 | {news_summary.get('company_news_count', 0)} 条 |
| 公司公告 | {news_summary.get('announcements_count', 0)} 条 |
| 研究报告 | {news_summary.get('research_reports_count', 0)} 条 |
| 行业新闻 | {news_summary.get('industry_news_count', 0)} 条 |
"""

        md += f"""

## 🎯 投资建议

### {report['recommendation']}

## 🤖 AI综合分析

{report['ai_analysis']}

---
*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*  
*分析器版本：{ANALYZER_VERSION}*  
*分析器类：{ANALYZER_CLASS.__name__}*  
*数据来源：多维度综合分析*
"""
        return md

    def clear_log(self):
        """清空日志显示"""
        self.log_display.clear_log()

    def analyze_single_stock(self):
        """分析单只股票"""
        stock_code = self.single_stock_input.text().strip()
        if not stock_code:
            self.show_warning('请输入股票代码')
            return

        self.analyze_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)

        enable_streaming = self.enable_streaming_cb.isChecked()
        
        self.worker = AnalysisWorker(self.analyzer, stock_code, enable_streaming)
        self.worker.finished.connect(self.handle_single_analysis_result)
        self.worker.error.connect(self.handle_analysis_error)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.log_message.connect(self.log_display.append_streaming_text)
        
        self.worker.start()

    def analyze_multiple_stocks(self):
        """批量分析股票"""
        text = self.batch_stock_input.toPlainText().strip()
        if not text:
            self.show_warning('请输入股票代码')
            return

        stock_list = [code.strip() for code in text.split('\n') if code.strip()]
        
        self.log_display.append_streaming_text(f"📊 准备批量分析以下股票: {', '.join(stock_list)}", "info")
        
        self.batch_analyze_btn.setEnabled(False)
        self.batch_progress_bar.setVisible(True)
        self.current_stock_label.setVisible(True)
        self.batch_progress_bar.setValue(0)

        self.batch_worker = BatchAnalysisWorker(self.analyzer, stock_list)
        self.batch_worker.finished.connect(self.handle_batch_analysis_result)
        self.batch_worker.error.connect(self.handle_analysis_error)
        self.batch_worker.progress.connect(self.batch_progress_bar.setValue)
        self.batch_worker.current_stock.connect(self.current_stock_label.setText)
        self.batch_worker.log_message.connect(self.log_display.append_streaming_text)
        self.batch_worker.start()

    def handle_single_analysis_result(self, report):
        """处理单只股票分析结果"""
        # 更新评分卡片
        self.update_score_cards(report['scores'])
        
        # 更新数据质量指示器
        self.update_data_quality_indicators(report)
        
        # 更新结果显示
        markdown_text = self.format_enhanced_report(report)
        html_content = markdown2.markdown(markdown_text, extras=['tables', 'fenced-code-blocks'])
        self.result_browser.setHtml(html_content)
        
        self.analyze_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        
        # 存储最新报告用于导出
        self.latest_report = report

    def handle_batch_analysis_result(self, recommendations):
        """处理批量分析结果"""
        if not recommendations:
            self.show_warning('没有成功分析的股票')
            return
            
        # 计算平均分数
        avg_scores = {
            'comprehensive': sum(r['scores']['comprehensive'] for r in recommendations) / len(recommendations),
            'technical': sum(r['scores']['technical'] for r in recommendations) / len(recommendations),
            'fundamental': sum(r['scores']['fundamental'] for r in recommendations) / len(recommendations),
            'sentiment': sum(r['scores']['sentiment'] for r in recommendations) / len(recommendations)
        }
        
        self.update_score_cards(avg_scores)
        
        # 更新数据质量指示器（批量）
        avg_financial = sum(r.get('data_quality', {}).get('financial_indicators_count', 0) for r in recommendations) / len(recommendations)
        avg_news = sum(r.get('sentiment_analysis', {}).get('total_analyzed', 0) for r in recommendations) / len(recommendations)
        
        batch_data_quality = {
            'data_quality': {
                'financial_indicators_count': int(avg_financial),
                'analysis_completeness': '批量分析'
            },
            'sentiment_analysis': {
                'total_analyzed': int(avg_news)
            }
        }
        self.update_data_quality_indicators(batch_data_quality)
        
        # 生成批量报告
        markdown_text = f"# 📊 批量股票分析报告\n\n"
        markdown_text += f"**分析时间：** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        markdown_text += f"**分析数量：** {len(recommendations)} 只股票\n\n"
        markdown_text += f"**分析器版本：** {ANALYZER_VERSION}\n\n"
        markdown_text += f"**分析器类：** {ANALYZER_CLASS.__name__}\n\n"
        
        # 添加汇总表格
        markdown_text += "## 📋 分析汇总\n\n"
        markdown_text += "| 排名 | 股票代码 | 股票名称 | 综合得分 | 技术面 | 基本面 | 情绪面 | 投资建议 |\n"
        markdown_text += "|------|----------|----------|----------|--------|--------|--------|----------|\n"
        
        for i, rec in enumerate(sorted(recommendations, key=lambda x: x['scores']['comprehensive'], reverse=True), 1):
            stock_name = rec.get('stock_name', rec['stock_code'])
            markdown_text += f"| {i} | {rec['stock_code']} | {stock_name} | {rec['scores']['comprehensive']:.1f} | {rec['scores']['technical']:.1f} | {rec['scores']['fundamental']:.1f} | {rec['scores']['sentiment']:.1f} | {rec['recommendation']} |\n"
        
        # 添加详细分析
        markdown_text += "\n## 📈 详细分析\n\n"
        for rec in recommendations:
            markdown_text += self.format_enhanced_report(rec, False)
            markdown_text += "\n---\n\n"
            
        html_content = markdown2.markdown(markdown_text, extras=['tables', 'fenced-code-blocks'])
        self.result_browser.setHtml(html_content)
        
        self.batch_analyze_btn.setEnabled(True)
        self.batch_progress_bar.setVisible(False)
        self.current_stock_label.setVisible(False)
        
        # 存储最新批量报告用于导出
        self.latest_batch_report = recommendations

    def update_score_cards(self, scores):
        """更新评分卡片"""
        # 清空现有卡片
        for i in reversed(range(self.score_layout.count())): 
            self.score_layout.itemAt(i).widget().setParent(None)
        
        # 创建新的增强版评分卡片
        comprehensive_card = EnhancedScoreCard("综合得分", scores['comprehensive'], 
                                             additional_info=self.get_score_description(scores['comprehensive']))
        technical_card = EnhancedScoreCard("技术分析", scores['technical'],
                                         additional_info=self.get_score_description(scores['technical']))
        fundamental_card = EnhancedScoreCard("基本面", scores['fundamental'],
                                           additional_info=self.get_score_description(scores['fundamental']))
        sentiment_card = EnhancedScoreCard("市场情绪", scores['sentiment'],
                                         additional_info=self.get_score_description(scores['sentiment']))
        
        self.score_layout.addWidget(comprehensive_card)
        self.score_layout.addWidget(technical_card)
        self.score_layout.addWidget(fundamental_card)
        self.score_layout.addWidget(sentiment_card)
        self.score_layout.addStretch()
        
        self.score_frame.setVisible(True)

    def update_data_quality_indicators(self, report):
        """更新数据质量指示器"""
        # 清空现有指示器
        for i in reversed(range(self.data_quality_layout.count())): 
            self.data_quality_layout.itemAt(i).widget().setParent(None)
        
        # 创建数据质量指示器
        data_quality = report.get('data_quality', {})
        sentiment_analysis = report.get('sentiment_analysis', {})
        
        financial_count = data_quality.get('financial_indicators_count', 0)
        news_count = sentiment_analysis.get('total_analyzed', 0)
        completeness = data_quality.get('analysis_completeness', '部分')
        
        # 添加标签
        quality_label = QLabel('📊 数据质量指标')
        quality_label.setStyleSheet("""
            QLabel {
                color: black;
                font-size: 14px;
                font-weight: bold;
                background: transparent;
                margin-right: 20px;
            }
        """)
        self.data_quality_layout.addWidget(quality_label)
        
        # 添加指示器
        financial_indicator = DataQualityIndicator("财务指标", financial_count, "项")
        news_indicator = DataQualityIndicator("新闻数据", news_count, "条")
        completeness_indicator = DataQualityIndicator("完整度", completeness[:2], "")
        
        self.data_quality_layout.addWidget(financial_indicator)
        self.data_quality_layout.addWidget(news_indicator)
        self.data_quality_layout.addWidget(completeness_indicator)
        self.data_quality_layout.addStretch()
        
        self.data_quality_frame.setVisible(True)

    def get_score_description(self, score):
        """获取分数描述"""
        if score >= 80:
            return "优秀"
        elif score >= 60:
            return "良好"
        elif score >= 40:
            return "一般"
        else:
            return "较差"

    def handle_analysis_error(self, error_message):
        """处理分析错误"""
        self.show_error(f'分析过程中出现错误：{error_message}')
        self.analyze_btn.setEnabled(True)
        self.batch_analyze_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.batch_progress_bar.setVisible(False)
        self.current_stock_label.setVisible(False)

    def export_report(self):
        """导出报告"""
        try:
            self.log_display.append_streaming_text("📤 开始导出分析报告...", "info")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if hasattr(self, 'latest_report'):
                # 导出单个报告
                filename = f"stock_analysis_{self.latest_report['stock_code']}_{timestamp}.md"
                content = self.format_enhanced_report(self.latest_report)
                report_type = f"单个股票({self.latest_report['stock_code']})"
                
                # 添加详细的数据统计
                data_quality = self.latest_report.get('data_quality', {})
                financial_count = data_quality.get('financial_indicators_count', 0)
                news_count = self.latest_report.get('sentiment_analysis', {}).get('total_analyzed', 0)
                
            elif hasattr(self, 'latest_batch_report'):
                # 导出批量报告
                filename = f"batch_analysis_{timestamp}.md"
                content = f"# 批量股票分析报告 - {ANALYZER_VERSION}\n\n"
                for rec in self.latest_batch_report:
                    content += self.format_enhanced_report(rec, False)
                    content += "\n---\n\n"
                report_type = f"批量分析({len(self.latest_batch_report)}只股票)"
                
                # 计算统计信息
                total_financial = sum(r.get('data_quality', {}).get('financial_indicators_count', 0) for r in self.latest_batch_report)
                total_news = sum(r.get('sentiment_analysis', {}).get('total_analyzed', 0) for r in self.latest_batch_report)
                
            else:
                self.log_display.append_streaming_text("⚠️ 没有可导出的报告", "warning")
                self.show_warning('没有可导出的报告')
                return
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.log_display.append_streaming_text(f"✅ {report_type}报告导出成功: {filename}", "success")
            
            file_size = len(content.encode("utf-8"))/1024
            
            QMessageBox.information(self, '导出成功', 
                                  f'分析报告已导出！\n\n'
                                  f'📄 文件名：{filename}\n'
                                  f'📊 报告类型：{report_type}\n'
                                  f'📏 文件大小：{file_size:.1f} KB\n'
                                  f'🔧 分析器：{ANALYZER_VERSION} | {ANALYZER_CLASS.__name__}')
            
        except Exception as e:
            error_msg = f'导出失败：{str(e)}'
            self.log_display.append_streaming_text(f"❌ {error_msg}", "error")
            self.show_error(error_msg)

    def show_config_dialog(self):
        """显示配置对话框"""
        self.log_display.append_streaming_text("⚙️ 打开配置对话框", "info")
        
        # 获取分析器功能信息
        features = self.detect_analyzer_features() if hasattr(self, 'analyzer') else ['基础功能']
        
        config_msg = f'''🔧 {ANALYZER_VERSION} 配置管理

当前分析器：{ANALYZER_CLASS.__name__}
检测功能：{', '.join(features)}

📋 可配置项目：
• AI模型选择 (GPT-4, Claude, 智谱AI等)
• 财务指标数量控制 (支持1-25项)
• 新闻数据获取量 (支持10-200条)
• 分析权重调整 (技术面/基本面/情绪面)
• API密钥管理
• 数据源选择和缓存设置
• 流式推理显示控制

💡 配置方法：
1. 编辑 config.json 文件
2. 或修改 stock_analyzer.py 内的默认配置
3. 重启程序生效

🎯 当前功能：
{chr(10).join(f"• {feature}" for feature in features)}

📁 文件位置：
• 分析器：stock_analyzer.py
• 配置文件：config.json (可选)'''
        
        QMessageBox.information(self, '系统配置', config_msg)

    def show_warning(self, message):
        """显示警告对话框"""
        warning = QMessageBox(self)
        warning.setIcon(QMessageBox.Icon.Warning)
        warning.setWindowTitle('⚠️ 警告')
        warning.setText(message)
        warning.setStandardButtons(QMessageBox.StandardButton.Ok)
        warning.setStyleSheet("""
            QMessageBox {
                background-color: white;
                border-radius: 8px;
            }
            QMessageBox QLabel {
                color: #2c3e50;
                min-width: 200px;
                font-size: 14px;
            }
            QPushButton {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                border-radius: 6px;
                padding: 8px 16px;
                min-width: 80px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: linear-gradient(135deg, #5a6fd8 0%, #6a4190 100%);
            }
        """)
        warning.exec()

    def show_error(self, message):
        """显示错误对话框"""
        error = QMessageBox(self)
        error.setIcon(QMessageBox.Icon.Critical)
        error.setWindowTitle('❌ 错误')
        error.setText(message)
        error.setStandardButtons(QMessageBox.StandardButton.Ok)
        error.setStyleSheet("""
            QMessageBox {
                background-color: white;
                border-radius: 8px;
            }
            QMessageBox QLabel {
                color: #e74c3c;
                min-width: 200px;
                font-size: 14px;
            }
            QPushButton {
                background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
                color: white;
                border: none;
                border-radius: 6px;
                padding: 8px 16px;
                min-width: 80px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: linear-gradient(135deg, #c0392b 0%, #a93226 100%);
            }
        """)
        error.exec()

def main():
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 控制台输出
        ]
    )
    
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    # 设置应用图标和基本信息
    app.setApplicationName("现代股票分析系统")
    app.setApplicationVersion("3.0")
    app.setOrganizationName("Smart Stock Analyzer Team")
    
    print("🚀 启动现代股票分析系统...")
    print("📋 系统信息:")
    print(f"   - Python版本: {sys.version}")
    print(f"   - PyQt6版本: {QApplication.applicationVersion()}")
    print(f"   - 分析器文件: stock_analyzer.py")
    print(f"   - 分析器类: {ANALYZER_CLASS.__name__}")
    print(f"   - 预设版本: {ANALYZER_VERSION}")
    print("   - 实际功能: 将在初始化时检测")
    
    # 检查依赖
    missing_deps = []
    try:
        import akshare
        print("   ✅ akshare: 已安装")
    except ImportError:
        missing_deps.append("akshare")
        print("   ❌ akshare: 未安装")
    
    try:
        import jieba
        print("   ✅ jieba: 已安装")
    except ImportError:
        missing_deps.append("jieba")
        print("   ❌ jieba: 未安装")
    
    try:
        import markdown2
        print("   ✅ markdown2: 已安装")
    except ImportError:
        missing_deps.append("markdown2")
        print("   ❌ markdown2: 未安装")
    
    if missing_deps:
        error_msg = f"缺少必要依赖: {', '.join(missing_deps)}\n\n请运行以下命令安装:\npip install {' '.join(missing_deps)}"
        QMessageBox.critical(None, "依赖检查失败", error_msg)
        print(f"❌ 依赖检查失败: {error_msg}")
        sys.exit(1)
    
    # 创建并显示主窗口
    try:
        print("🎨 正在创建用户界面...")
        window = ModernStockAnalyzerGUI()
        window.show()
        print("✅ 系统启动成功！")
        print("💡 系统将自动检测分析器功能并适配界面")
        sys.exit(app.exec())
    except Exception as e:
        error_msg = f"程序启动失败：{str(e)}"
        print(f"❌ {error_msg}")
        QMessageBox.critical(None, "启动错误", error_msg)
        sys.exit(1)

if __name__ == '__main__':
    main()
