"""
量化主线共振狙击系统 - GUI标签页（预测型 v2.0）
核心改进：从"描述现状"升级为"预测未来"
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QTextBrowser, QGroupBox, QTableWidget,
                             QTableWidgetItem, QHeaderView, QSplitter,
                             QMessageBox, QProgressBar)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor
from datetime import datetime
import json

class QuantMainlineThread(QThread):
    """量化分析线程"""
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    
    def __init__(self, analyzer):
        super().__init__()
        self.analyzer = analyzer
    
    def run(self):
        try:
            self._scan()
        except Exception as e:
            self.error.emit(str(e))
    
    def _scan(self):
        """执行扫描"""
        try:
            # 尝试使用预测引擎
            self.progress.emit(10, "🔮 启动预测引擎...")
            
            from mainline_engine.predictive_mainline_identifier import PredictiveMainlineEngine
            from leader_monitor.predictive_leader_selector import PredictiveLeaderMonitor
            from prediction_aggregator.summary_generator import PredictionAggregator
            
            try:
                with open('config/predictive_quant_config.json', 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except:
                config = {}
            
            mainline_engine = PredictiveMainlineEngine(self.analyzer, config.get('mainline_engine', {}))
            leader_monitor = PredictiveLeaderMonitor(self.analyzer, config.get('leader_monitor', {}))
            aggregator = PredictionAggregator(config.get('prediction_aggregator', {}))
            
            self.progress.emit(20, "🔍 识别预测主线...")
            mainlines = mainline_engine.identify_predictive_mainlines()
            self.progress.emit(50, f"✅ {len(mainlines)}个预测主线")
            
            self.progress.emit(60, "👑 构建龙头股池...")
            leaders = leader_monitor.build_predictive_leader_pool(mainlines)
            self.progress.emit(80, f"✅ {len(leaders)}只龙头")
            
            self.progress.emit(90, "🎣 识别超跌候选...")
            oversold = leader_monitor.identify_enhanced_oversold_candidates(leaders, mainlines)
            
            self.progress.emit(95, "🔮 生成预测摘要...")
            summary = aggregator.generate_prediction_summary(mainlines, leaders, oversold)
            
            self.progress.emit(100, "✅ 预测完成")
            
            results = {
                'mainlines': [self._fmt_ml(ml) for ml in mainlines],
                'leaders': [self._fmt_ld(ld) for ld in leaders],
                'oversold': [self._fmt_os(os) for os in oversold],
                'summary': summary.to_dict(),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'mode': 'predictive'
            }
            self.finished.emit(results)
            
        except ImportError:
            self.progress.emit(10, "⚠️ 预测引擎未加载，使用模拟数据")
            self._mock_scan()
    
    def _mock_scan(self):
        """模拟扫描"""
        import time
        self.progress.emit(30, "📊 模拟分析...")
        time.sleep(0.5)
        
        results = {
            'mainlines': [
                {'rank': 1, 'industry': '新能源汽车', 'score': 85.2, 'status': '预测强势', 'days': 5, 'stocks': 45, 'strong': 12},
                {'rank': 2, 'industry': '半导体', 'score': 78.6, 'status': '新兴信号', 'days': 3, 'stocks': 38, 'strong': 9},
            ],
            'leaders': [
                {'code': '300750', 'name': '宁德时代', 'type': '综合龙头', 'industry': '新能源汽车', 
                 'score': 92.5, 'tech': 88, 'fund': 95, 'flow': 5.68},
            ],
            'oversold': [
                {'code': '002415', 'name': '海康威视', 'coefficient': 0.72, 'drawdown': 18.5, 
                 'divergence': True, 'support': 42.30, 'price': 43.80},
            ],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mode': 'mock'
        }
        
        self.progress.emit(100, "✅ 完成")
        self.finished.emit(results)
    
    def _fmt_ml(self, ml):
        return {
            'rank': ml.rank, 'industry': ml.industry_name,
            'score': ml.prediction_score, 'status': ml.status.value,
            'days': ml.continuity_days, 'stocks': ml.total_stocks, 'strong': ml.strong_stocks
        }
    
    def _fmt_ld(self, ld):
        return {
            'code': ld.stock_code, 'name': ld.stock_name, 'type': ld.leader_type,
            'industry': ld.industry, 'score': ld.leader_score,
            'tech': ld.technical_score, 'fund': ld.capital_score,
            'flow': ld.capital_flow_3d.get('main_force_net', 0)
        }
    
    def _fmt_os(self, os):
        return {
            'code': os.stock_code, 'name': os.stock_name,
            'coefficient': os.adjusted_coefficient, 'drawdown': os.drawdown_from_high,
            'divergence': os.technical_divergence, 'support': os.support_level, 'price': os.current_price
        }


class QuantMainlineTab(QWidget):
    """量化主线狙击标签页"""
    
    def __init__(self, analyzer, parent=None):
        super().__init__(parent)
        self.analyzer = analyzer
        self.results = None
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 顶部
        hdr = QHBoxLayout()
        title = QLabel('🎯 量化主线狙击（预测型 v2.0）')
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1a73e8;")
        hdr.addWidget(title)
        hdr.addStretch()
        
        self.scan_btn = QPushButton('🔍 盘前扫描')
        self.scan_btn.setStyleSheet("""
            QPushButton {
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white; border: none; padding: 10px 20px;
                border-radius: 8px; font-weight: bold;
            }
            QPushButton:hover { background: linear-gradient(135deg, #5568d3, #65408b); }
            QPushButton:disabled { background: #ccc; }
        """)
        self.scan_btn.clicked.connect(self.run_scan)
        hdr.addWidget(self.scan_btn)
        
        self.export_btn = QPushButton('📥 导出')
        self.export_btn.setStyleSheet("background: #f0f0f0; border: 2px solid #ddd; padding: 10px 20px; border-radius: 8px;")
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export)
        hdr.addWidget(self.export_btn)
        
        layout.addLayout(hdr)
        
        # 进度条
        self.progress = QProgressBar()
        self.progress.setStyleSheet("""
            QProgressBar {
                border: 2px solid #e3f2fd; border-radius: 8px; height: 25px; background: #f5f5f5;
            }
            QProgressBar::chunk { background: linear-gradient(90deg, #667eea, #764ba2); border-radius: 6px; }
        """)
        self.progress.setVisible(False)
        layout.addWidget(self.progress)
        
        # 状态
        self.status = QLabel('💡 点击"盘前扫描"开始预测分析')
        self.status.setStyleSheet("color: #666; font-size: 14px;")
        layout.addWidget(self.status)
        
        # 预测摘要
        self.summary_box = QGroupBox('🔮 核心预测摘要')
        self.summary_box.setStyleSheet("""
            QGroupBox {
                font-weight: bold; border: 2px solid #ffd700; border-radius: 8px;
                background: #fffef0; padding-top: 15px;
            }
            QGroupBox::title { color: #ff6b00; }
        """)
        sum_layout = QVBoxLayout(self.summary_box)
        self.summary_text = QTextBrowser()
        self.summary_text.setMaximumHeight(140)
        self.summary_text.setStyleSheet("background: white; border: 1px solid #e0e0e0; padding: 8px;")
        self.summary_text.setHtml("<p style='color: #999; text-align: center;'>等待扫描...</p>")
        sum_layout.addWidget(self.summary_text)
        self.summary_box.setVisible(False)
        layout.addWidget(self.summary_box)
        
        # 三个表格
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        # 主线
        ml_grp = self._grp('🎯 预测主线 Top 5', '#1a73e8')
        self.ml_table = QTableWidget(0, 7)
        self.ml_table.setHorizontalHeaderLabels(['排名', '行业', '预测分', '状态', '天数', '成分', '强势'])
        self._tbl(self.ml_table)
        ml_grp.layout().addWidget(self.ml_table)
        splitter.addWidget(ml_grp)
        
        # 龙头
        ld_grp = self._grp('👑 龙头股池', '#34a853')
        self.ld_table = QTableWidget(0, 8)
        self.ld_table.setHorizontalHeaderLabels(['代码', '名称', '类型', '主线', '评分', '技术', '资金', '流入'])
        self._tbl(self.ld_table)
        ld_grp.layout().addWidget(self.ld_table)
        splitter.addWidget(ld_grp)
        
        # 超跌
        os_grp = self._grp('🎣 超跌候选', '#ea4335')
        self.os_table = QTableWidget(0, 7)
        self.os_table.setHorizontalHeaderLabels(['代码', '名称', '超跌系数', '回撤%', '背离', '支撑', '价格'])
        self._tbl(self.os_table)
        os_grp.layout().addWidget(self.os_table)
        splitter.addWidget(os_grp)
        
        splitter.setSizes([180, 180, 130])
        layout.addWidget(splitter)
    
    def _grp(self, title, color):
        grp = QGroupBox(title)
        grp.setStyleSheet(f"QGroupBox {{font-weight: bold; border: 2px solid #e3f2fd; border-radius: 8px; padding-top: 15px;}} QGroupBox::title {{color: {color};}}")
        grp.setLayout(QVBoxLayout())
        return grp
    
    def _tbl(self, tbl):
        tbl.setStyleSheet("QTableWidget {background: white; border: 1px solid #e0e0e0;} QHeaderView::section {background: #f5f5f5; font-weight: bold;}")
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)
    
    def run_scan(self):
        self.scan_btn.setEnabled(False)
        self.progress.setVisible(True)
        self.progress.setValue(0)
        
        self.thread = QuantMainlineThread(self.analyzer)
        self.thread.progress.connect(self._upd)
        self.thread.finished.connect(self._fin)
        self.thread.error.connect(self._err)
        self.thread.start()
    
    def _upd(self, val, msg):
        self.progress.setValue(val)
        self.status.setText(msg)
    
    def _fin(self, res):
        self.results = res
        
        if 'summary' in res and res.get('mode') == 'predictive':
            self._upd_sum(res['summary'])
            self.summary_box.setVisible(True)
        
        self._upd_ml(res['mainlines'])
        self._upd_ld(res['leaders'])
        self._upd_os(res['oversold'])
        
        self.scan_btn.setEnabled(True)
        self.export_btn.setEnabled(True)
        self.progress.setVisible(False)
        self.status.setText(f"✅ 完成 - {res['timestamp']}")
        
        QMessageBox.information(self, '完成', f"✅ 预测分析完成！\n\n主线: {len(res['mainlines'])}\n龙头: {len(res['leaders'])}\n超跌: {len(res['oversold'])}")
    
    def _err(self, msg):
        self.scan_btn.setEnabled(True)
        self.progress.setVisible(False)
        self.status.setText(f"❌ {msg}")
        QMessageBox.critical(self, '错误', msg)
    
    def _upd_sum(self, s):
        c = {'高': '#4caf50', '中': '#ff9800', '低': '#9e9e9e'}.get(s.get('top_mainline_confidence', '低'), '#999')
        html = f"""
        <div style='font-family: Arial;'>
            <div style='background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 10px; border-radius: 6px; margin-bottom: 6px;'>
                <h4 style='margin: 0;'>🎯 {s.get('top_mainline', 'N/A')}</h4>
                <p style='margin: 3px 0 0 0; font-size: 12px;'>
                    强度: {s.get('top_mainline_score', 0):.1f} | 
                    置信: <span style='background: {c}; padding: 2px 5px; border-radius: 3px;'>{s.get('top_mainline_confidence', 'N/A')}</span>
                </p>
            </div>
            <div style='background: #fff3cd; padding: 8px; border-radius: 4px; font-size: 11px; border-left: 3px solid #ffc107;'>
                <strong>💡 建议:</strong> {s.get('action_recommendation', 'N/A')[:60]}...<br>
                <strong>仓位:</strong> {s.get('position_sizing', 'N/A')}
            </div>
        </div>
        """
        self.summary_text.setHtml(html)
    
    def _upd_ml(self, data):
        self.ml_table.setRowCount(len(data))
        for i, d in enumerate(data):
            self.ml_table.setItem(i, 0, self._it(str(d['rank'])))
            self.ml_table.setItem(i, 1, self._it(d['industry']))
            self.ml_table.setItem(i, 2, self._it(f"{d['score']:.1f}"))
            self.ml_table.setItem(i, 3, self._it(d['status']))
            self.ml_table.setItem(i, 4, self._it(f"{d['days']}"))
            self.ml_table.setItem(i, 5, self._it(str(d['stocks'])))
            self.ml_table.setItem(i, 6, self._it(str(d['strong'])))
    
    def _upd_ld(self, data):
        self.ld_table.setRowCount(len(data))
        for i, d in enumerate(data):
            self.ld_table.setItem(i, 0, self._it(d['code']))
            self.ld_table.setItem(i, 1, self._it(d['name']))
            self.ld_table.setItem(i, 2, self._it(d['type']))
            self.ld_table.setItem(i, 3, self._it(d['industry']))
            self.ld_table.setItem(i, 4, self._it(f"{d['score']:.1f}"))
            self.ld_table.setItem(i, 5, self._it(f"{d['tech']:.1f}"))
            self.ld_table.setItem(i, 6, self._it(f"{d['fund']:.1f}"))
            it = self._it(f"{d['flow']:.2f}亿")
            it.setForeground(QColor('#4caf50') if d['flow'] > 0 else QColor('#f44336'))
            self.ld_table.setItem(i, 7, it)
    
    def _upd_os(self, data):
        self.os_table.setRowCount(len(data))
        for i, d in enumerate(data):
            self.os_table.setItem(i, 0, self._it(d['code']))
            self.os_table.setItem(i, 1, self._it(d['name']))
            self.os_table.setItem(i, 2, self._it(f"{d['coefficient']:.2f}"))
            self.os_table.setItem(i, 3, self._it(f"{d['drawdown']:.1f}"))
            self.os_table.setItem(i, 4, self._it('✅' if d['divergence'] else '❌'))
            self.os_table.setItem(i, 5, self._it(f"¥{d['support']:.2f}"))
            self.os_table.setItem(i, 6, self._it(f"¥{d['price']:.2f}"))
    
    def _it(self, txt):
        it = QTableWidgetItem(str(txt))
        it.setFlags(it.flags() & ~Qt.ItemFlag.ItemIsEditable)
        it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        return it
    
    def export(self):
        """导出完整预测报告（包含三个模块）"""
        if not self.results:
            QMessageBox.warning(self, '警告', '没有数据可导出')
            return
        
        fn = f"量化主线预测报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        try:
            # 构建完整报告
            report = []
            
            # === 报告头部 ===
            report.append("# 🔮 量化主线共振狙击系统 - 预测报告")
            report.append("")
            report.append(f"**生成时间**: {self.results['timestamp']}")
            report.append(f"**分析模式**: {self.results.get('mode', 'predictive')}")
            report.append("")
            report.append("---")
            report.append("")
            
            # === 核心预测摘要 ===
            if 'summary' in self.results and self.results.get('mode') == 'predictive':
                s = self.results['summary']
                report.append("## 📊 核心预测摘要")
                report.append("")
                report.append(f"### 🎯 顶级主线")
                report.append(f"- **行业**: {s.get('top_mainline', 'N/A')}")
                report.append(f"- **预测强度**: {s.get('top_mainline_score', 0):.1f}")
                report.append(f"- **置信度**: {s.get('top_mainline_confidence', 'N/A')}")
                report.append("")
                report.append(f"### 💡 操作建议")
                report.append(f"{s.get('action_recommendation', 'N/A')}")
                report.append("")
                report.append(f"### 💰 仓位建议")
                report.append(f"{s.get('position_sizing', 'N/A')}")
                report.append("")
                report.append("---")
                report.append("")
            
            # === 1. 预测主线 Top 5 ===
            report.append("## 1️⃣ 预测主线 Top 5")
            report.append("")
            report.append("| 排名 | 行业 | 预测分 | 状态 | 持续天数 | 成分股 | 强势股 |")
            report.append("|:---:|:---|:---:|:---:|:---:|:---:|:---:|")
            
            for d in self.results['mainlines']:
                report.append(
                    f"| **{d['rank']}** | {d['industry']} | **{d['score']:.1f}** | "
                    f"{d['status']} | {d['days']}天 | {d['stocks']}只 | {d['strong']}只 |"
                )
            
            report.append("")
            report.append("### 📈 主线说明")
            report.append("- **预测分**: 综合5个领先指标计算（动量加速30% + 内部强度25% + 资金背离20% + 情绪极点15% + 趋势惯性10%）")
            report.append("- **状态**: 预测明日走势（预测强势/新兴信号/即将转弱/观察等待）")
            report.append("")
            report.append("---")
            report.append("")
            
            # === 2. 龙头股池 ===
            report.append("## 2️⃣ 龙头股池")
            report.append("")
            report.append("| 排名 | 代码 | 名称 | 类型 | 主线 | 综合分 | 技术分 | 资金分 | 3日流入 |")
            report.append("|:---:|:---|:---|:---:|:---|:---:|:---:|:---:|:---:|")
            
            for i, d in enumerate(self.results['leaders'], 1):
                flow_text = f"{d['flow']:.2f}亿" if d['flow'] >= 0 else f"**{d['flow']:.2f}亿**"
                report.append(
                    f"| {i} | `{d['code']}` | {d['name']} | {d['type']} | "
                    f"{d['industry']} | **{d['score']:.1f}** | {d['tech']:.1f} | "
                    f"{d['fund']:.1f} | {flow_text} |"
                )
            
            report.append("")
            report.append("### 👑 龙头分类")
            report.append("- **综合龙头**: 综合评分>88 且属于预测主线前3")
            report.append("- **资金龙头**: 资金分>85 且3日主力净流入>2亿")
            report.append("- **潜在龙头**: 具备龙头潜质，持续关注")
            report.append("")
            report.append("### 📊 评分权重")
            report.append("- 技术分权重: 40%")
            report.append("- 资金分权重: 35% (已提升)")
            report.append("- 情绪分权重: 25% (已提升)")
            report.append("- 主线加成: Top1=1.20x, Top2=1.15x, Top3=1.10x")
            report.append("")
            report.append("---")
            report.append("")
            
            # === 3. 超跌候选 ===
            report.append("## 3️⃣ 超跌候选")
            report.append("")
            
            if self.results['oversold']:
                report.append("| 排名 | 代码 | 名称 | 超跌系数 | 回撤% | 背离 | 支撑位 | 当前价 | 潜力 |")
                report.append("|:---:|:---|:---|:---:|:---:|:---:|:---:|:---:|:---:|")
                
                for i, d in enumerate(self.results['oversold'], 1):
                    divergence = "✅" if d['divergence'] else "❌"
                    report.append(
                        f"| {i} | `{d['code']}` | {d['name']} | **{d['coefficient']:.2f}** | "
                        f"{d['drawdown']:.1f}% | {divergence} | ¥{d['support']:.2f} | "
                        f"¥{d['price']:.2f} | - |"
                    )
                
                report.append("")
                report.append("### 🎣 超跌说明")
                report.append("- **超跌系数**: 综合回撤幅度和技术背离，≥0.4入选，≥0.6优质")
                report.append("- **主线加成**: 属于预测主线前3的股票，系数×加成倍数")
                report.append("- **技术背离**: 价格新低但成交量未新低，可能见底信号")
                report.append("- **支撑位**: MA20均线作为参考支撑")
            else:
                report.append("**暂无符合条件的超跌候选**")
                report.append("")
                report.append("筛选条件：超跌系数≥0.4，属于预测主线成分股")
            
            report.append("")
            report.append("---")
            report.append("")
            
            # === 免责声明 ===
            report.append("## ⚠️ 风险提示")
            report.append("")
            report.append("1. 本报告由量化系统自动生成，仅供参考，不构成投资建议")
            report.append("2. 预测结果基于历史数据统计，市场存在不确定性")
            report.append("3. 股市有风险，投资需谨慎，请根据自身风险承受能力决策")
            report.append("4. 建议结合基本面分析、市场环境综合判断")
            report.append("")
            report.append("---")
            report.append("")
            report.append(f"*报告生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}*")
            report.append("")
            report.append("*由量化主线共振狙击系统自动生成 v2.0*")
            
            # 写入文件
            content = "\n".join(report)
            with open(fn, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # 统计信息
            stats = (
                f"✅ 导出成功！\n\n"
                f"📄 文件: {fn}\n"
                f"📊 主线: {len(self.results['mainlines'])} 个\n"
                f"👑 龙头: {len(self.results['leaders'])} 只\n"
                f"🎣 超跌: {len(self.results['oversold'])} 只"
            )
            
            QMessageBox.information(self, '导出成功', stats)
            
        except Exception as e:
            QMessageBox.critical(self, '导出失败', f'错误: {str(e)}\n\n请检查文件权限')
            import traceback
            traceback.print_exc()
