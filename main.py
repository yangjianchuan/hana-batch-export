import sys
import os
import time
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QPushButton, 
                           QTextEdit, QProgressBar, QFileDialog, QComboBox,
                           QSpinBox, QGroupBox, QMessageBox,
                           QListWidget, QSplitter)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QTextCharFormat, QSyntaxHighlighter, QColor, QShortcut, QKeySequence
import pandas as pd
from pygments import lex
from pygments.lexers.sql import SqlLexer
from pygments.token import Token
from utils import ExcelExporter, StreamExporter

class SqlHighlighter(QSyntaxHighlighter):
    """SQL语法高亮类"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_formats()
        
    def setup_formats(self):
        """设置不同token类型的格式"""
        # 预定义格式对象
        self.formats = {
            Token.Keyword: self.create_format("blue"),
            Token.Operator: self.create_format("red"),
            Token.Literal.String: self.create_format("green"),
            Token.Comment: self.create_format("gray"),
            Token.Name.Builtin: self.create_format("purple"),
            Token.Punctuation: self.create_format("brown")
        }
        
    def create_format(self, color):
        """创建并返回格式对象"""
        format = QTextCharFormat()
        format.setForeground(QColor(color))
        return format

    def highlightBlock(self, text):
        """实现高亮逻辑"""
        # 使用pygments进行语法分析
        for token, content in lex(text, SqlLexer()):
            format = self.formats.get(token, None) or self.formats.get(token.parent, None)
            if format is not None:
                self.setFormat(text.find(content), len(content), format)

class ExportThread(QThread):
    """导出处理线程"""
    progress_signal = pyqtSignal(int, int)  # 当前进度, 总数
    finished_signal = pyqtSignal(bool, str)  # 是否成功, 消息

    def __init__(self, sql_query, output_file, page_size):
        super().__init__()
        self.sql_query = sql_query
        self.output_file = output_file
        self.page_size = page_size

    def run(self):
        try:
            # 继承ExcelExporter并添加进度通知功能
            class UIExcelExporter(ExcelExporter):
                def __init__(self, sql_query, output_file, page_size=None, progress_signal=None):
                    super().__init__(sql_query, output_file, page_size)
                    self.progress_signal = progress_signal
                    self.last_update_time = 0
                    self._original_sql = sql_query
                    
                def export_page(self, cursor):
                    # 调用父类的export_page前记录原始SQL
                    if not hasattr(self, '_original_sql'):
                        self._original_sql = self.sql_query
                    
                    # 限制进度更新频率
                    current_time = time.time()
                    update_progress = not hasattr(self, 'last_update_time') or current_time - self.last_update_time >= 1
                    
                    # 调用父类的export_page，这会触发_add_order_by
                    super().export_page(cursor)
                    
                    # 检查SQL是否被修改
                    if hasattr(self, '_ordered_query') and self._ordered_query != self._original_sql:
                        self.modified_sql = self._ordered_query
                    
                    # 更新进度
                    if update_progress:
                        processed = min(self.current_offset, self.total_records)
                        if self.progress_signal:
                            self.progress_signal.emit(processed, self.total_records)
                        self.last_update_time = current_time
                        
            exporter = UIExcelExporter(self.sql_query, self.output_file, self.page_size, self.progress_signal)
            # 先连接数据库并初始化
            cursor = exporter.connect()
            total = exporter.get_total_records(cursor)
            exporter.init_excel_writer()
            
            # 导出数据
            while exporter.current_offset < total:
                exporter.export_page(cursor)
            
            exporter.close()
            
            # 检查SQL是否被修改
            if hasattr(exporter, 'modified_sql'):
                self.finished_signal.emit(True, f"SQL语句已自动添加ORDER BY子句:\n{exporter.modified_sql}\n\n成功导出到: {os.path.abspath(self.output_file)}")
            else:
                self.finished_signal.emit(True, f"成功导出到: {os.path.abspath(self.output_file)}")
        except Exception as e:
            self.finished_signal.emit(False, f"导出失败: {str(e)}")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        
    def initUI(self):
        """初始化UI界面"""
        self.setWindowTitle('HANA数据导出工具')
        self.setGeometry(100, 100, 800, 600)

        # 创建主窗口部件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        
        # 创建QSplitter来分隔主内容区和日志区域
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(self.splitter)
        
        # 创建上部内容区域的容器
        content_widget = QWidget()
        self.content_layout = QVBoxLayout(content_widget)
        self.splitter.addWidget(content_widget)
        
        # 创建下部日志区域
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(150)
        self.log_text.setPlaceholderText("日志输出区域...")
        self.splitter.addWidget(self.log_text)
        
        # 调整分割比例
        self.splitter.setStretchFactor(0, 7)  # 内容区域占比
        self.splitter.setStretchFactor(1, 3)  # 日志区域占比
        
        # 先显示基本框架
        self.initBasicUI(self.content_layout)
        
        # 延迟加载其他组件
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(100, self.initRemainingUI)
        
    def initBasicUI(self, layout):
        """初始化基本UI"""
        # 创建进度条和状态标签
        self.progress = QProgressBar()
        layout.addWidget(self.progress)
        self.status_label = QLabel()
        layout.addWidget(self.status_label)
        
    def initRemainingUI(self):
        """延迟初始化剩余UI组件"""
        layout = self.content_layout

        # SQL配置组
        self.sql_group = QGroupBox("SQL配置")
        self.sql_layout = QVBoxLayout()
        
        # 添加SQL输入方式选择
        self.input_mode_layout = QHBoxLayout()
        self.sql_mode_combo = QComboBox()
        self.sql_mode_combo.addItems(["上传SQL文件", "直接输入SQL"])
        self.sql_mode_combo.currentTextChanged.connect(self.switchSqlMode)
        self.input_mode_layout.addWidget(QLabel("输入方式:"))
        self.input_mode_layout.addWidget(self.sql_mode_combo)
        self.sql_layout.addLayout(self.input_mode_layout)
        
        # 创建两个输入界面容器
        self.file_widget = QWidget()
        self.input_widget = QWidget()
        
        # 文件选择界面
        self.file_layout = QVBoxLayout(self.file_widget)
        self.file_btn_layout = QHBoxLayout()
        self.select_file_btn = QPushButton("选择SQL文件")
        self.select_file_btn.clicked.connect(self.selectSQLFile)
        self.file_btn_layout.addWidget(self.select_file_btn)
        self.file_layout.addLayout(self.file_btn_layout)
        
        # SQL文件列表
        self.sql_files_list = QListWidget()
        self.sql_files_list.setMaximumHeight(100)
        self.sql_files_list.itemClicked.connect(self.preview_sql_file)
        self.file_layout.addWidget(self.sql_files_list)
        
        # SQL预览
        self.sql_preview = QTextEdit()
        self.sql_preview.setPlaceholderText("SQL预览(点击列表中的文件查看内容)...")
        self.sql_preview.setReadOnly(True)
        self.sql_preview_highlighter = SqlHighlighter(self.sql_preview.document())
        self.file_layout.addWidget(self.sql_preview)
        
        # SQL直接输入界面
        self.input_layout = QVBoxLayout(self.input_widget)
        self.sql_input = QTextEdit()
        self.sql_input.setPlaceholderText("在此输入SQL语句...")
        self.sql_input_highlighter = SqlHighlighter(self.sql_input.document())
        self.input_layout.addWidget(self.sql_input)
        
        # 默认显示文件上传界面
        self.sql_layout.addWidget(self.file_widget)
        self.sql_layout.addWidget(self.input_widget)
        self.input_widget.hide()
        
        self.sql_group.setLayout(self.sql_layout)
        layout.addWidget(self.sql_group)

        # 导出配置组
        self.export_group = QGroupBox("导出配置")
        self.export_layout = QHBoxLayout()
        
        # 分页大小设置
        self.page_size_layout = QHBoxLayout()
        self.page_size_layout.addWidget(QLabel("分页大小:"))
        self.page_size_input = QSpinBox()
        self.page_size_input.setRange(100, 10000)
        self.page_size_input.setValue(int(os.getenv('PAGE_SIZE', 2000)))
        self.page_size_input.setSingleStep(100)
        self.page_size_layout.addWidget(self.page_size_input)
        
        self.export_layout.addLayout(self.page_size_layout)
        
        # 导出按钮
        self.stream_export_btn = QPushButton("流式导出(F12)")
        self.stream_export_btn.clicked.connect(self.stream_export)
        self.export_layout.addWidget(self.stream_export_btn)
        
        self.page_export_btn = QPushButton("分页导出(Ctrl+F12)")  
        self.page_export_btn.clicked.connect(self.startExport)
        self.export_layout.addWidget(self.page_export_btn)
        
        # 绑定快捷键
        QShortcut(QKeySequence("F12"), self).activated.connect(self.stream_export)
        QShortcut(QKeySequence("Ctrl+F12"), self).activated.connect(self.startExport)
        
        self.export_group.setLayout(self.export_layout)
        layout.addWidget(self.export_group)



    def switchSqlMode(self, mode):
        """切换SQL输入模式"""
        if mode == "上传SQL文件":
            self.file_widget.show()
            self.input_widget.hide()
        else:
            self.file_widget.hide()
            self.input_widget.show()

    def preview_sql_file(self, item):
        """预览选中的SQL文件"""
        try:
            with open(item.text(), 'r', encoding='utf-8') as f:
                self.sql_preview.setText(f.read())
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法读取SQL文件: {str(e)}")

    def selectSQLFile(self):
        """选择多个SQL文件"""
        file_names, _ = QFileDialog.getOpenFileNames(
            self,
            "选择SQL文件",
            "",
            "SQL文件 (*.sql)"
        )
        
        if file_names:
            # 更新文件列表显示
            self.sql_files = [os.path.abspath(f) for f in file_names]  # 确保存储绝对路径
            print("Selected files:", self.sql_files)  # 调试信息
            self.sql_files_list.clear()
            for file_name in self.sql_files:
                self.sql_files_list.addItem(file_name)
            
            # 自动选中并预览第一个文件
            if self.sql_files_list.count() > 0:
                self.sql_files_list.setCurrentRow(0)
                self.preview_sql_file(self.sql_files_list.item(0))

    def startExport(self):
        """开始批量导出数据"""
        output_dir = ""
        
        if self.sql_mode_combo.currentText() == "上传SQL文件":
            if not hasattr(self, 'sql_files') or not self.sql_files:
                QMessageBox.warning(self, "警告", "请先选择SQL文件！")
                return
            print("Exporting files:", self.sql_files)  # 添加调试信息
            output_dir = os.path.dirname(os.path.abspath(self.sql_files[0]))
        else:
            # 直接输入SQL模式
            sql_text = self.sql_input.toPlainText().strip()
            if not sql_text:
                QMessageBox.warning(self, "警告", "请输入SQL语句！")
                return
                
            # 为直接输入的SQL创建临时文件
            temp_file = os.path.join(os.getcwd(), "temp_sql.sql")
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(sql_text)
            self.sql_files = [temp_file]
            output_dir = os.getcwd()
            
        # 开始批量导出
        self.current_export_index = 0
        self.output_dir = output_dir
        self.export_next_file()
    
    def export_next_file(self):
        """导出下一个文件"""
        if self.current_export_index >= len(self.sql_files):
            self.stream_export_btn.setEnabled(True)
            self.page_export_btn.setEnabled(True)
            self.progress.setValue(100)  # 确保进度条显示100%
            return
        
        sql_file = self.sql_files[self.current_export_index]
        output_file = os.path.splitext(sql_file)[0] + '.xlsx'
        
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql = f.read()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法读取SQL文件 {sql_file}: {str(e)}")
            self.current_export_index += 1
            self.export_next_file()
            return
        
        # 创建并启动导出线程
        self.export_thread = ExportThread(
            sql,
            output_file,
            self.page_size_input.value()
        )
        
        # 连接信号
        self.export_thread.progress_signal.connect(self.updateProgress)
        self.export_thread.finished_signal.connect(self.exportFinished)
        
        # 禁用导出按钮
        self.stream_export_btn.setEnabled(False)
        self.page_export_btn.setEnabled(False)
        self.status_label.setText(f"正在导出 ({self.current_export_index + 1}/{len(self.sql_files)}): {os.path.basename(sql_file)}")
        self.progress.setValue(0)
        
        # 启动线程
        self.export_thread.start()

    def log_message(self, message):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        
    def updateProgress(self, current, total):
        """更新进度条"""
        progress = int((current / total) * 100)
        self.progress.setValue(progress)
        file_name = os.path.basename(self.sql_files[self.current_export_index])
        status_msg = f"正在导出 {file_name}: {current}/{total} ({progress}%)"
        self.status_label.setText(status_msg)
        self.log_message(status_msg)

    def exportFinished(self, success, message):
        """单个文件导出完成处理"""
        if success:
            self.log_message(message)
            self.status_label.setText("")  # 清空状态标签
            self.current_export_index += 1
            
            # 如果全部导出完成，设置完成状态并打开输出文件夹
            if self.current_export_index >= len(self.sql_files):
                self.progress.setValue(100)  # 确保进度条显示100%
                self.progress.hide()  # 隐藏进度条
                os.startfile(self.output_dir)  # 在Windows上打开文件夹
            
            # 清理临时SQL文件
            if self.sql_mode_combo.currentText() == "直接输入SQL":
                try:
                    os.remove(self.sql_files[0])
                except:
                    pass
                    
            self.export_next_file()
        else:
            self.stream_export_btn.setEnabled(True)
            self.page_export_btn.setEnabled(True)
            self.log_message(message)
            self.status_label.setText("")  # 清空状态标签
            QMessageBox.critical(self, "错误", message)

    def stream_export(self):
        """流式导出到Excel"""
        if self.sql_mode_combo.currentText() == "上传SQL文件":
            if not hasattr(self, 'sql_files') or not self.sql_files:
                self.log_message("警告：请先选择SQL文件！")
                QMessageBox.warning(self, "警告", "请先选择SQL文件！")
                return
            output_dir = os.path.dirname(os.path.abspath(self.sql_files[0]))
        else:
            # 直接输入SQL模式
            sql_text = self.sql_input.toPlainText().strip()
            if not sql_text:
                self.log_message("警告：请输入SQL语句！")
                QMessageBox.warning(self, "警告", "请输入SQL语句！")
                return
                
            # 为直接输入的SQL创建临时文件
            temp_file = os.path.join(os.getcwd(), "temp_sql.sql")
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(sql_text)
            self.sql_files = [temp_file]
            output_dir = os.getcwd()
            
        # 开始批量导出
        self.current_export_index = 0
        self.output_dir = output_dir
        self.stream_export_next_file()
        
    def stream_export_next_file(self):
        """流式导出下一个文件"""
        if self.current_export_index >= len(self.sql_files):
            self.stream_export_btn.setEnabled(True)
            self.page_export_btn.setEnabled(True)
            self.progress.setValue(100)  # 确保进度条显示100%
            return
            
        sql_file = self.sql_files[self.current_export_index]
        output_file = os.path.splitext(sql_file)[0] + '_stream.xlsx'
        
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql = f.read()
        except Exception as e:
            error_msg = f"错误：无法读取SQL文件 {sql_file}: {str(e)}"
            self.log_message(error_msg)
            QMessageBox.critical(self, "错误", error_msg)
            self.current_export_index += 1
            self.stream_export_next_file()
            return
            
        # 创建并启动流式导出线程
        self.stream_export_thread = StreamExportThread(
            sql,
            output_file
        )
        
        # 连接信号
        self.stream_export_thread.progress_signal.connect(self.updateProgress)
        self.stream_export_thread.finished_signal.connect(self.streamExportFinished)
        
        # 禁用导出按钮
        self.stream_export_btn.setEnabled(False)
        self.page_export_btn.setEnabled(False)
        self.status_label.setText(f"正在流式导出 ({self.current_export_index + 1}/{len(self.sql_files)}): {os.path.basename(sql_file)}")
        self.progress.setValue(0)
        
        # 启动线程
        self.stream_export_thread.start()
        
    def streamExportFinished(self, success, message):
        """单个文件流式导出完成处理"""
        if success:
            self.log_message(message)
            self.status_label.setText("")  # 清空状态标签
            self.current_export_index += 1
            
            # 如果全部导出完成，设置完成状态并打开输出文件夹
            if self.current_export_index >= len(self.sql_files):
                self.progress.setValue(100)  # 确保进度条显示100%
                self.progress.hide()  # 隐藏进度条
                os.startfile(self.output_dir)  # 在Windows上打开文件夹
            
            # 清理临时SQL文件
            if self.sql_mode_combo.currentText() == "直接输入SQL":
                try:
                    os.remove(self.sql_files[0])
                except:
                    pass
                    
            self.stream_export_next_file()
        else:
            self.stream_export_btn.setEnabled(True)
            self.page_export_btn.setEnabled(True)
            self.log_message(message)
            self.status_label.setText("")  # 清空状态标签
            QMessageBox.critical(self, "错误", message)

class StreamExportThread(QThread):
    """流式导出处理线程"""
    progress_signal = pyqtSignal(int, int)  # 当前进度, 总数
    finished_signal = pyqtSignal(bool, str)  # 是否成功, 消息
    
    def __init__(self, sql_query, output_file):
        super().__init__()
        self.sql_query = sql_query
        self.output_file = output_file
        
    def run(self):
        try:
            # 继承StreamExporter添加进度通知功能
            class UIStreamExporter(StreamExporter):
                def __init__(self, sql_query, output_file, progress_signal=None):
                    super().__init__(sql_query, output_file)
                    self.progress_signal = progress_signal
                    
                def export(self):
                    try:
                        cursor = self.utils.get_cursor()
                        self.get_total_records(cursor)
                        self.init_excel_writer()

                        cursor.execute(self.sql_query)
                        columns = [desc[0] for desc in cursor.description]
                        
                        # 写入表头等初始化操作...
                        df = pd.DataFrame(columns=columns)
                        df.to_excel(self.writer, sheet_name='Data', index=False, startrow=0)
                        self.worksheet = self.writer.sheets['Data']
                        
                        for col_num, value in enumerate(columns):
                            self.worksheet.write(0, col_num, value, self.header_format)
                        
                        self.worksheet.set_column(0, len(columns) - 1, 20)
                        self.worksheet.freeze_panes(1, 0)
                        
                        row = 1
                        processed = 0
                        
                        while True:
                            results = cursor.fetchmany(self.chunk_size)
                            if not results:
                                break
                                
                            df = pd.DataFrame(results, columns=columns)
                            
                            for col in df.columns:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except (ValueError, TypeError):
                                    continue
                            
                            for r_idx, data_row in enumerate(df.values):
                                for c_idx, value in enumerate(data_row):
                                    if pd.isna(value) or (isinstance(value, float) and (value == float('inf') or value == float('-inf'))):
                                        value = None
                                    self.worksheet.write(row + r_idx, c_idx, value, self.body_format)
                            
                            row += len(results)
                            processed += len(results)
                            
                            # 发送进度信号
                            if self.progress_signal:
                                self.progress_signal.emit(processed, self.total_records)
                        
                        return True
                    finally:
                        if self.writer:
                            self.writer.close()
            
            # 使用自定义导出器
            exporter = UIStreamExporter(self.sql_query, self.output_file, self.progress_signal)
            exporter.utils.connect()
            
            # 执行导出
            exporter.export()
            
            # 发送成功消息，包含完整的输出路径
            self.finished_signal.emit(True, f"成功导出到: {os.path.abspath(self.output_file)}")
        except Exception as e:
            self.finished_signal.emit(False, f"导出失败: {str(e)}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
