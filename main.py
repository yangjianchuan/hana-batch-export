import sys
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                           QTextEdit, QProgressBar, QFileDialog, QComboBox,
                           QSpinBox, QGroupBox, QCheckBox, QMessageBox,
                           QGridLayout, QListWidget)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from utils import ExcelExporter

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
            exporter = ExcelExporter(self.sql_query, self.output_file, self.page_size)
            cursor = exporter.connect()
            total = exporter.get_total_records(cursor)
            exporter.init_excel_writer()
            
            while exporter.current_offset < total:
                exporter.export_page(cursor)
                self.progress_signal.emit(min(exporter.current_offset + exporter.page_size, total), total)
            
            exporter.close()
            self.finished_signal.emit(True, f"成功导出到: {self.output_file}")
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

        # 创建主窗口部件和布局
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        

        # SQL配置组
        sql_group = QGroupBox("SQL配置")
        sql_layout = QVBoxLayout()
        
        # 添加SQL输入方式选择
        input_mode_layout = QHBoxLayout()
        self.sql_mode_combo = QComboBox()
        self.sql_mode_combo.addItems(["上传SQL文件", "直接输入SQL"])
        self.sql_mode_combo.currentTextChanged.connect(self.switchSqlMode)
        input_mode_layout.addWidget(QLabel("输入方式:"))
        input_mode_layout.addWidget(self.sql_mode_combo)
        sql_layout.addLayout(input_mode_layout)
        
        # 创建两个输入界面容器
        self.file_widget = QWidget()
        self.input_widget = QWidget()
        
        # 文件选择界面
        file_layout = QVBoxLayout(self.file_widget)
        file_btn_layout = QHBoxLayout()
        select_file_btn = QPushButton("选择SQL文件")
        select_file_btn.clicked.connect(self.selectSQLFile)
        file_btn_layout.addWidget(select_file_btn)
        file_layout.addLayout(file_btn_layout)
        
        # SQL文件列表
        self.sql_files_list = QListWidget()
        self.sql_files_list.setMaximumHeight(100)
        self.sql_files_list.itemClicked.connect(self.preview_sql_file)
        file_layout.addWidget(self.sql_files_list)
        
        # SQL预览
        self.sql_preview = QTextEdit()
        self.sql_preview.setPlaceholderText("SQL预览(点击列表中的文件查看内容)...")
        self.sql_preview.setReadOnly(True)
        file_layout.addWidget(self.sql_preview)
        
        # SQL直接输入界面
        input_layout = QVBoxLayout(self.input_widget)
        self.sql_input = QTextEdit()
        self.sql_input.setPlaceholderText("在此输入SQL语句...")
        input_layout.addWidget(self.sql_input)
        
        # 默认显示文件上传界面
        sql_layout.addWidget(self.file_widget)
        sql_layout.addWidget(self.input_widget)
        self.input_widget.hide()
        
        sql_group.setLayout(sql_layout)
        layout.addWidget(sql_group)

        # 导出配置组
        export_group = QGroupBox("导出配置")
        export_layout = QHBoxLayout()
        
        # 分页大小设置
        page_size_layout = QHBoxLayout()
        page_size_layout.addWidget(QLabel("分页大小:"))
        self.page_size_input = QSpinBox()
        self.page_size_input.setRange(100, 10000)
        self.page_size_input.setValue(2000)
        self.page_size_input.setSingleStep(100)
        page_size_layout.addWidget(self.page_size_input)
        
        export_layout.addLayout(page_size_layout)
        
        # 导出按钮
        self.export_btn = QPushButton("导出到Excel")
        self.export_btn.clicked.connect(self.startExport)
        export_layout.addWidget(self.export_btn)
        
        export_group.setLayout(export_layout)
        layout.addWidget(export_group)

        # 进度条
        self.progress = QProgressBar()
        layout.addWidget(self.progress)

        # 状态标签
        self.status_label = QLabel()
        layout.addWidget(self.status_label)



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
            self.export_btn.setEnabled(True)
            QMessageBox.information(self, "完成", "所有文件导出完成！")
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
        self.export_btn.setEnabled(False)
        self.status_label.setText(f"正在导出 ({self.current_export_index + 1}/{len(self.sql_files)}): {os.path.basename(sql_file)}")
        self.progress.setValue(0)
        
        # 启动线程
        self.export_thread.start()

    def updateProgress(self, current, total):
        """更新进度条"""
        progress = int((current / total) * 100)
        self.progress.setValue(progress)
        file_name = os.path.basename(self.sql_files[self.current_export_index])
        self.status_label.setText(f"正在导出 {file_name}: {current}/{total} ({progress}%)")

    def exportFinished(self, success, message):
        """单个文件导出完成处理"""
        if success:
            self.status_label.setText(message)
            self.current_export_index += 1
            
            # 如果全部导出完成，打开输出文件夹
            if self.current_export_index >= len(self.sql_files):
                os.startfile(self.output_dir)  # 在Windows上打开文件夹
            
            # 清理临时SQL文件
            if self.sql_mode_combo.currentText() == "直接输入SQL":
                try:
                    os.remove(self.sql_files[0])
                except:
                    pass
                    
            self.export_next_file()
        else:
            self.export_btn.setEnabled(True)
            self.status_label.setText(message)
            QMessageBox.critical(self, "错误", message)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
