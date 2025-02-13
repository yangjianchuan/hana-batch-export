import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import os
from datetime import datetime
from pygments import lex
from pygments.lexers.sql import SqlLexer
from pygments.token import Token
import time
import pandas as pd
import threading
import queue
from utils import HANAUtils

class HanaQueryAnalyzer:
    def __init__(self, root):
        self.root = root
        self.root.title("HANA 查询分析器")
        self.root.geometry("1200x800")
        
        # 存储每个标签页对应的文件路径
        self.tab_file_paths = {}
        
        # 从环境变量获取最大结果集大小，默认为100
        self.max_results = int(os.getenv('RESULT_SIZE', '100'))
        
        # 创建主框架
        main_frame = ttk.Frame(root)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建顶部按钮区域
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 创建按钮并绑定快捷键
        ttk.Button(button_frame, text="新增查询窗口 (Ctrl+N)", command=self.add_tab).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="清空查询语句 (Ctrl+D)", command=self.clear_sql).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="保存 (Ctrl+S)", command=self.save_sql).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="加载 (Ctrl+O)", command=self.load_sql).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="执行选中 (Ctrl+F8)", command=self.execute_selected).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="执行全部 (F8)", command=self.execute_all).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="流式导出 (F12)", command=self.stream_export_results).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="分页导出 (Ctrl+F12)", command=self.export_results).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="直接导出 (Shift+F12)", command=self.export_all).pack(side=tk.LEFT, padx=2)
        self.stop_button = ttk.Button(button_frame, text="终止查询 (Esc)", command=self.stop_query, state="disabled")
        self.stop_button.pack(side=tk.LEFT, padx=2)
        
        # 添加数据库连接/断开按钮
        self.connect_button = ttk.Button(button_frame, text="连接数据库", command=self.connect_disconnect_db)
        self.connect_button.pack(side=tk.LEFT, padx=2)

        # 绑定快捷键（同时支持大小写）
        self.root.bind_all("<Escape>", lambda e: self.stop_query())
        self.root.bind_all("<Control-n>", lambda e: self.add_tab())
        self.root.bind_all("<Control-N>", lambda e: self.add_tab())
        self.root.bind_all("<Control-d>", lambda e: self.clear_sql())
        self.root.bind_all("<Control-D>", lambda e: self.clear_sql())
        self.root.bind_all("<Control-s>", lambda e: self.save_sql())
        self.root.bind_all("<Control-S>", lambda e: self.save_sql())
        self.root.bind_all("<Control-o>", lambda e: self.load_sql())
        self.root.bind_all("<Control-O>", lambda e: self.load_sql())
        self.root.bind_all("<Control-F8>", lambda e: self.execute_selected())
        self.root.bind_all("<F8>", lambda e: self.execute_all())
        self.root.bind_all("<F12>", lambda e: self.stream_export_results())
        self.root.bind_all("<Control-F12>", lambda e: self.export_results())
        self.root.bind_all("<Shift-F12>", lambda e: self.export_all())
        self.root.bind_all("<Control-w>", lambda e: self.close_tab())
        self.root.bind_all("<Control-W>", lambda e: self.close_tab())
        
        # 注册窗口关闭事件处理
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 创建多标签页
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # 添加初始标签页
        self.add_tab()
        
        # 添加关闭按钮
        ttk.Button(button_frame, text="关闭标签页 (Ctrl+W)", command=self.close_tab).pack(side=tk.LEFT, padx=2)
        
        # 初始化HANA数据库工具（启动时自动连接）
        self.hana_utils = HANAUtils()
        self.connected = False  # 标记是否已连接
        
        # 尝试自动连接数据库
        try:
            self.hana_utils.connect()
            if self.check_connection_status():
                self.connected = True
                self.log_message("数据库连接成功")
            else:
                self.log_message("数据库连接失败：无法验证连接")
        except Exception as e:
            self.log_message(f"数据库自动连接失败: {str(e)}")
            
        # 更新连接按钮状态
        self.update_connection_button()
        
    def add_tab(self):
        if len(self.notebook.tabs()) >= 10:
            return
            
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text=f"Query {len(self.notebook.tabs()) + 1}")
        
        # SQL输入框
        sql_input = scrolledtext.ScrolledText(frame, wrap=tk.WORD)
        sql_input.pack(fill=tk.BOTH, expand=True)
        sql_input.bind('<KeyRelease>', self.highlight_sql)
        
        # 使用PanedWindow来分隔结果区域和日志区域
        paned = ttk.PanedWindow(frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # 结果展示区域
        result_frame = ttk.Frame(paned)
        paned.add(result_frame, weight=3)  # 结果区域占比更大
        
        # 创建Treeview组件和滚动条
        # 设置height参数让Treeview显示固定行数，避免挤压水平滚动条
        tree = ttk.Treeview(result_frame, show="headings", selectmode="extended", height=15)
        
        # 添加垂直滚动条
        vsb = ttk.Scrollbar(result_frame, orient="vertical", command=tree.yview)
        # 添加水平滚动条
        hsb = ttk.Scrollbar(result_frame, orient="horizontal", command=tree.xview)
        
        # 配置Treeview的滚动
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        # 设置列宽自动调整
        style = ttk.Style()
        style.configure('Treeview', rowheight=25)  # 调整行高
        # 禁用换行，确保水平滚动条可以工作
        style.configure('Treeview', wrap='none')
        
        # 使用grid布局管理器
        tree.grid(column=0, row=0, sticky='nsew')
        vsb.grid(column=1, row=0, sticky='ns')
        hsb.grid(column=0, row=1, sticky='ew')
        
        # 确保水平滚动条位置正确
        result_frame.grid_columnconfigure(0, weight=1)
        result_frame.grid_columnconfigure(1, weight=0)  # 垂直滚动条列不伸展
        result_frame.grid_rowconfigure(0, weight=1)
        result_frame.grid_rowconfigure(1, weight=0)  # 水平滚动条行不伸展
        
        # 配置grid权重，使Treeview能够自动扩展
        result_frame.grid_columnconfigure(0, weight=1)
        result_frame.grid_rowconfigure(0, weight=1)
        
        # 日志区域（在PanedWindow中）
        log_frame = ttk.Frame(paned)
        paned.add(log_frame, weight=1)  # 日志区域占比较小
        
        log_text = scrolledtext.ScrolledText(log_frame, height=6)
        log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 将组件存储为frame的属性
        frame.sql_input = sql_input
        frame.result_text = tree  # 将tree组件赋值给result_text
        frame.log_text = log_text
        
    def get_current_tab_widgets(self):
        """获取当前标签页的组件"""
        current_tab = self.notebook.nametowidget(self.notebook.select())
        if not current_tab:
            return None, None, None
        return (
            getattr(current_tab, 'sql_input', None),
            getattr(current_tab, 'result_text', None),
            getattr(current_tab, 'log_text', None)
        )
        
    def highlight_sql(self, event=None):
        # 获取当前标签页的SQL输入框
        sql_input, _, _ = self.get_current_tab_widgets()
        if not sql_input:
            return
            
        # 获取SQL文本
        sql_text = sql_input.get("1.0", tk.END)
        
        # 删除所有现有的标记
        for tag in sql_input.tag_names():
            sql_input.tag_remove(tag, "1.0", tk.END)
        
        # 使用pygments进行语法分析
        for token, content in lex(sql_text, SqlLexer()):
            if not content.strip():  # 跳过空白内容
                continue
                
            start_index = "1.0"
            while True:
                # 查找下一个匹配位置
                pos = sql_input.search(content, start_index, tk.END)
                if not pos:
                    break
                    
                # 计算结束位置
                end = f"{pos}+{len(content)}c"
                
                # 添加标记
                token_str = str(token)
                sql_input.tag_add(token_str, pos, end)
                sql_input.tag_config(token_str, foreground=self.get_token_color(token))
                
                # 更新起始位置
                start_index = end
                
    def get_token_color(self, token):
        # 定义不同token类型的颜色
        colors = {
            Token.Keyword: "blue",
            Token.Operator: "red",
            Token.Literal.String: "green",
            Token.Comment: "gray",
            Token.Name.Builtin: "purple",
            Token.Punctuation: "brown"
        }
        return colors.get(token, "black")
        
    def save_sql(self):
        # 获取当前标签页的SQL输入框
        sql_input, _, _ = self.get_current_tab_widgets()
        if not sql_input:
            return
        
        # 获取当前标签页ID
        current_tab = self.notebook.select()
        # 获取SQL文本
        sql_text = sql_input.get("1.0", tk.END)
        
        # 检查当前标签页是否有关联的文件路径
        file_path = self.tab_file_paths.get(current_tab)
        
        # 如果没有关联的文件路径，则打开文件保存对话框
        if not file_path:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".sql",
                filetypes=[("SQL Files", "*.sql"), ("All Files", "*.*")]
            )
        
        if file_path:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(sql_text)
                self.log_message(f"SQL脚本已保存到: {file_path}")
            except Exception as e:
                self.log_message(f"保存失败: {str(e)}")
        
    def load_sql(self):
        # 获取当前标签页的SQL输入框
        sql_input, _, _ = self.get_current_tab_widgets()
        if not sql_input:
            return
        
        # 获取当前标签页ID
        current_tab = self.notebook.select()
        
        # 打开文件选择对话框
        file_path = filedialog.askopenfilename(
            filetypes=[("SQL Files", "*.sql"), ("All Files", "*.*")]
        )
        
        if file_path:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    sql_text = f.read()
                    sql_input.delete("1.0", tk.END)
                    sql_input.insert("1.0", sql_text)
                    # 保存文件路径到当前标签页
                    self.tab_file_paths[current_tab] = file_path
                    # 更新标签页标题为文件名
                    self.notebook.tab(current_tab, text=os.path.basename(file_path))
                    # 加载后触发语法高亮
                    self.highlight_sql()
                self.log_message(f"已加载SQL脚本: {file_path}")
            except Exception as e:
                self.log_message(f"加载失败: {str(e)}")
        
    def execute_selected(self):
        # 获取当前标签页的组件
        sql_input, result_text, _ = self.get_current_tab_widgets()
        if not sql_input or not result_text:
            return
        
        # 获取选中的SQL文本
        try:
            selected_text = sql_input.get(tk.SEL_FIRST, tk.SEL_LAST)
            if not selected_text.strip():
                self.log_message("未选中任何SQL语句")
                return
                
            # 执行SQL并显示结果
            self.execute_sql(selected_text, result_text)
            
        except tk.TclError:
            self.log_message("请先选择要执行的SQL语句")
            
    def execute_all(self):
        # 获取当前标签页的组件
        sql_input, result_text, _ = self.get_current_tab_widgets()
        if not sql_input or not result_text:
            return
        
        # 获取所有SQL文本
        all_text = sql_input.get("1.0", tk.END)
        if not all_text.strip():
            self.log_message("SQL输入框为空")
            return
            
        # 执行SQL并显示结果
        self.execute_sql(all_text, result_text)
        
    def on_closing(self):
        """窗口关闭时的清理操作"""
        try:
            # 断开数据库连接
            self.hana_utils.disconnect()
        finally:
            self.root.destroy()
            
    def execute_sql(self, sql_text, result_text):
        # 清除之前的结果
        for item in result_text.get_children():
            result_text.delete(item)
        result_text["columns"] = []
        
        # 禁用执行按钮
        self.disable_execute_buttons()
        self.log_message("正在执行查询...")
        
        # 创建队列用于线程间通信
        self.result_queue = queue.Queue()
        
        # 创建并启动后台线程
        thread = threading.Thread(
            target=self._execute_sql_in_thread,
            args=(sql_text, result_text)
        )
        thread.daemon = True
        thread.start()
        
        # 启动定时器检查线程状态
        self.root.after(100, self._check_thread_status, result_text)
        
    def stop_query(self):
        """终止当前查询"""
        if hasattr(self, 'stop_requested'):
            self.stop_requested = True
            self.log_message("正在终止查询...")

    def _execute_sql_in_thread(self, sql_text, result_text):
        """在后台线程中执行SQL查询"""
        self.stop_requested = False
        try:
            # 记录开始时间
            start_time = time.time()
            
            # 检查并确保数据库连接
            if not self.check_connection_status():
                self.hana_utils.connect()
                if not self.check_connection_status():
                    self.result_queue.put(("error", "无法建立数据库连接"))
                    return
                self.update_connection_button()

            # 执行SQL查询
            try:
                cursor = self.hana_utils.get_cursor()
                cursor.execute(sql_text)
            except Exception as e:
                # 如果执行失败，再次检查连接状态
                if not self.check_connection_status():
                    self.update_connection_button()
                    self.result_queue.put(("error", "数据库连接已断开"))
                    return
                else:
                    raise  # 如果连接正常但执行出错，抛出原始异常
            
            # 检查是否请求终止
            if self.stop_requested:
                self.result_queue.put(("info", "查询已终止"))
                return
                
            results = cursor.fetchmany(self.max_results + 1)  # 多获取一条用于判断是否超出限制
            columns = [desc[0] for desc in cursor.description]
            
            if results:
                # 检查是否超出最大结果集限制
                if len(results) > self.max_results:
                    results = results[:self.max_results]  # 只保留前max_results条
                    self.result_queue.put(("warning", f"警告：结果集已被限制为前{self.max_results}条记录"))
                
                # 将结果转换为DataFrame以便格式化显示
                df = pd.DataFrame(results, columns=columns)
                
                # 将结果放入队列
                self.result_queue.put(("columns", columns))
                self.result_queue.put(("data", df))
                
                # 记录数
                self.result_queue.put(("info", f"共 {len(results)} 条记录"))
            else:
                self.result_queue.put(("info", "查询未返回结果"))
            
            duration = time.time() - start_time
            self.result_queue.put(("info", f"SQL执行成功，耗时: {duration:.2f}秒"))
            
        except Exception as e:
            # 记录错误信息
            duration = time.time() - start_time
            self.result_queue.put(("error", f"SQL执行失败: {str(e)}"))
        finally:
            # 标记任务完成
            self.result_queue.put(("done", None))
            
    def _check_thread_status(self, result_text):
        """检查后台线程状态并更新UI"""
        try:
            while not self.result_queue.empty():
                msg_type, content = self.result_queue.get_nowait()
                
                if msg_type == "columns":
                    # 设置列
                    result_text["columns"] = content
                    for col in content:
                        result_text.heading(col, text=col)
                        result_text.column(col, width=150, minwidth=100, stretch=False, anchor='center')
                elif msg_type == "data":
                    # 插入数据
                    for _, row in content.iterrows():
                        result_text.insert("", "end", values=tuple(row))
                elif msg_type in ["info", "warning", "error"]:
                    self.log_message(content)
                elif msg_type == "done":
                    # 启用执行按钮
                    self.enable_execute_buttons()
                    return
                    
            # 继续检查
            self.root.after(100, self._check_thread_status, result_text)
        except queue.Empty:
            # 继续检查
            self.root.after(100, self._check_thread_status, result_text)
            
    def disable_execute_buttons(self):
        """禁用所有执行相关按钮"""
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Button) and child["text"] in ["执行选中 (Ctrl+F8)", "执行全部 (F8)"]:
                child["state"] = "disabled"
        self.stop_button["state"] = "normal"
                
    def enable_execute_buttons(self):
        """启用所有执行相关按钮"""
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Button) and child["text"] in ["执行选中 (Ctrl+F8)", "执行全部 (F8)"]:
                child["state"] = "normal"
        self.stop_button["state"] = "disabled"
        
    def is_select_query(self, sql_text):
        """检查SQL语句是否以select开头(不区分大小写)"""
        sql_text = (sql_text or "").strip().lower()
        return sql_text.lstrip().startswith("select")

    def stream_export_results(self):
        # 获取当前标签页的SQL输入框和结果区域
        sql_input, result_text, _ = self.get_current_tab_widgets()
        if not sql_input or not result_text:
            return
        
        # 获取SQL文本
        sql_text = sql_input.get("1.0", tk.END).strip()
        if not sql_text:
            self.log_message("没有SQL语句可执行")
            return

        if not self.is_select_query(sql_text):
            self.log_message("非SELECT语句自动执行直接导出")
            self.export_all()
            return
            
        # 打开文件保存对话框
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
        )
        
        if file_path:
            # 禁用所有导出按钮
            for child in self.root.winfo_children():
                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                    child["state"] = "disabled"

            # 创建自定义StreamExporter子类用于日志输出
            from utils import StreamExporter
            
            # 创建队列用于线程间通信
            self.stream_queue = queue.Queue()

            def export_in_thread():
                try:
                    class UIStreamExporter(StreamExporter):
                        def __init__(self, sql_query, output_file, queue=None):
                            super().__init__(sql_query, output_file)
                            self.queue = queue
                            self.last_update_time = 0

                        def export(self):
                            try:
                                cursor = self.utils.get_cursor()
                                self.get_total_records(cursor)
                                self.init_excel_writer()

                                cursor.execute(self.sql_query)
                                columns = [desc[0] for desc in cursor.description]
                                
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
                                    
                                    # 每隔一秒更新一次进度
                                    current_time = time.time()
                                    if current_time - self.last_update_time >= 1:
                                        progress = processed/self.total_records
                                        if self.queue:
                                            self.queue.put(("progress", f"已导出 {processed}/{self.total_records} 条记录 ({progress:.1%})"))
                                        self.last_update_time = current_time
                                
                                return True
                                
                            except Exception as e:
                                if self.queue:
                                    self.queue.put(("error", f"导出失败: {str(e)}"))
                                raise
                            finally:
                                if self.writer:
                                    self.writer.close()

                    exporter = UIStreamExporter(sql_text, file_path, queue=self.stream_queue)
                    exporter.utils = self.hana_utils  # 使用已有的数据库连接
                    exporter.export()
                    self.stream_queue.put(("success", f"结果已导出到: {file_path}"))
                except Exception as e:
                    self.stream_queue.put(("error", f"导出失败: {str(e)}"))
                finally:
                    self.stream_queue.put(("done", None))

            def check_export_status():
                try:
                    while not self.stream_queue.empty():
                        msg_type, content = self.stream_queue.get_nowait()
                        
                        if msg_type in ["info", "progress", "success", "error"]:
                            self.log_message(content)
                        elif msg_type == "done":
                            # 启用所有导出按钮
                            for child in self.root.winfo_children():
                                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                                    child["state"] = "normal"
                            return
                            
                    # 继续检查
                    self.root.after(100, check_export_status)
                except queue.Empty:
                    # 继续检查
                    self.root.after(100, check_export_status)

            # 创建并启动后台线程
            thread = threading.Thread(target=export_in_thread)
            thread.daemon = True
            thread.start()

            # 开始检查导出状态
            self.root.after(100, check_export_status)

    def export_all(self):
        """直接导出所有数据"""
        # 获取当前标签页的SQL输入框和结果区域
        sql_input, result_text, _ = self.get_current_tab_widgets()
        if not sql_input or not result_text:
            return
        
        # 获取SQL文本
        sql_text = sql_input.get("1.0", tk.END).strip()
        if not sql_text:
            self.log_message("没有SQL语句可执行")
            return
            
        # 打开文件保存对话框
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
        )
        
        if file_path:
            # 禁用导出按钮
            for child in self.root.winfo_children():
                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                    child["state"] = "disabled"

            # 创建队列用于线程间通信
            self.export_queue = queue.Queue()

            def export_in_thread():
                try:
                    # 创建Excel导出器实例
                    from utils import ExcelExporter
                    exporter = ExcelExporter(sql_text, file_path)
                    exporter.utils = self.hana_utils  # 使用已有的数据库连接
                    exporter.export_all()
                    self.export_queue.put(("success", f"结果已直接导出到: {file_path}"))
                except Exception as e:
                    self.export_queue.put(("error", f"直接导出失败: {str(e)}"))
                finally:
                    self.export_queue.put(("done", None))

            def check_export_status():
                try:
                    while not self.export_queue.empty():
                        msg_type, content = self.export_queue.get_nowait()
                        
                        if msg_type in ["info", "progress", "success", "error"]:
                            self.log_message(content)
                        elif msg_type == "done":
                            # 启用所有导出按钮
                            for child in self.root.winfo_children():
                                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                                    child["state"] = "normal"
                            return
                            
                    # 继续检查
                    self.root.after(100, check_export_status)
                except queue.Empty:
                    # 继续检查
                    self.root.after(100, check_export_status)

            # 创建并启动后台线程
            thread = threading.Thread(target=export_in_thread)
            thread.daemon = True
            thread.start()

            # 开始检查导出状态
            self.root.after(100, check_export_status)

    def export_results(self):
        # 获取当前标签页的SQL输入框和结果区域
        sql_input, result_text, _ = self.get_current_tab_widgets()
        if not sql_input or not result_text:
            return
        
        # 获取SQL文本
        sql_text = sql_input.get("1.0", tk.END).strip()
        if not sql_text:
            self.log_message("没有SQL语句可执行")
            return

        if not self.is_select_query(sql_text):
            self.log_message("非SELECT语句自动执行直接导出")
            self.export_all()
            return
            
        # 打开文件保存对话框
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
        )
        
        if file_path:
            # 禁用导出按钮
            for child in self.root.winfo_children():
                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                    child["state"] = "disabled"
                    if child["text"] == "分页导出 (F12)":
                        export_button = child

            # 创建自定义ExcelExporter子类用于日志输出
            from utils import ExcelExporter
            
            class UIExcelExporter(ExcelExporter):
                def __init__(self, sql_query, output_file, page_size=None, log_text=None, queue=None):
                    # 创建回调函数用于日志输出
                    def log_callback(message):
                        if queue:
                            queue.put(("info", message))
                            
                    super().__init__(sql_query, output_file, page_size, log_callback=log_callback)
                    self.log_text = log_text
                    self.queue = queue
                    
                def get_total_records(self, cursor=None):
                    total = super().get_total_records(cursor)
                    if self.queue:
                        self.queue.put(("info", f"共找到 {total} 条记录，开始分页导出..."))
                    return total
                
                def export_page(self, cursor):
                    if hasattr(self, 'last_update_time'):
                        current_time = time.time()
                        # 每5秒更新一次进度,避免过于频繁的UI更新
                        if current_time - self.last_update_time < 5:
                            super().export_page(cursor)
                            return
                    else:
                        self.last_update_time = time.time()
                    
                    super().export_page(cursor)
                    processed = min(self.current_offset, self.total_records)
                    if self.queue:
                        progress = processed/self.total_records
                        self.queue.put(("progress", f"已导出 {processed}/{self.total_records} 条记录 ({progress:.1%})"))
                        self.last_update_time = time.time()

            # 创建队列用于线程间通信
            self.export_queue = queue.Queue()

            def export_in_thread():
                try:
                    exporter = UIExcelExporter(sql_text, file_path, queue=self.export_queue)
                    exporter.utils = self.hana_utils  # 使用已有的数据库连接
                    exporter.export()
                    self.export_queue.put(("success", f"结果已导出到: {file_path}"))
                except Exception as e:
                    self.export_queue.put(("error", f"导出失败: {str(e)}"))
                finally:
                    self.export_queue.put(("done", None))

            def check_export_status():
                try:
                    while not self.export_queue.empty():
                        msg_type, content = self.export_queue.get_nowait()
                        
                        if msg_type in ["info", "progress", "success", "error"]:
                            self.log_message(content)
                        elif msg_type == "done":
                            # 启用所有导出按钮
                            for child in self.root.winfo_children():
                                if isinstance(child, ttk.Button) and ("导出" in child["text"]):
                                    child["state"] = "normal"
                            return
                            
                    # 继续检查
                    self.root.after(100, check_export_status)
                except queue.Empty:
                    # 继续检查
                    self.root.after(100, check_export_status)

            # 创建并启动后台线程
            thread = threading.Thread(target=export_in_thread)
            thread.daemon = True
            thread.start()

            # 开始检查导出状态
            self.root.after(100, check_export_status)
        
    def close_tab(self):
        """关闭当前标签页"""
        if len(self.notebook.tabs()) <= 1:
            self.log_message("不能关闭最后一个标签页")
            return
            
        current_tab = self.notebook.nametowidget(self.notebook.select())
        sql_input = getattr(current_tab, 'sql_input', None)
        
        if sql_input:
            # 检查SQL输入框内容是否已保存
            sql_text = sql_input.get("1.0", tk.END).strip()
            if sql_text:
                # 提示用户保存
                save = tk.messagebox.askyesnocancel(
                    "保存更改",
                    "当前标签页有未保存的更改，是否保存？"
                )
                if save is None:  # 用户点击取消
                    return
                if save:  # 用户选择保存
                    self.save_sql()
        
        # 从文件路径映射中移除该标签页
        if current_tab in self.tab_file_paths:
            del self.tab_file_paths[current_tab]
            
        # 关闭标签页
        self.notebook.forget(current_tab)
        self.log_message("已关闭当前标签页")
        
    def clear_sql(self):
        # 清空当前查询窗口的SQL文本
        sql_input, _, _ = self.get_current_tab_widgets()
        if sql_input:
            sql_input.delete("1.0", tk.END)
            self.log_message("已清空查询语句")
            
    def check_connection_status(self):
        """检查数据库实际连接状态"""
        try:
            # 尝试执行一个简单的查询来验证连接
            if hasattr(self.hana_utils, '_connection') and self.hana_utils._connection:
                cursor = self.hana_utils.get_cursor()
                cursor.execute("SELECT 1 FROM DUMMY")
                cursor.fetchone()
                return True
        except Exception:
            return False
        return False

    def update_connection_button(self):
        """更新连接按钮状态和文本"""
        is_connected = self.check_connection_status()
        self.connected = is_connected
        self.connect_button.configure(
            text="断开数据库" if is_connected else "连接数据库"
        )

    def connect_disconnect_db(self):
        """连接或断开数据库"""
        current_status = self.check_connection_status()
        
        if not current_status:
            try:
                # 尝试连接数据库
                self.hana_utils.connect()
                if self.check_connection_status():
                    self.connected = True
                    self.log_message("数据库连接成功")
                else:
                    self.connected = False
                    self.log_message("数据库连接失败：无法验证连接")
            except Exception as e:
                self.connected = False
                self.log_message(str(e))
        else:
            try:
                # 尝试断开连接
                self.hana_utils.disconnect()
                self.connected = False
                self.log_message("数据库已断开连接")
            except Exception as e:
                self.log_message(str(e))
                # 再次检查连接状态
                self.connected = self.check_connection_status()

        # 更新按钮状态
        self.update_connection_button()
                
    def create_menu(self):
        # 创建菜单栏
        pass
        
    def log_message(self, message):
        # 记录日志
        _, _, log_text = self.get_current_tab_widgets()
        if not log_text:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        log_text.see(tk.END)
        
if __name__ == "__main__":
    root = tk.Tk()
    app = HanaQueryAnalyzer(root)
    root.mainloop()
