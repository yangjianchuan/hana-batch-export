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
import re

class HanaQueryAnalyzer:
    def __init__(self, root):
        # 初始化数据相关的属性
        self._all_data = []
        self._display_data = []
        self._batch_size = 100
        self._current_sort_col = None
        self._sort_reverse = False

        # 高亮相关的属性
        self._highlight_after_id = None
        self._last_content = None
        self._token_colors = {}
        
        # 列宽相关的缓存
        self._column_widths = {}  # 缓存列宽
        self._column_content_widths = {}  # 缓存内容宽度
        self._last_resize_time = 0  # 上次调整列宽的时间
        
        self.root = root
        self.root.title("HANA 查询分析器")
        
        # 设置窗口全屏
        self.root.state('zoomed')  # 在Windows上使用zoomed状态实现全屏
        # 对于其他操作系统，可以使用以下方式：
        # self.root.attributes('-zoomed', True)  # Linux
        # self.root.attributes('-fullscreen', True)  # macOS
        
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

        # 添加高亮开关状态
        self.highlight_enabled = False
        
        # 在按钮区域添加高亮开关按钮
        ttk.Button(button_frame, text="开启/关闭高亮 (Ctrl+L)", command=self.toggle_highlight).pack(side=tk.LEFT, padx=2)
        
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
        self.root.bind_all("<Control-l>", lambda e: self.toggle_highlight())
        self.root.bind_all("<Control-L>", lambda e: self.toggle_highlight())
        
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
        
        # 添加参数值缓存字典
        self.param_cache = {}
        
    def create_context_menu(self, sql_input):
        context_menu = tk.Menu(self.root, tearoff=0)
        
        # 标准编辑功能
        context_menu.add_command(label="复制 (Ctrl+C)", command=lambda: sql_input.event_generate("<<Copy>>"))
        context_menu.add_command(label="粘贴 (Ctrl+V)", command=lambda: sql_input.event_generate("<<Paste>>"))
        context_menu.add_command(label="剪切 (Ctrl+X)", command=lambda: sql_input.event_generate("<<Cut>>"))
        context_menu.add_separator()
        context_menu.add_command(label="全选 (Ctrl+A)", command=lambda: self.select_all(sql_input))
        
        # SQL相关功能
        context_menu.add_separator()
        context_menu.add_command(label="执行选中 (Ctrl+F8)", command=self.execute_selected)
        context_menu.add_command(label="格式化SQL", command=lambda: self.format_sql(sql_input))
        
        return context_menu
        
    def show_context_menu(self, event, sql_input):
        context_menu = self.create_context_menu(sql_input)
        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()
            
    def select_all(self, widget):
        widget.tag_add(tk.SEL, "1.0", tk.END)
        widget.mark_set(tk.INSERT, "1.0")
        widget.see(tk.INSERT)
        return 'break'
        
    def format_sql(self, sql_input):
        try:
            # 获取选中的文本，如果没有选中则获取所有文本
            try:
                sql_text = sql_input.get(tk.SEL_FIRST, tk.SEL_LAST)
                has_selection = True
            except tk.TclError:
                sql_text = sql_input.get("1.0", tk.END)
                has_selection = False
                
            # 简单的SQL格式化
            formatted_sql = self._format_sql_text(sql_text)
            
            if has_selection:
                sql_input.delete(tk.SEL_FIRST, tk.SEL_LAST)
                sql_input.insert(tk.INSERT, formatted_sql)
            else:
                sql_input.delete("1.0", tk.END)
                sql_input.insert("1.0", formatted_sql)
                
            self.highlight_sql()
            self.log_message("SQL格式化完成")
        except Exception as e:
            self.log_message(f"SQL格式化失败: {str(e)}")
            
    def _format_sql_text(self, sql_text):
        # 基本的SQL格式化逻辑
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'AND', 'OR']
        lines = []
        current_indent = 0
        
        # 分割SQL语句
        sql_parts = sql_text.split('\n')
        
        for part in sql_parts:
            part = part.strip()
            if not part:
                continue
                
            # 检查关键字
            upper_part = part.upper()
            is_keyword = any(upper_part.startswith(keyword) for keyword in keywords)
            
            # 调整缩进
            if any(k in upper_part for k in ['SELECT', 'FROM', 'WHERE']):
                if 'SELECT' in upper_part:
                    current_indent = 0
                else:
                    current_indent = 1
            
            # 添加格式化的行
            formatted_line = '    ' * current_indent + part
            lines.append(formatted_line)
            
        return '\n'.join(lines)
        
    def add_tab(self):
        if len(self.notebook.tabs()) >= 10:
            return
            
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text=f"Query {len(self.notebook.tabs()) + 1}")
        
        # 创建SQL输入区域的容器框架
        sql_frame = ttk.Frame(frame)
        sql_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建行号文本框
        line_numbers = tk.Text(sql_frame, width=4)
        line_numbers.pack(side=tk.LEFT, fill=tk.Y)
        
        # 配置行号文本框的样式
        line_numbers.configure(
            background='#f0f0f0',
            foreground='gray',
            padx=5,
            state='disabled',
            relief=tk.FLAT,
            takefocus=0,
            wrap='none',  # 禁用自动换行
            cursor='arrow'  # 使用箭头光标
        )
        
        # SQL输入框
        sql_input = scrolledtext.ScrolledText(sql_frame, wrap=tk.NONE)  # 修改为 NONE
        sql_input.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # 绑定右键菜单
        sql_input.bind('<Button-3>', lambda e: self.show_context_menu(e, sql_input))
        # 绑定Ctrl+A快捷键
        sql_input.bind('<Control-Key-a>', lambda e: self.select_all(sql_input))
        # 绑定更新行号的事件
        sql_input.bind('<Key>', lambda e: self.update_line_numbers(sql_input, line_numbers))
        sql_input.bind('<Return>', lambda e: self.update_line_numbers(sql_input, line_numbers))
        sql_input.bind('<BackSpace>', lambda e: self.update_line_numbers(sql_input, line_numbers))
        sql_input.bind('<Delete>', lambda e: self.update_line_numbers(sql_input, line_numbers))  # 添加Delete键的绑定
        sql_input.bind('<<Paste>>', lambda e: self.root.after(10, lambda: self.update_line_numbers(sql_input, line_numbers)))
        sql_input.bind('<<Cut>>', lambda e: self.root.after(10, lambda: self.update_line_numbers(sql_input, line_numbers)))  # 添加剪切事件的绑定
        
        # 修改滚动同步处理
        def on_scroll(*args):
            try:
                if args[0] == 'scroll':
                    # 滚动事件
                    line_numbers.yview_scroll(int(args[1]), args[2])
                    sql_input.yview_scroll(int(args[1]), args[2])
                elif args[0] == 'moveto':
                    # 移动到指定位置
                    line_numbers.yview_moveto(float(args[1]))
                    sql_input.yview_moveto(float(args[1]))
            except Exception as e:
                print(f"Scroll error: {str(e)}")

        def on_mousewheel(event):
            try:
                # 处理鼠标滚轮事件
                delta = -1 * (event.delta // 120)
                line_numbers.yview_scroll(delta, "units")
                sql_input.yview_scroll(delta, "units")
                return "break"  # 阻止事件继续传播
            except Exception as e:
                print(f"Mousewheel error: {str(e)}")
        
        # 绑定滚动事件
        sql_input.bind("<MouseWheel>", on_mousewheel)
        
        # 获取滚动条组件
        scrollbar = sql_input.vbar
        
        # 配置滚动条命令
        scrollbar.config(command=on_scroll)
        
        # 配置同步滚动
        def sync_scroll(*args):
            try:
                line_numbers.yview_moveto(args[0])
                return True
            except Exception as e:
                print(f"Sync scroll error: {str(e)}")
                return False
        
        sql_input.config(yscrollcommand=scrollbar.set)
        line_numbers.config(yscrollcommand=sync_scroll)
        
        # 保存行号文本框的引用
        sql_input.line_numbers = line_numbers
        
        # 初始化行号
        self.update_line_numbers(sql_input, line_numbers)
        
        # 使用PanedWindow来分隔结果区域和日志区域
        paned = ttk.PanedWindow(frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # 结果展示区域
        result_frame = ttk.Frame(paned)
        paned.add(result_frame, weight=3)  # 结果区域占比更大
        
        # 创建带虚拟滚动的Treeview
        tree_frame = ttk.Frame(result_frame)  # 新增容器frame
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建水平滚动条容器
        hsb_frame = ttk.Frame(tree_frame)
        hsb_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        # 设置固定行高和列宽以提高渲染性能
        tree = ttk.Treeview(tree_frame, show="headings", selectmode="extended", height=15)
        tree.tag_configure('evenrow', background='#f0f0f0')
        tree.tag_configure('oddrow', background='#ffffff')
        
        # 自定义滚动条类，实现平滑滚动
        class SmoothScrollbar(ttk.Scrollbar):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.hover = False
                
            def set(self, first, last):
                first = float(first)
                last = float(last)
                if first <= 0 and last >= 1:
                    self.pack_forget()
                else:
                    self.pack(fill=tk.Y if self['orient'] == 'vertical' else tk.X, expand=True)
                super().set(first, last)
                
        # 添加滚动条并保存为tree的属性以便后续访问
        tree.vsb = SmoothScrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.hsb = SmoothScrollbar(hsb_frame, orient="horizontal", command=tree.xview)
        
        # 使用pack布局管理器，添加水平滚动条到专用容器
        tree.vsb.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)
        tree.hsb.pack(fill=tk.X, expand=True)
        
        # 配置Treeview的滚动
        tree.configure(yscrollcommand=tree.vsb.set, xscrollcommand=tree.hsb.set)
        
        # 优化样式设置
        style = ttk.Style()
        style.configure('Treeview', rowheight=25)
        style.configure('Treeview', wrap='none')
        
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
        """手动触发的高亮处理"""
        # 获取当前标签页的SQL输入框
        sql_input, _, _ = self.get_current_tab_widgets()
        if not sql_input:
            return

        # 使用防抖动延迟执行高亮处理
        if self._highlight_after_id:
            self.root.after_cancel(self._highlight_after_id)
        self._highlight_after_id = self.root.after(100, lambda: self._do_highlight(sql_input))

    def _do_highlight(self, sql_input):
        """实际执行高亮处理的函数"""
        try:
            # 获取整个文本内容
            text_content = sql_input.get("1.0", tk.END)

            # 如果内容没有变化，跳过处理
            if text_content == self._last_content:
                return
            self._last_content = text_content

            # 移除所有现有标记
            for tag in sql_input.tag_names():
                if str(tag).startswith('Token') or tag == "quoted_string":
                    sql_input.tag_remove(tag, "1.0", tk.END)

            # 先处理引号内容
            start_index = "1.0"
            while True:
                # 查找下一个引号
                quote_pos = sql_input.search(r'["\']', start_index, tk.END)
                if not quote_pos:
                    break
                    
                try:
                    # 获取引号类型
                    quote_char = sql_input.get(quote_pos)
                    # 查找匹配的结束引号
                    end_pos = sql_input.search(quote_char, f"{quote_pos}+1c", tk.END)
                    if not end_pos:
                        break
                    
                    # 为整个引号内容（包括引号）添加标记
                    sql_input.tag_add("quoted_string", quote_pos, f"{end_pos}+1c")
                    
                    # 更新搜索起始位置
                    start_index = f"{end_pos}+1c"
                    
                except tk.TclError:
                    break

            # 配置引号内容的样式
            sql_input.tag_config("quoted_string", foreground="blue")

            # 使用pygments进行其他语法分析
            for token, content in lex(text_content, SqlLexer()):
                if not content.strip() or any(c in content for c in '"\''):  # 跳过空白内容和引号内容
                    continue

                start_index = "1.0"
                while True:
                    try:
                        # 在整个文本中搜索匹配
                        pos = sql_input.search(content, start_index, tk.END)
                        if not pos:
                            break

                        # 检查该位置是否已经被引号标记覆盖
                        if "quoted_string" in sql_input.tag_names(pos):
                            start_index = f"{pos}+{len(content)}c"
                            continue

                        # 计算结束位置
                        end = f"{pos}+{len(content)}c"

                        # 使用缓存的颜色配置
                        token_str = str(token)
                        if token_str not in self._token_colors:
                            self._token_colors[token_str] = self.get_token_color(token)
                            # 为关键字配置粗体
                            if token is Token.Keyword:
                                sql_input.tag_config(token_str, foreground=self._token_colors[token_str], font=('TkDefaultFont', 10, 'bold'))
                            else:
                                sql_input.tag_config(token_str, foreground=self._token_colors[token_str])

                        # 添加标记
                        sql_input.tag_add(token_str, pos, end)
                        
                        # 更新起始位置
                        start_index = end
                    except tk.TclError:
                        break  # 处理搜索过程中可能出现的错误
        except Exception as e:
            print(f"Highlighting error: {str(e)}")  # 添加错误输出以便调试
                
    def get_token_color(self, token):
        # 定义不同token类型的颜色
        colors = {
            Token.Keyword: "blue",  # 关键字改为蓝色
            Token.Operator: "red",
            Token.Literal.String: "blue",  # 字符串为蓝色
            Token.Literal.String.Single: "blue",  # 单引号字符串
            Token.Literal.String.Double: "blue",  # 双引号字符串
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
                    
                    # 手动更新行号
                    self.update_line_numbers(sql_input, sql_input.line_numbers)
                    
                    # 如果高亮功能已开启，则触发高亮
                    if self.highlight_enabled:
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
        """执行SQL查询"""
        # 检查是否包含占位符
        placeholders = self.get_placeholders(sql_text)
        if placeholders:
            # 显示参数输入对话框
            params = self.show_parameter_dialog(placeholders)
            if params is None:  # 用户取消了输入
                self.log_message("已取消执行")
                return
            # 替换占位符
            sql_text = self.replace_placeholders(sql_text, params)
            self.log_message("已替换占位符")
        
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
                    self._handle_columns(result_text, content)
                elif msg_type == "data":
                    self._handle_data(result_text, content)
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

    def _handle_columns(self, result_text, content):
        """处理列信息"""
        # 清除现有数据和排序状态
        self._current_sort_col = None
        self._sort_reverse = False
        self._all_data = []
        self._display_data = []
        self._batch_size = 100
        
        # 配置列并存储列名
        result_text["columns"] = content
        result_text._column_names = content  # 存储列名以供复制使用
        
        # 设置每列的标题
        for col in content:
            result_text.heading(col, text=col)  # 添加这行来设置列标题
            result_text.column(col, width=50, minwidth=50, stretch=False)  # 设置初始列宽
        
        # 延迟计算列宽，提升性能
        def resize_columns():
            if hasattr(self, '_resize_after_id'):
                result_text.after_cancel(self._resize_after_id)
                
            def do_resize():
                # 计算最佳列宽，使用缓存优化
                for col in content:
                    # 优先使用缓存的列宽
                    if col in self._column_widths:
                        result_text.column(col, width=self._column_widths[col], minwidth=50, stretch=False)
                        continue
                        
                    # 基于列名的最小宽度
                    col_width = max(100, min(200, len(col) * 10))
                    
                    # 检查内容宽度，使用缓存
                    if col in self._column_content_widths:
                        content_width = self._column_content_widths[col]
                    else:
                        content_width = 0
                        sample_items = result_text.get_children()[:50]  # 减少采样数量提升性能
                        for item in sample_items:
                            val = result_text.set(item, col)
                            content_width = max(content_width, len(str(val)) * 8)
                        self._column_content_widths[col] = content_width
                        
                    # 计算并缓存最终宽度
                    final_width = max(col_width, content_width)
                    self._column_widths[col] = final_width
                    result_text.column(col, width=final_width, minwidth=50, stretch=False)
                    
                    # 添加列排序功能
                    result_text.heading(col, text=col, command=lambda c=col: self.sort_column(result_text, c))
                    
                # 最后一列可以stretch
                if content:
                    result_text.column(content[-1], stretch=True)
                
                # 更新滚动条状态
                self._update_scrollbars(result_text)
                    
            # 延迟执行以提升性能
            self._resize_after_id = result_text.after(100, do_resize)
            
        # 延迟调用列宽计算
        result_text.after(50, resize_columns)
        
    def _update_scrollbars(self, tree):
        """更新树控件的滚动条状态"""
        if not hasattr(tree, 'vsb') or not hasattr(tree, 'hsb'):
            return
            
        try:
            # 检查是否需要垂直滚动条
            first, last = tree.yview()
            if first <= 0 and last >= 1:
                tree.vsb.pack_forget()
            else:
                tree.vsb.pack(side=tk.RIGHT, fill=tk.Y)
                
            # 检查是否需要水平滚动条
            first, last = tree.xview()
            if first <= 0 and last >= 1:
                tree.hsb.pack_forget()
            else:
                tree.hsb.pack(fill=tk.X, expand=True)
                
            # 强制更新UI
            tree.update_idletasks()
        except Exception:
            # 忽略任何可能的tkinter错误
            pass

    def _handle_data(self, result_text, content):
        """处理数据"""
        # 批量处理数据
        self._all_data.extend([tuple(row) for _, row in content.iterrows()])
        # 初始显示第一批数据
        display_rows = self._all_data[:self._batch_size]
        for i, row in enumerate(display_rows):
            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            result_text.insert("", "end", values=row, tags=(tag,))

    def _setup_tree_events(self, result_text):
        """设置树形控件的事件绑定"""
        # 绑定滚动相关事件以便动态加载数据
        result_text.bind('<MouseWheel>', lambda e: self.on_scroll(e, result_text))
        result_text.bind('<Motion>', lambda e: self.on_mouse_move(e, result_text))
        
        # 绑定快捷键
        result_text.bind('<Control-c>', lambda e: self.copy_selected_with_headers(result_text))
        result_text.bind('<Control-a>', lambda e: self.select_all_results(result_text))
        
        # 绑定右键菜单
        result_text.bind('<Button-3>', lambda e: self.show_results_context_menu(e, result_text))
        
        # 绑定双击事件
        result_text.bind('<Double-Button-1>', lambda e: self.copy_cell_content(e, result_text))
        
        # 滚动条事件绑定 - 使用树控件的内置滚动条属性
        if hasattr(result_text, 'vsb'):
            result_text.vsb.bind('<B1-Motion>', lambda e: self.on_scrollbar_drag(e, result_text))
        
        # 内存优化事件绑定
        result_text.bind('<Leave>', lambda e: self.clear_tree_memory(result_text))
        result_text.bind('<Enter>', lambda e: self.clear_tree_memory(result_text))
        result_text.bind('<Configure>', lambda e: self.clear_tree_memory(result_text))
        
        # 定期清理定时器
        self._cleanup_after_id = self.root.after(5000, lambda: self.clear_tree_memory(result_text))
            
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

        # 处理占位符
        placeholders = self.get_placeholders(sql_text)
        if placeholders:
            # 显示参数输入对话框
            params = self.show_parameter_dialog(placeholders)
            if params is None:  # 用户取消了输入
                self.log_message("已取消导出")
                return
            # 替换占位符
            sql_text = self.replace_placeholders(sql_text, params)
            self.log_message("已替换占位符")

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

        # 处理占位符
        placeholders = self.get_placeholders(sql_text)
        if placeholders:
            # 显示参数输入对话框
            params = self.show_parameter_dialog(placeholders)
            if params is None:  # 用户取消了输入
                self.log_message("已取消导出")
                return
            # 替换占位符
            sql_text = self.replace_placeholders(sql_text, params)
            self.log_message("已替换占位符")
        
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

        # 处理占位符
        placeholders = self.get_placeholders(sql_text)
        if placeholders:
            # 显示参数输入对话框
            params = self.show_parameter_dialog(placeholders)
            if params is None:  # 用户取消了输入
                self.log_message("已取消导出")
                return
            # 替换占位符
            sql_text = self.replace_placeholders(sql_text, params)
            self.log_message("已替换占位符")

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
        
    def sort_column(self, tree, col):
        """列排序功能"""
        if not self._all_data:
            return
            
        # 更新排序状态
        if self._current_sort_col != col:
            self._sort_reverse = False
        else:
            self._sort_reverse = not self._sort_reverse
        self._current_sort_col = col
        
        # 获取列索引
        col_idx = tree["columns"].index(col)
        
        # 对所有数据进行排序
        self._all_data.sort(key=lambda x: (x[col_idx] is None, x[col_idx]), reverse=self._sort_reverse)
        
        # 清除现有显示
        for item in tree.get_children():
            tree.delete(item)
            
        # 重新显示第一批数据
        for i, row in enumerate(self._all_data[:self._batch_size]):
            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            tree.insert("", "end", values=row, tags=(tag,))

    def on_scroll(self, event, tree):
        """处理滚动事件"""
        if not hasattr(self, '_all_data') or not self._all_data or len(self._all_data) <= self._batch_size:
            return
            
        # 取消之前的清理定时器
        if hasattr(self, '_cleanup_after_id'):
            self.root.after_cancel(self._cleanup_after_id)
            
        current_items = len(tree.get_children())
        if current_items >= len(self._all_data):
            return
            
        # 检查是否接近底部
        first, last = tree.yview()
        if last > 0.9:
            self.load_more_data(tree)
            
        # 设置新的清理定时器
        self._cleanup_after_id = self.root.after(5000, lambda: self.clear_tree_memory(tree))
            
    def on_mouse_move(self, event, tree):
        """处理鼠标移动事件，优化滚动和内存使用"""
        # 取消之前的定时器
        for timer_id in ['_scroll_after_id', '_cleanup_after_id']:
            if hasattr(self, timer_id):
                self.root.after_cancel(getattr(self, timer_id))
                
        # 设置新的滚动检查定时器
        self._scroll_after_id = self.root.after(50, lambda: self.check_scroll_position(tree))
        # 设置新的内存清理定时器
        self._cleanup_after_id = self.root.after(5000, lambda: self.clear_tree_memory(tree))
        
    def on_scrollbar_drag(self, event, tree):
        """处理滚动条拖动，优化性能和内存使用"""
        # 取消之前的定时器
        for timer_id in ['_drag_after_id', '_cleanup_after_id']:
            if hasattr(self, timer_id):
                self.root.after_cancel(getattr(self, timer_id))
                
        # 设置新的滚动检查定时器
        self._drag_after_id = self.root.after(50, lambda: self.check_scroll_position(tree))
        # 设置新的内存清理定时器
        self._cleanup_after_id = self.root.after(5000, lambda: self.clear_tree_memory(tree))
        
    def check_scroll_position(self, tree):
        """检查滚动位置并按需加载数据"""
        if not self._all_data:
            return
            
        first, last = tree.yview()
        if last > 0.7:  # 当滚动到70%时加载更多
            self.load_more_data(tree)
            
    def load_more_data(self, tree):
        """加载更多数据到显示区域"""
        current_items = len(tree.get_children())
        if current_items >= len(self._all_data):
            return
            
        # 计算新的显示范围
        end_idx = min(current_items + self._batch_size, len(self._all_data))
        new_data = self._all_data[current_items:end_idx]
        
        # 批量插入新数据
        for i, row in enumerate(new_data, start=current_items):
            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            tree.insert("", "end", values=row, tags=(tag,))
            
    def log_message(self, message):
        """记录日志信息"""
        _, _, log_text = self.get_current_tab_widgets()
        if not log_text:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        log_text.see(tk.END)
        
    def create_results_context_menu(self, tree):
        """创建结果区域的右键菜单"""
        context_menu = tk.Menu(self.root, tearoff=0)
        context_menu.add_command(label="复制 (Ctrl+C)", command=lambda: self.copy_selected_with_headers(tree))
        context_menu.add_command(label="全选 (Ctrl+A)", command=lambda: self.select_all_results(tree))
        return context_menu
        
    def show_results_context_menu(self, event, tree):
        """显示结果区域的右键菜单"""
        context_menu = self.create_results_context_menu(tree)
        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()
            
    def select_all_results(self, tree):
        """选择所有结果"""
        for item in tree.get_children():
            tree.selection_add(item)
            
    def copy_cell_content(self, event, tree):
        """复制单元格内容到剪贴板并显示提示"""
        # 获取点击位置的单元格
        item = tree.identify('item', event.x, event.y)
        column = tree.identify('column', event.x, event.y)
        
        if not item or not column:
            return
            
        # 获取实际的列信息
        columns = tree["columns"]
        column_name = tree.heading(column)["text"]
        col_idx = columns.index(column_name)
        
        # 从values中获取对应列的值
        values = tree.item(item)['values']
        value = values[col_idx] if values and col_idx < len(values) else ''
            
        # 复制到剪贴板
        self.root.clipboard_clear()
        self.root.clipboard_append(str(value))
        
        # 记录日志提示
        self.log_message(f"已复制单元格内容: {value}")
            
    def clear_tree_memory(self, tree):
        """清理Treeview的内存使用"""
        if not hasattr(self, '_all_data') or not self._all_data:
            return
            
    def copy_selected_with_headers(self, tree):
        """复制选中行及表头数据到剪贴板"""
        # 确保有列名
        if not hasattr(tree, '_column_names'):
            return
            
        # 获取选中的行
        selection = tree.selection()
        if not selection:
            return
            
        # 获取列名
        headers = tree._column_names
            
        # 构建要复制的数据
        rows = []
        # 添加表头
        rows.append('\t'.join(headers))
        
        # 添加选中的行
        for item in selection:
            values = tree.item(item)['values']
            if values:
                # 将所有值转换为字符串并确保None显示为空字符串
                values = [str(v) if v is not None else '' for v in values]
                rows.append('\t'.join(values))
        
        # 合并所有行，用换行符分隔
        copy_text = '\n'.join(rows)
        
        # 复制到剪贴板
        self.root.clipboard_clear()
        self.root.clipboard_append(copy_text)
            
        visible_items = tree.get_children()
        if len(visible_items) <= self._batch_size:
            return
            
        # 获取可见区域的起始和结束位置
        first, last = tree.yview()
        total_items = len(visible_items)
        
        # 计算可见项目的范围
        visible_start = int(first * total_items)
        visible_end = int(last * total_items) + 1
        
        # 保留可见区域周围的缓冲区
        buffer_size = self._batch_size // 2
        start_idx = max(0, visible_start - buffer_size)
        end_idx = min(total_items, visible_end + buffer_size)
        
        # 删除缓冲区外的项目
        items_to_keep = visible_items[start_idx:end_idx]
        items_to_remove = [item for item in visible_items if item not in items_to_keep]
        
        if items_to_remove:
            # 暂时隐藏滚动条避免闪烁
            if hasattr(tree, 'vsb'):
                tree.vsb.pack_forget()
            if hasattr(tree, 'hsb'):
                tree.hsb.pack_forget()
            
            # 批量删除项目
            for item in items_to_remove:
                tree.delete(item)
            
            # 恢复滚动条
            if hasattr(tree, 'vsb'):
                tree.vsb.pack(side=tk.RIGHT, fill=tk.Y)
            if hasattr(tree, 'hsb'):
                tree.hsb.pack(fill=tk.X, expand=True)
        
    def show_parameter_dialog(self, params):
        """显示参数输入对话框"""
        dialog = tk.Toplevel(self.root)
        dialog.title("请输入参数")
        dialog.geometry("400x500")  # 增加窗口宽度和高度
        
        # 使对话框模态
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 创建滚动框架
        canvas = tk.Canvas(dialog)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 存储参数输入框的字典
        param_entries = {}
        
        # 为每个参数创建标签和输入框
        for i, param in enumerate(params):
            param_frame = ttk.Frame(scrollable_frame)
            param_frame.pack(fill="x", padx=10, pady=5)  # 增加内边距
            
            # 创建标签，固定宽度
            label = ttk.Label(param_frame, text=f"{param}:", width=15)  # 设置固定宽度
            label.pack(side="left", padx=5)
            
            # 创建输入框，占用剩余空间
            entry = ttk.Entry(param_frame)
            entry.pack(side="left", fill="x", expand=True, padx=5)
            
            # 如果有缓存的值，填入输入框
            if param in self.param_cache:
                entry.insert(0, self.param_cache[param])
            param_entries[param] = entry
        
        # 按钮框架
        button_frame = ttk.Frame(dialog)
        
        # 结果变量
        dialog.result = None
        
        def on_ok():
            # 收集所有参数值并更新缓存
            values = {param: entry.get() for param, entry in param_entries.items()}
            self.param_cache.update(values)  # 更新参数缓存
            dialog.result = values
            dialog.destroy()
            
        def on_cancel():
            dialog.result = None
            dialog.destroy()
            
        def on_clear():
            # 清空所有输入框
            for entry in param_entries.values():
                entry.delete(0, tk.END)
        
        def on_copy():
            # 收集当前参数值
            values = {param: entry.get() for param, entry in param_entries.items()}
            # 获取当前标签页的SQL输入框
            sql_input, _, _ = self.get_current_tab_widgets()
            if sql_input:
                # 获取原始SQL
                sql_text = sql_input.get("1.0", tk.END).strip()
                # 替换占位符
                replaced_sql = self.replace_placeholders(sql_text, values)
                # 复制到剪贴板
                dialog.clipboard_clear()
                dialog.clipboard_append(replaced_sql)
                # 显示提示消息
                self.log_message("已复制替换后的SQL语句到剪贴板")
        
        # 创建两行按钮布局
        top_button_frame = ttk.Frame(button_frame)
        top_button_frame.pack(fill="x", pady=(5,2))
        
        bottom_button_frame = ttk.Frame(button_frame)
        bottom_button_frame.pack(fill="x", pady=(2,5))
        
        # 使用更醒目的样式创建按钮
        ok_button = ttk.Button(top_button_frame, text="确定", command=on_ok, style="Accent.TButton", width=15)
        ok_button.pack(side="left", padx=10)
        
        copy_button = ttk.Button(top_button_frame, text="复制语句", command=on_copy, width=15)
        copy_button.pack(side="right", padx=10)
        
        clear_button = ttk.Button(bottom_button_frame, text="清空", command=on_clear, width=15)
        clear_button.pack(side="left", padx=10)
        
        cancel_button = ttk.Button(bottom_button_frame, text="取消", command=on_cancel, width=15)
        cancel_button.pack(side="right", padx=10)
        
        # 创建按钮样式
        style = ttk.Style()
        style.configure("Accent.TButton", foreground="blue")
        
        # 布局按钮框架
        button_frame.pack(side="bottom", fill="x", padx=10, pady=10)
        
        # 布局主要组件
        canvas.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        scrollbar.pack(side="right", fill="y", pady=5)
        
        # 等待对话框关闭
        dialog.wait_window()
        return dialog.result

    def get_placeholders(self, sql):
        """从SQL中提取占位符"""
        # 处理 ${xxx} 格式的占位符
        pattern1 = r'\${([^}]+)}'
        matches1 = re.finditer(pattern1, sql)
        placeholders1 = set(match.group(1) for match in matches1)
        
        # 处理 ? 格式的占位符
        pattern2 = r'\?'
        matches2 = re.findall(pattern2, sql)
        # 为问号占位符生成参数名
        placeholders2 = {f"第{i+1}个参数" for i in range(len(matches2))}
        
        # 合并两种格式的占位符并排序
        return sorted(placeholders1.union(placeholders2))

    def replace_placeholders(self, sql, params):
        """替换SQL中的占位符"""
        # 先处理 ${xxx} 格式的占位符
        for param, value in params.items():
            if not param.startswith("第") or not param.endswith("个参数"):
                placeholder = f'${{{param}}}'
                if "PLACEHOLDER." in sql:
                    # 对于计算视图的参数，不需要额外添加引号，直接使用值
                    sql = sql.replace(placeholder, value)
                else:
                    # 普通SQL参数的处理
                    try:
                        float(value)
                        sql = sql.replace(placeholder, value)
                    except ValueError:
                        sql = sql.replace(placeholder, f"'{value}'")
        
        # 再处理 ? 格式的占位符
        question_marks = re.findall(r'\?', sql)
        if question_marks:
            for i in range(len(question_marks)):
                param_name = f"第{i+1}个参数"
                if param_name in params:
                    value = params[param_name]
                    # 对于计算视图的参数，总是添加单引号
                    if "PLACEHOLDER." in sql:
                        # 如果值本身已经包含引号，则不添加
                        if value.startswith("'") and value.endswith("'"):
                            sql = sql.replace('?', value, 1)
                        else:
                            sql = sql.replace('?', f"'{value}'", 1)
                    else:
                        # 普通SQL参数的处理
                        try:
                            float(value)
                            sql = sql.replace('?', value, 1)
                        except ValueError:
                            sql = sql.replace('?', f"'{value}'", 1)
        
        return sql

    def toggle_highlight(self):
        """切换高亮状态并应用高亮"""
        self.highlight_enabled = not self.highlight_enabled
        
        # 获取当前标签页的SQL输入框
        sql_input, _, _ = self.get_current_tab_widgets()
        if not sql_input:
            return
        
        if self.highlight_enabled:
            # 强制重置上次内容，确保会触发高亮
            self._last_content = None
            self.highlight_sql()  # 直接调用高亮处理
            self.log_message("SQL高亮已开启")
        else:
            # 清除所有高亮标记
            for tag in sql_input.tag_names():
                if str(tag).startswith('Token') or tag == "quoted_string":
                    sql_input.tag_remove(tag, "1.0", tk.END)
            self.log_message("SQL高亮已关闭")

    def update_line_numbers(self, sql_input, line_numbers):
        """更新行号"""
        try:
            # 获取文本内容并计算行数
            text_content = sql_input.get("1.0", "end-1c")
            # 计算实际行数，确保至少有1行
            num_lines = max(1, text_content.count('\n') + 1)
            
            # 生成行号文本，每个行号后加换行符
            line_numbers_text = '\n'.join(str(i).rjust(3) for i in range(1, num_lines + 1))
            
            # 更新行号文本框
            line_numbers.configure(state='normal')
            line_numbers.delete("1.0", tk.END)
            line_numbers.insert("1.0", line_numbers_text)
            
            # 如果最后一行不是空行，确保行号区域和文本区域对齐
            if not text_content.endswith('\n'):
                line_numbers.insert(tk.END, '\n')
                
            line_numbers.configure(state='disabled')
            
            # 同步滚动位置
            sql_input.update_idletasks()
            line_numbers.update_idletasks()
            line_numbers.yview_moveto(sql_input.yview()[0])
            
        except Exception as e:
            print(f"Error updating line numbers: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = HanaQueryAnalyzer(root)
    root.mainloop()
