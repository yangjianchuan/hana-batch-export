import os
from datetime import datetime
from hdbcli import dbapi
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

class HANAUtils:
    """HANA数据库工具类"""
    
    def __init__(self):
        # 从环境变量中读取连接信息
        self.host = os.getenv("HANA_HOST")
        self.port = int(os.getenv("HANA_PORT", 30041))
        self.user = os.getenv("HANA_USER")
        self.password = os.getenv("HANA_PASSWORD")
        self.connection = None

    def connect(self):
        """连接到HANA数据库"""
        try:
            self.connection = dbapi.connect(
                address=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            print("成功连接到HANA数据库。")
        except Exception as e:
            print(f"连接失败: {e}")

    def disconnect(self):
        """断开HANA数据库连接"""
        if self.connection:
            self.connection.close()
            print("已断开HANA数据库连接。")

    def get_cursor(self):
        """获取数据库游标"""
        if self.connection:
            return self.connection.cursor()
        else:
            raise Exception("未连接到数据库，请先调用connect()方法。")

    def execute_query(self, query):
        """执行SQL查询并返回结果"""
        try:
            self.connect()
            cursor = self.get_cursor()
            cleaned_query = self._clean_query(query)
            cursor.execute(cleaned_query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"查询执行失败: {e}")
            return None
        finally:
            self.disconnect()

    def _clean_query(self, query):
        """清理SQL语句，去除末尾分号"""
        if query and query.strip().endswith(';'):
            return query.rstrip(';')
        return query

    @staticmethod
    def read_sql_from_file(file_path):
        """从文件中读取SQL语句"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql = file.read().strip()
                if sql and sql.endswith(';'):
                    sql = sql.rstrip(';')
                return sql
        except Exception as e:
            print(f"读取SQL文件失败: {e}")
            return None

    @staticmethod
    def generate_timestamp_filename(prefix=None, extension=None):
        """生成带时间戳的文件名"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_prefix = os.getenv("FILE_PREFIX", "output") if prefix is None else prefix
        file_extension = os.getenv("FILE_EXTENSION", "xlsx") if extension is None else extension
        return f"{file_prefix}_{timestamp}.{file_extension}"

class ExcelExporter:
    """Excel导出工具类"""
    
    def __init__(self, sql_query, output_file, page_size=None):
        """初始化导出器"""
        self.sql_query = sql_query
        self.output_file = output_file
        self.page_size = int(os.getenv("PAGE_SIZE", 2000)) if page_size is None else page_size
        self.utils = HANAUtils()
        self.writer = None
        self.total_records = 0
        self.current_offset = 0
        self.page_number = 1

    def connect(self):
        """建立数据库连接"""
        self.utils.connect()
        return self.utils.get_cursor()

    def get_total_records(self, cursor):
        """获取总记录数"""
        count_query = f"SELECT COUNT(*) FROM ({self.sql_query})"
        cursor.execute(count_query)
        self.total_records = cursor.fetchone()[0]
        return self.total_records

    def init_excel_writer(self, writer=None, sheet_name='Data'):
        """初始化Excel写入器"""
        if writer:
            self.writer = writer
        else:
            self.writer = pd.ExcelWriter(self.output_file, engine='xlsxwriter')
            
        self.workbook = self.writer.book
        self.worksheet = None
        self.header_written = False
        self.start_row = 0
        self.sheet_name = sheet_name
        
        # 定义表头格式（包含居中对齐）
        self.header_format = self.workbook.add_format({
            'font_color': os.getenv("HEADER_FONT_COLOR", '#50596d'),
            'font_size': int(os.getenv("HEADER_FONT_SIZE", 9)),
            'bg_color': os.getenv("HEADER_BG_COLOR", '#f5f7f8'),
            'border': 1,
            'border_color': os.getenv("HEADER_BORDER_COLOR", '#e0e4e6'),
            'font_name': os.getenv("FONT_NAME", "Arial"),
            'align': 'center',
            'valign': 'vcenter'
        })

        # 定义表格正文格式（包含居中对齐）
        self.body_format = self.workbook.add_format({
            'font_color': os.getenv("BODY_FONT_COLOR", '#50596d'),
            'font_size': int(os.getenv("BODY_FONT_SIZE", 9)),
            'bg_color': os.getenv("BODY_BG_COLOR", '#ffffff'),
            'border': 1,
            'border_color': os.getenv("BODY_BORDER_COLOR", '#f4f4f8'),
            'font_name': os.getenv("FONT_NAME", "Arial"),
            'align': 'center',
            'valign': 'vcenter'
        })

        # 定义居中对齐和边框格式
        self.center_format = self.workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'border_color': os.getenv("BODY_BORDER_COLOR", '#f4f4f8'),
            'font_name': os.getenv("FONT_NAME", "Arial")
        })

    def export_page(self, cursor):
        """导出单个分页"""
        paginated_query = f"{self.sql_query} LIMIT {self.page_size} OFFSET {self.current_offset}"
        cursor.execute(paginated_query)
        
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        
        # 转换数值字段
        for col in df.columns:
            try:
                # 尝试将字段转换为数值类型
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                # 如果转换失败，保持原值不变
                continue
                
        if not self.header_written:
            # 写入表头
            df.to_excel(self.writer, sheet_name=self.sheet_name, index=False, startrow=self.start_row)
            self.worksheet = self.writer.sheets[self.sheet_name]
            
            # 应用表头格式到所有列
            for col_num, value in enumerate(df.columns):
                self.worksheet.write(0, col_num, value, self.header_format)
            self.header_written = True
            self.start_row += 1  # 跳过表头行
        else:
            # 追加数据
            df.to_excel(self.writer, sheet_name=self.sheet_name, index=False, header=False, startrow=self.start_row)
        
        # 应用正文格式
        (num_rows, num_cols) = df.shape
        for row in range(self.start_row, self.start_row + num_rows):
            self.worksheet.set_row(row, None, self.body_format)
        
        # 设置列宽
        column_width = int(os.getenv("COLUMN_WIDTH", 20))
        self.worksheet.set_column(0, num_cols - 1, column_width, self.center_format)
        
        # 冻结首行
        if os.getenv("FREEZE_PANES", "True").lower() == "true":
            self.worksheet.freeze_panes(1, 0)
        
        processed = min(self.current_offset + self.page_size, self.total_records)
        print(f"已导出 {processed}/{self.total_records} 条记录 ({processed/self.total_records:.1%})")
        
        self.start_row += num_rows
        self.current_offset += self.page_size

    def close(self):
        """关闭资源"""
        if self.writer:
            self.writer.close()
        self.utils.disconnect()

    def export(self):
        """执行导出流程"""
        try:
            cursor = self.connect()
            self.get_total_records(cursor)
            self.init_excel_writer()
            
            while self.current_offset < self.total_records:
                self.export_page(cursor)
            
            print(f"成功导出数据到 {self.output_file}")
            
        except Exception as e:
            print(f"导出失败: {e}")
        finally:
            self.close()