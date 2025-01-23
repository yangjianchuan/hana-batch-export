import os
import pandas as pd
from datetime import datetime
from utils import HANAUtils, ExcelExporter

def batch_export_to_one_excel(folder_path):
    """将指定文件夹中的所有SQL文件导出到同一个Excel文件的不同sheet页"""
    if not os.path.exists(folder_path):
        print(f"文件夹不存在: {folder_path}")
        return

    utils = HANAUtils()

    # 获取所有.sql文件并按名称排序
    sql_files = sorted(
        [f for f in os.listdir(folder_path) if f.endswith('.sql')],
        key=lambda x: x.lower()
    )
    
    if not sql_files:
        print("未找到任何.sql文件")
        return

    # 生成带时间戳的输出文件名，放在同一文件夹
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(folder_path, f"output_{timestamp}.xlsx")

    # 初始化Excel写入器
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
    workbook = writer.book

    # 遍历所有.sql文件
    for filename in sql_files:
        # 构建完整文件路径
        sql_file = os.path.join(folder_path, filename)
        
        # 读取SQL内容
        sql = utils.read_sql_from_file(sql_file)
        if not sql:
            print(f"跳过空文件: {filename}")
            continue
            
        # 创建sheet页，使用文件名（去掉扩展名）作为sheet名称
        sheet_name = os.path.splitext(filename)[0]
        print(f"正在导出: {filename} -> {sheet_name}")

        # 使用ExcelExporter执行导出
        try:
            exporter = ExcelExporter(sql, output_file)
            exporter.init_excel_writer(writer, sheet_name)
            
            cursor = exporter.connect()
            exporter.get_total_records(cursor)
            
            while exporter.current_offset < exporter.total_records:
                exporter.export_page(cursor)
                
            print(f"成功导出: {sheet_name}")
            
        except Exception as e:
            print(f"导出失败: {e}")
        finally:
            # 只关闭数据库连接，不关闭writer
            exporter.utils.disconnect()

    try:
        # 保存并关闭Excel文件
        if writer:
            writer.close()
            print(f"所有文件已成功导出到 {output_file}")
    except Exception as e:
        print(f"保存Excel文件时出错: {e}")

if __name__ == "__main__":
    # 指定要导出的文件夹路径
    export_folder = "批量导出"
    batch_export_to_one_excel(export_folder)