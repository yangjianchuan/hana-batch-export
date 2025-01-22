import os
from utils import HANAUtils, ExcelExporter

def batch_export(folder_path):
    """批量导出指定文件夹中的所有SQL文件"""
    if not os.path.exists(folder_path):
        print(f"文件夹不存在: {folder_path}")
        return
    
    utils = HANAUtils()
    
    # 遍历文件夹中的所有.sql文件
    for filename in os.listdir(folder_path):
        if not filename.endswith('.sql'):
            continue
            
        # 构建完整文件路径
        sql_file = os.path.join(folder_path, filename)
        
        # 读取SQL内容
        sql = utils.read_sql_from_file(sql_file)
        if not sql:
            continue
            
        # 生成输出文件名（与SQL文件同名，扩展名改为.xlsx）
        output_file = os.path.splitext(sql_file)[0] + '.xlsx'
        
        # 执行导出
        print(f"正在导出: {filename}")
        exporter = ExcelExporter(sql, output_file)
        exporter.export()
        print(f"成功导出: {output_file}\n")

if __name__ == "__main__":
    # 指定要导出的文件夹路径
    export_folder = "批量导出"
    batch_export(export_folder)