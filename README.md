# HANA 查询与导出工具

这是一个用于从 SAP HANA 数据库查询数据并导出到 Excel 的 Python 工具集。

## 功能特性

- 支持批量执行 SQL 查询
- 支持将查询结果导出到 Excel 文件
- 支持分页导出大数据量
- 支持环境变量配置数据库连接

## 使用说明

1. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

2. 配置数据库连接：
   - 复制 `.env.example` 为 `.env`
   - 编辑 `.env` 文件配置数据库连接信息

3. 脚本使用方法：

   ### batch_export_sql_to_excel.py
   - 功能：批量执行多个 SQL 查询，每个查询生成单独的 Excel 文件
   - 使用方法：
     1. 将所有 SQL 文件放在 `批量导出/` 目录下
     2. 运行命令：
        ```bash
        python batch_export_sql_to_excel.py
        ```
     3. 输出文件：每个 SQL 文件生成对应的 Excel 文件，保存在 `批量导出/` 目录下

   ### batch_export_sql_to_one_excel.py
   - 功能：批量执行多个 SQL 查询，将所有结果合并到一个 Excel 文件的不同 sheet 中
   - 使用方法：
     1. 将所有 SQL 文件放在 `批量导出/` 目录下
     2. 运行命令：
        ```bash
        python batch_export_to_one_excel.py
        ```
     3. 输出文件：`批量导出/output_YYYYMMDD_HHMMSS.xlsx`，每个查询结果在单独的 sheet 中

4. 查看导出结果：
   - 所有导出的 Excel 文件都保存在 `批量导出/` 目录下

## 环境变量说明

### 数据库连接配置
- `HANA_HOST`: HANA数据库主机地址
- `HANA_PORT`: HANA数据库端口号
- `HANA_USER`: HANA数据库用户名
- `HANA_PASSWORD`: HANA数据库密码

### 分页配置
- `PAGE_SIZE`: 分页导出时每页的记录数，默认2000

### 导出文件配置
- `FILE_PREFIX`: 导出文件前缀，默认"output"
- `FILE_EXTENSION`: 导出文件扩展名，默认"xlsx"

### 列宽配置
- `COLUMN_WIDTH`: Excel列宽，默认20

### 表头样式配置
- `HEADER_FONT_COLOR`: 表头字体颜色，默认"#50596d"
- `HEADER_FONT_SIZE`: 表头字体大小，默认9
- `HEADER_BG_COLOR`: 表头背景颜色，默认"#f5f7f8"
- `HEADER_BORDER_COLOR`: 表头边框颜色，默认"#e0e4e6"

### 正文样式配置
- `BODY_FONT_COLOR`: 正文字体颜色，默认"#50596d"
- `BODY_FONT_SIZE`: 正文字体大小，默认9
- `BODY_BG_COLOR`: 正文背景颜色，默认"#ffffff"
- `BODY_BORDER_COLOR`: 正文边框颜色，默认"#f4f4f8"
- `FONT_NAME`: 导出文件使用的字体，默认"Arial"

### 其他配置
- `FREEZE_PANES`: 是否冻结首行，默认"True"，可设置为"False"禁用

## 文件说明

- `utils.py`: 工具函数
- `.env`: 环境变量配置文件
- `.env.example`: 环境变量配置示例
- `batch_export_sql_to_excel.py`: 批量导出脚本
- `batch_export_sql_to_one_excel.py`: 批量导出到单个 Excel 文件
- `requirements.txt`: 依赖包列表
- `批量导出/`: 包含示例 SQL 和导出的 Excel 文件

## 分页导出原理

1. 工具会先执行COUNT(*)查询获取总记录数
2. 根据PAGE_SIZE设置的分页大小，将查询结果分成多个批次
3. 每个批次使用LIMIT和OFFSET进行分页查询
4. 将每个批次的结果追加到Excel文件中
5. 重复上述过程直到所有数据导出完成

要修改每页条数：
1. 编辑.env文件中的PAGE_SIZE值
2. 或者在使用工具时传入page_size参数

## 注意事项

- 确保数据库连接信息正确
- 大数据量查询建议使用分页导出
- 导出前请确认目标目录有足够空间
- SQL查询文件需要是.sql结尾的文件
- 由于使用了分页导出，SQL语句中需要写ORDER BY，并且字段要是一个唯一值
