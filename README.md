# HANA 查询与导出工具

这是一个用于从 SAP HANA 数据库查询数据并导出到 Excel 的 Python 工具集，提供命令行和图形界面两种使用方式。

## Release 使用说明

### 下载和安装
1. 访问项目的 [Releases](../../releases) 页面
2. 下载最新版本的 `hana-batch-export.zip`
3. 解压 zip 包，你会得到：
   - `hana-batch-export.exe`和`hana_query_analyzer.exe`: 主程序
   - `.env.example`: 配置文件模板

### 配置和使用
1. 复制 `.env.example` 为 `.env`
2. 编辑 `.env` 文件，配置你的数据库连接信息
3. 运行 `hana-batch-export.exe`或者`hana_query_analyzer.exe` 启动程序

### 如何发布新版本
如果你是项目维护者，要发布新版本：
1. 确保代码变更已提交到主分支
2. 创建新的 tag（例如 `v1.0.1`）：
   ```bash
   git tag v1.0.1
   git push origin v1.0.1
   ```
3. Github Actions 会自动：
   - 编译最新代码生成 exe 文件
   - 将 exe 和配置模板打包成 zip
   - 创建新的 Release 并上传 zip 包

## 功能特性

- 支持批量执行 SQL 查询
- 支持将查询结果导出到 Excel 文件
- 支持三种数据导出方式：
  - 流式导出 (F12)：直接从数据库游标流式读取数据
  - 分页导出 (Ctrl+F12)：使用分页方式批量导出数据
  - 直接导出 (Shift+F12)：一次性导出全部数据
- 支持环境变量配置数据库连接
- 提供图形化界面(GUI)操作
  - 支持SQL文件上传和直接输入两种模式
  - 实时进度显示
  - 数据库连接测试
  - 导出完成后自动打开输出目录
  - 支持多标签页查询
  - 支持SQL语法高亮
  - 支持执行选中SQL或全部SQL
  - 支持查询结果导出为Excel
  - 支持SQL脚本的保存和加载
  - 支持查询日志记录
  - 支持丰富的快捷键操作：
    - F12：流式导出数据
    - Ctrl+F12：分页导出数据
    - F8：执行全部SQL
    - Ctrl+F8：执行选中SQL
    - Ctrl+N：新增查询窗口
    - Ctrl+S：保存SQL
    - Ctrl+O：加载SQL
    - Ctrl+D：清空查询语句
    - Ctrl+W：关闭标签页
    - Esc：终止查询

## 使用说明

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置数据库连接
- 复制 `.env.example` 为 `.env`
- 编辑 `.env` 文件配置数据库连接信息

### 3. 使用方式

### 图形界面模式

#### main.py
1. 启动GUI：
   ```bash
   python main.py
   ```

2. 界面说明：
   - 数据库配置：输入HANA数据库连接信息
   - SQL配置：
     - 上传SQL文件：选择本地SQL文件
     - 直接输入SQL：在文本框中输入SQL语句
   - 导出配置：
     - 设置分页大小（默认2000）
     - 点击"导出到Excel"开始导出
   - SQL语法高亮：实时显示SQL语法高亮
   
3. 导出过程：
   - 进度条显示当前导出进度

#### hana_query_analyzer.py
1. 启动GUI：
   ```bash
   python hana_query_analyzer.py
   ```

2. 主要功能：
   - 查询分析：执行SQL查询
   - 查询统计：统计查询执行时间、资源消耗等指标
   
3. 界面说明：
   - 查询输入：输入或上传SQL查询
   - 分析结果：显示查询结果

## 环境变量说明

### 数据库连接配置
- `HANA_HOST`: HANA数据库主机地址
- `HANA_PORT`: HANA数据库端口号
- `HANA_USER`: HANA数据库用户名
- `HANA_PASSWORD`: HANA数据库密码

### 分页配置
- `PAGE_SIZE`: 分页导出时每页的记录数，默认10000

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

- `main.py`: 图形界面主程序
- `utils.py`: 工具函数
- `.env`: 环境变量配置文件
- `.env.example`: 环境变量配置示例
- `batch_export_sql_to_excel.py`: 批量导出脚本
- `batch_export_sql_to_one_excel.py`: 批量导出到单个 Excel 文件
- `requirements.txt`: 依赖包列表
- `批量导出/`: 包含示例 SQL 和导出的 Excel 文件

## 数据导出功能说明

### 流式导出 (F12)
- 原理：使用数据库游标直接流式读取数据，一次查询持续获取数据直到结束
- 特点：
  - 在同一个查询会话中完成数据获取
  - 内存占用相对稳定，适合导出大量数据
  - 即使原始SQL没有ORDER BY也不会出现数据重复或丢失
  - 读取过程中保持数据库连接
- 适用场景：
  - 大数据量导出
  - 对数据完整性要求高的场景
  - 不需要对数据进行排序的场景
  - 只能导出SELECT 开头的语句

### 分页导出 (Ctrl+F12)

1. 工具会先执行COUNT(*)查询获取总记录数
2. 根据PAGE_SIZE设置的分页大小，将查询结果分成多个批次
3. 每个批次使用LIMIT和OFFSET进行分页查询
4. 将每个批次的结果追加到Excel文件中
5. 重复上述过程直到所有数据导出完成
- 适用场景：
  - 大数据量导出
  - 只能导出SELECT 开头的语句
特点：
- 如果原始SQL没有ORDER BY子句，工具会自动添加所有字段作为排序条件
- 分页过程中会在日志区域显示实际执行的SQL语句
- 可以通过环境变量PAGE_SIZE调整每页记录数
- 支持两种方式修改每页条数：
  1. 编辑.env文件中的PAGE_SIZE值
  2. 或者在使用工具时传入page_size参数

### 直接导出 (Shift+F12)

1. 不进行COUNT(*)查询，直接执行原始SQL
2. 一次性将所有查询结果读取到内存
3. 将完整结果集写入Excel文件
4. 适合处理中小规模数据集
5. 特点：
   - 执行简单快速
   - 内存占用较高
   - 不适合处理超大规模数据集
   - 不会因为缺少ORDER BY而影响结果
   - 支持导出WITH 或者 DO BEGIN开头的语句
   
## 注意事项

- 确保数据库连接信息正确
- 大数据量查询建议使用分页导出
- 导出前请确认目标目录有足够空间
- SQL查询文件需要是.sql结尾的文件
- 使用分页导出时：
  - 如果SQL中已有ORDER BY子句，会保持原样执行
  - 如果没有ORDER BY子句，系统会自动添加所有字段作为排序条件
- 使用流式导出时不要求SQL中包含ORDER BY子句
- GUI模式下，导出完成后会自动打开输出目录
