# 金融工程项目

该项目是一个综合性的金融工程平台，集成了股票数据采集、分析处理和智能对话功能。

## 项目结构

- `input_data/`: 股票和ETF数据导入和处理
- `stock_select_strategy/`: 股票选择策略分析
- `web/`: Web API接口和服务
  - `stock_chat_service.py`: 股票聊天核心服务
  - `stock_chat_api.py`: 股票聊天API入口
- `services/`: 核心服务类
  - `app_factory.py`: 应用工厂，用于创建标准化FastAPI应用
  - `chat_api_service.py`: 聊天API服务，处理聊天相关路由
  - `data_api_service.py`: 数据API服务，处理数据处理相关路由
  - `data_processing_service.py`: 数据处理服务
  - `scheduler_service.py`: 定时任务调度服务
- `main.py`: 项目统一入口文件

## 架构设计

项目采用了模块化、服务化的架构设计：

1. **服务层**: 每个功能点都被封装为独立的服务类，提供特定功能
2. **API层**: 通过API服务类将服务层的功能暴露为Web API
3. **应用层**: 使用应用工厂统一创建和配置Web应用
4. **入口层**: 提供统一和独立的应用入口

## 主要功能

1. **股票数据处理**:
   - 导入股票和ETF历史数据
   - 计算均线
   - 检查成交量增加情况
   - 分析涨停和概念关系

2. **股票聊天API**:
   - 智能对话功能
   - 对话历史管理
   - 股票数据查询

3. **定时任务调度**:
   - 自动执行每日数据处理
   - 可配置执行时间
   - 支持立即执行

## 使用方法

### 安装依赖

```bash
pip install -r requirements.txt
```

### 环境变量配置

在项目根目录创建`.env`文件，包含以下内容：

```
DEEPSEEK_API_KEY=your_api_key_here
DEEPSEEK_API_BASE=https://api.deepseek.com/v1
```

### 启动服务

#### 启动完整服务（包含股票聊天和数据处理）

```bash
cd program/python/com/caicongyang/financial/engineering
python main.py
```

#### 仅启动股票聊天服务

```bash
cd program/python/com/caicongyang/financial/engineering/web
python stock_chat_api.py
```

启动完整服务后，服务将：
1. 运行FastAPI Web服务，监听在`http://0.0.0.0:8000`
2. 在后台启动每日数据处理定时任务（18:30自动执行）

## API接口

### 聊天功能

- `POST /conversations`: 创建新对话
- `GET /conversations`: 获取对话列表
- `DELETE /conversations/{conversation_id}`: 删除对话
- `POST /conversations/{conversation_id}/messages`: 发送消息
- `GET /conversations/{conversation_id}/history`: 获取对话历史

### 数据处理功能

- `POST /process-daily-data/{date}`: 手动触发指定日期的数据处理
- `GET /health`: 健康检查

### 调度器功能

- `POST /scheduler/run-now`: 立即执行一次定时任务
- `POST /scheduler/set-time`: 设置定时任务执行时间

## 定时任务

系统默认每天18:30自动执行数据处理任务，包括：
1. 导入股票和ETF历史数据
2. 检查股票和ETF成交量
3. 检查涨停情况
4. 分析概念关系

## 示例

### 手动触发数据处理

```bash
curl -X POST "http://localhost:8000/process-daily-data/2023-04-22"
```

### 创建对话并发送消息

```bash
# 创建对话
curl -X POST "http://localhost:8000/conversations" -H "Content-Type: application/json"

# 发送消息
curl -X POST "http://localhost:8000/conversations/{conversation_id}/messages" \
  -H "Content-Type: application/json" \
  -d '{"message": "请分析贵州茅台最近的趋势"}'
```

### 调度器操作

```bash
# 立即执行一次任务
curl -X POST "http://localhost:8000/scheduler/run-now"

# 修改定时任务执行时间
curl -X POST "http://localhost:8000/scheduler/set-time" \
  -H "Content-Type: application/json" \
  -d '{"time": "20:00"}'
``` 