# smart_money_monitor

一个本地可运行的 A 股投研监控系统，支持：

- 命令行执行一次或定时执行
- FastAPI 接口服务
- FastAPI 直接托管本地前端页面

本项目定位为投研辅助与信息监控，不接券商接口，不执行自动交易，不输出明确买卖指令。

## 当前推荐运行方式

现在只需要启动一个服务：

```powershell
uvicorn api:app --reload
```

启动后访问：

- 前端首页：`http://127.0.0.1:8000/`
- 前端静态页：`http://127.0.0.1:8000/frontend/index.html`
- Swagger：`http://127.0.0.1:8000/docs`
- ReDoc：`http://127.0.0.1:8000/redoc`

说明：

- 不再需要 `python -m http.server 5500`
- 前端和 API 共用同一个 `8000` 端口
- 前端页面中的请求已改为相对路径，不再依赖跨域

## 项目结构

```text
smart_money_monitor/
├── app.py
├── api.py
├── config.py
├── requirements.txt
├── .env.example
├── README.md
├── frontend/
│   └── index.html
├── data/
│   ├── watchlist.json
│   ├── reports/
│   └── cache/
├── modules/
│   ├── __init__.py
│   ├── fetch_market.py
│   ├── fetch_fund_flow.py
│   ├── analyzer.py
│   ├── ai_summary.py
│   ├── reporter.py
│   ├── notifier.py
│   ├── scheduler_job.py
│   ├── watchlist_service.py
│   ├── report_service.py
│   └── run_service.py
└── logs/
    └── app.log
```

## 安装依赖

### 1. 进入项目目录

```powershell
cd D:\CODEX\smart_money_monitor
```

### 2. 创建虚拟环境

```powershell
py -3.11 -m venv .venv
```

如果你的机器没有 `py -3.11`，可以改用：

```powershell
python -m venv .venv
```

### 3. 激活虚拟环境

```powershell
.venv\Scripts\Activate.ps1
```

如果 PowerShell 限制脚本执行，可先运行：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

### 4. 安装依赖

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 5. 配置环境变量

```powershell
Copy-Item .env.example .env
```

`.env` 示例：

```env
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1-mini
FEISHU_WEBHOOK=
DEFAULT_SCHEDULE_TIME=09:00
```

说明：

- `OPENAI_API_KEY` 可留空，程序会自动使用规则兜底摘要
- `FEISHU_WEBHOOK` 可留空，系统只保存本地报告

## 启动服务

```powershell
uvicorn api:app --reload
```

启动成功后，直接打开：

```text
http://127.0.0.1:8000/
```

## 前端页面说明

前端页面文件位置：

```text
frontend/index.html
```

页面中包含：

- 一个“运行分析”按钮
- 一个结果展示区域

点击按钮后会调用：

```text
/api/run-once
```

并把返回的 JSON 结果直接渲染在页面中。

## API 列表

- `GET /`
- `GET /api/health`
- `GET /api/config`
- `POST /api/config`
- `GET /api/stocks`
- `POST /api/stocks`
- `DELETE /api/stocks/{code}`
- `POST /api/run-once`
- `GET /api/reports`
- `GET /api/reports/latest`
- `GET /api/reports/{filename}`

## 接口测试

### 健康检查

```powershell
curl http://127.0.0.1:8000/api/health
```

### 获取股票列表

```powershell
curl http://127.0.0.1:8000/api/stocks
```

### 执行一次分析

```powershell
curl -X POST http://127.0.0.1:8000/api/run-once
```

## 常见问题排查

### 1. 打开 `/` 为空白或 404

请确认：

- `frontend/index.html` 文件存在
- 你访问的是 `http://127.0.0.1:8000/`
- 启动命令是 `uvicorn api:app --reload`

### 2. 点击“运行分析”出现 `Failed to fetch`

请确认：

- 页面是从 `http://127.0.0.1:8000/` 打开的
- 不是通过其他端口或直接双击 HTML 文件打开
- FastAPI 服务已正常运行

### 3. `/api/run-once` 没有结果

请检查：

- `data/watchlist.json` 是否为空
- akshare 是否能正常访问
- 当前网络是否正常

### 4. `/api/reports/latest` 返回 404

说明还没有生成过报告，请先点击页面里的“运行分析”按钮，或执行：

```powershell
curl -X POST http://127.0.0.1:8000/api/run-once
```

### 5. `uvicorn api:app --reload` 启动失败

请确认：

- 已安装 `fastapi` 和 `uvicorn`
- 当前目录在项目根目录
- 虚拟环境已激活

## 最终访问地址

现在前端页面应该打开这个网址：

```text
http://127.0.0.1:8000/
```
