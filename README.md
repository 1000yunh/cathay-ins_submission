# RIS Scraper System - 內政部戶政司門牌資料爬蟲系統

本專案為內政部戶政司門牌編釘資料的自動化爬蟲系統，包含資料擷取、API 查詢服務、Log 監控與異常通報功能。

---

## 目錄

1. [環境需求](#環境需求)
2. [快速開始](#快速開始)
3. [服務一覽](#服務一覽)
4. [專案結構](#專案結構)
5. [系統架構](#系統架構)
6. [試題1: 爬蟲程式](#試題1-爬蟲程式)
7. [試題2: API 服務](#試題2-api-服務)
8. [試題3: Log 收集 & 異常通報](#試題3-log-收集--異常通報)
9. [試題4: 系統架構圖](#試題4-系統架構圖)
10. [常見問題](#常見問題)

---

## 環境需求

| 需求 | 版本 | 檢查指令 |
|------|------|----------|
| Docker Desktop | 運行中 | `docker --version` |
| Python | 3.10+ | `python3 --version` |
| Chrome | 最新版 | 爬蟲需要 |

---

## 快速開始

### 1. 安裝 Python 套件

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 啟動 Docker 服務

```bash
cd 試題3/docker
docker compose up -d
```

### 3. 確認服務狀態

```bash
docker compose ps
```

預期輸出:
```
NAME           STATUS         PORTS
ris_postgres   Up (healthy)   0.0.0.0:5432->5432/tcp
ris_api        Up             0.0.0.0:8000->8000/tcp
ris_loki       Up             0.0.0.0:3100->3100/tcp
ris_grafana    Up             0.0.0.0:3000->3000/tcp
ris_pgadmin    Up             0.0.0.0:5050->80/tcp
```

### 4. 執行爬蟲

```bash
cd 試題1
python main.py --districts "大安區"
```

### 5. 驗證結果

- **API 查詢**: http://localhost:8000/records?city=臺北市&district=大安區
- **Grafana 日誌**: http://localhost:3000 (admin/admin)
- **pgAdmin 資料庫**: http://localhost:5050 (admin@example.com/admin)

### 6. 停止服務

```bash
cd 試題3/docker
docker compose down
```

---

## 服務一覽

| 服務 | 網址 | 帳號 / 密碼 |
|------|------|-------------|
| **API Docs** | http://localhost:8000/docs | - |
| **Grafana** | http://localhost:3000 | admin / admin |
| **pgAdmin** | http://localhost:5050 | admin@example.com / admin |
| **PostgreSQL** | localhost:5432 | postgres / postgres |
| **Loki** | localhost:3100 | - |

---

## 專案結構

```
├── 試題1/                 # 爬蟲程式
│   ├── scraper/          # 爬蟲模組
│   ├── main.py           # 主程式
│   ├── scheduler.py      # 排程服務
│   └── data/             # CSV 輸出
│
├── 試題2/                 # API 服務
│   ├── api_server.py     # FastAPI
│   └── screenshots/      # 截圖
│
├── 試題3/                 # Log & Alert
│   ├── docker/           # Docker Compose
│   └── screenshots/      # Grafana 截圖
│
├── 試題4/                 # 架構文件
│   └── architecture.md
│
├── sql/schema.sql        # 資料庫 Schema
├── requirements.txt      # Python 套件
└── .env.example          # 環境變數範本
```

---

## 系統架構

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  RIS 網站   │────▶│   Scraper   │────▶│ PostgreSQL  │
│  (戶政司)   │     │  (Selenium) │     │  Database   │
└─────────────┘     └─────────────┘     └─────────────┘
                          │                    │
                          ▼                    ▼
                    ┌─────────────┐     ┌─────────────┐
                    │    Loki     │     │   FastAPI   │
                    │   (Logs)    │     │    (API)    │
                    └─────────────┘     └─────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │   Grafana   │───▶ Email Alert
                    │ (Dashboard) │
                    └─────────────┘
```

### 功能特色

| 功能 | 技術 | 說明 |
|------|------|------|
| 網頁爬蟲 | Selenium | 自動化爬取戶政司網站 |
| 驗證碼辨識 | ddddocr | 自動辨識驗證碼 |
| 資料庫 | PostgreSQL | 儲存門牌資料 |
| REST API | FastAPI | 查詢門牌資料 |
| 日誌監控 | Grafana + Loki | 即時日誌視覺化 |
| 異常告警 | Grafana Alert | Email 通知 |
| 排程自動化 | APScheduler | 定時執行爬蟲 |
| 容器化 | Docker Compose | 一鍵啟動所有服務 |

---

## 試題1: 爬蟲程式

### 功能說明

- 爬取內政部戶政司網站 (https://www.ris.gov.tw/app/portal/3053)
- 使用「以編釘日期、編釘類別查詢」條件擷取資料
- 自動辨識驗證碼 (ddddocr)
- 資料清理與結構化解析
- 輸出 CSV 檔案並寫入資料庫

### 執行方式

```bash
cd 試題1

# 爬取單一行政區
python main.py --districts "大安區"

# 爬取多個行政區
python main.py --districts "大安區,中正區,信義區"

# 爬取全部行政區
python main.py --all-districts
```

### 參數說明

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `--city` | 縣市名稱 | 臺北市 |
| `--districts` | 行政區 (逗號分隔) | 大安區 |
| `--all-districts` | 爬取所有行政區 | - |
| `--start-date` | 起始日期 (民國年) | 114-09-01 |
| `--end-date` | 結束日期 (民國年) | 114-11-30 |
| `--register-type` | 編釘類別 | 門牌初編 |

### 輸出檔案

| 檔案 | 說明 |
|------|------|
| `data/raw_addresses_*.csv` | 原始資料 |
| `data/cleaned_addresses_*.csv` | 清理後資料 |

### CSV 欄位

| 欄位 | 說明 | 範例 |
|------|------|------|
| city | 縣市 | 臺北市 |
| district | 行政區 | 大安區 |
| village | 里 | 富台里 |
| neighborhood | 鄰 | 19 |
| road | 路/街 | 信義路 |
| section | 段 | 四段 |
| lane | 巷 | 100 |
| alley | 弄 | 5 |
| number | 號 | 10 |
| floor | 樓 | 3 |
| floor_dash | 之 | 1 |
| assignment_date | 編釘日期 | 2025-09-15 |
| assignment_type | 編釘類別 | 門牌初編 |

### 自動化排程

```bash
cd 試題1

# 前景執行 (測試)
python3 scheduler.py

# 背景執行 (生產)
nohup python3 scheduler.py > logs/scheduler.log 2>&1 &
```

環境變數設定 (`.env`):
```env
SCHEDULER_ENABLED=true
SCHEDULER_CRON=0 2 * * 1    # 每週一凌晨 2:00
SCHEDULER_TIMEZONE=Asia/Taipei
```

### 技術選型: 為何選擇 Selenium

1. **動態網頁**: 戶政司網站使用 JavaScript 動態載入
2. **表單互動**: 需選擇縣市、行政區、填寫日期
3. **驗證碼處理**: 需擷取驗證碼圖片進行 OCR
4. **分頁處理**: 搜尋結果多頁，需模擬點擊換頁

---

## 試題2: API 服務

### 功能說明

提供 RESTful API 查詢爬取的門牌資料。

### API Endpoints

| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/` | 健康檢查 |
| GET | `/records` | 查詢門牌記錄 |
| GET | `/records/{id}` | 取得單筆記錄 |
| GET | `/stats` | 統計資訊 |
| GET | `/alerts` | 告警歷史 |
| GET | `/docs` | Swagger UI |

### 查詢參數

| 參數 | 型別 | 說明 |
|------|------|------|
| `city` | string | 縣市 (臺北市) |
| `district` | string | 行政區 (大安區) |
| `start_date` | string | 起始日期 (2025-01-01) |
| `end_date` | string | 結束日期 (2025-12-31) |
| `page` | int | 頁碼 (預設 1) |
| `page_size` | int | 每頁筆數 (預設 50, 最大 100) |

### 使用範例

```bash
# 瀏覽器
open "http://localhost:8000/records?city=臺北市&district=大安區"

# cURL
curl "http://localhost:8000/records?city=臺北市&district=大安區"
```

Response:
```json
{
  "total": 150,
  "page": 1,
  "page_size": 50,
  "records": [
    {
      "id": 1,
      "city": "臺北市",
      "district": "大安區",
      "full_address": "臺北市大安區富台里19鄰信義路四段100巷5弄10號3樓之1",
      "assignment_date": "2025-09-15",
      "assignment_type": "門牌初編"
    }
  ]
}
```

---

## 試題3: Log 收集 & 異常通報

### Grafana Dashboard

系統已自動載入 **RIS Scraper Logs** Dashboard:

| Panel | 說明 |
|-------|------|
| Scraper Logs (24h) | 爬蟲 Log 數量 |
| API Logs (24h) | API Log 數量 |
| Warnings (24h) | 警告數量 |
| Errors (24h) | 錯誤數量 |
| Live Logs | 即時 Log |

**存取**: http://localhost:3000 → Dashboards → RIS Scraper Logs

### Log 查詢 (Explore)

```logql
{job="scraper"}              # 爬蟲 Log
{job="api"}                  # API Log
{job="scraper"} |= "ERROR"   # 錯誤 Log
```

> **注意**: 需先執行爬蟲程式才會有 Log 資料

### Email 告警

| 告警 | 觸發條件 | 嚴重度 |
|------|----------|--------|
| Scraper Error | 爬蟲出現 ERROR | error |
| CAPTCHA Failed | 驗證碼失敗 | warning |
| API Error | API 出現 ERROR | error |

### Email 設定

編輯 `試題3/docker/.env`:

```env
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_FROM=your_email@gmail.com
```

> Gmail 需使用 App Password: https://myaccount.google.com/apppasswords

---

## 試題4: 系統架構圖

詳見 [`試題4/architecture.md`](試題4/architecture.md)

---

## 常見問題

### Port 被佔用

```bash
lsof -i :5432
lsof -i :3000
brew services stop postgresql@15  # macOS
```

### 驗證碼辨識失敗

系統會自動重試 (最多 5 次)。確認安裝: `pip show ddddocr`

### 資料庫連線失敗

確認 Docker 服務已啟動: `docker compose ps`

### Grafana 看不到 Log

1. 確認 Loki 運行中: `docker ps | grep loki`
2. 執行爬蟲後等待 1-2 分鐘

### Email 告警沒收到

1. 確認 SMTP 設定
2. 使用 Gmail 應用程式密碼
3. Grafana → Alerting → Contact points → Test

### pgAdmin 連線

首次登入需輸入密碼 `postgres`，勾選 Save Password
