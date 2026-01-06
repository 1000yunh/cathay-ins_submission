# RIS Scraper System - 內政部戶政司門牌資料爬蟲系統

本專案為內政部戶政司門牌編釘資料的自動化爬蟲系統，包含資料擷取、API 查詢服務、Log 監控與異常通報功能。

## 專案結構

```
├── 試題1/                 # 爬蟲程式
│   ├── scraper/          # 爬蟲模組
│   ├── main.py           # 主程式入口
│   ├── data_processing.py # 資料清理與解析
│   ├── scheduler.py      # 自動化排程
│   └── data/             # CSV 輸出範例
│
├── 試題2/                 # API 服務
│   ├── api_server.py     # FastAPI 伺服器
│   └── screenshots/      # API 執行截圖
│
├── 試題3/                 # Log 收集 & 異常通報
│   ├── docker/           # Docker Compose 配置
│   ├── loki_logger.py    # Loki 日誌模組
│   ├── alert_service.py  # 異常通報服務
│   └── screenshots/      # Grafana 截圖
│
├── 試題4/                 # 系統架構圖
│   └── architecture.md   # 架構說明文件
│
├── sql/                   # 資料庫 Schema
├── requirements.txt       # Python 套件
├── .env.example          # 環境變數範本
└── README.md             # 本文件
```

---

## 環境需求

- Python 3.10+
- PostgreSQL 15+
- Docker & Docker Compose
- Google Chrome (for Selenium)

---

## 快速開始

### 1. 複製專案並安裝套件

```bash
# 建立虛擬環境
python3 -m venv venv
source venv/bin/activate

# 安裝套件
pip install -r requirements.txt
```

### 2. 設定環境變數

```bash
cp .env.example .env
# 編輯 .env 設定資料庫連線等參數
```

### 3. 啟動 Docker 服務 (PostgreSQL, Grafana, Loki)

```bash
cd 試題3/docker
docker compose up -d
```

### 4. 初始化資料庫

```bash
# 連線到 PostgreSQL 並執行 schema (密碼: postgres)
PGPASSWORD=postgres psql -h localhost -U postgres -d ris_scraper -f sql/schema.sql
```

### 5. 開啟服務

| 服務 | 網址 | 帳號 / 密碼 |
|------|------|-------------|
| **Grafana** (日誌監控) | http://localhost:3000 | admin / admin |
| **API Docs** (Swagger UI) | http://localhost:8000/docs | - |
| **pgAdmin** (資料庫管理) | http://localhost:5050 | admin@example.com / admin |
| **PostgreSQL** | localhost:5432 | postgres / postgres |

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
python main.py --districts "大安區" --start-date "114-09-01" --end-date "114-11-30"

# 爬取多個行政區
python main.py --districts "大安區,中正區,信義區"

# 爬取台北市所有行政區
python main.py --all-districts

# 查看可用行政區
python main.py --fetch-districts
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

- `data/raw_addresses_YYYYMMDD_HHMMSS.csv` - 原始資料
- `data/cleaned_addresses_YYYYMMDD_HHMMSS.csv` - 清理後資料

### CSV 欄位說明

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

### 技術選型: 為何選擇 Selenium

1. **動態網頁**: 戶政司網站使用 JavaScript 動態載入內容，需要瀏覽器渲染
2. **表單互動**: 需要選擇縣市、行政區、填寫日期等表單操作
3. **驗證碼處理**: 需要擷取驗證碼圖片進行 OCR 辨識
4. **分頁處理**: 搜尋結果可能有多頁，需要模擬點擊換頁

### 異常處理

- 網路請求失敗: 自動重試 3 次
- 驗證碼辨識失敗: 自動刷新驗證碼重試
- 無資料: 記錄 Log 並繼續下一區域
- 網站結構變更: 記錄詳細錯誤訊息至 Log

---

## 試題2: API 服務

### 功能說明

提供 RESTful API 查詢爬取的門牌資料。

### 啟動服務

```bash
cd 試題2
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

### API 文件

啟動後訪問: http://localhost:8000/docs (Swagger UI)

### 查詢 API

**Endpoint**: `GET /records`

**Request**:
```json
{
  "city": "臺北市",
  "district": "大安區"
}
```

**瀏覽器測試**:
```bash
open "http://localhost:8000/records?city=臺北市&district=大安區"
```

**cURL 範例**:
```bash
curl "http://localhost:8000/records?city=臺北市&district=大安區"
```

**Response**:
```json
{
  "success": true,
  "count": 150,
  "data": [
    {
      "id": 1,
      "city": "臺北市",
      "district": "大安區",
      "full_address": "臺北市大安區富台里19鄰信義路四段100巷5弄10號3樓之1",
      "village": "富台里",
      "neighborhood": "19",
      "road": "信義路",
      "section": "四段",
      "lane": "100",
      "alley": "5",
      "number": "10",
      "floor": "3",
      "floor_dash": "1",
      "assignment_date": "2025-09-15",
      "assignment_type": "門牌初編"
    }
  ]
}
```

### 健康檢查

```bash
curl http://localhost:8000/
```

---

## 試題3: Log 收集 & 異常通報

### 架構說明

- **Loki**: 日誌收集與儲存
- **Grafana**: 日誌查詢與視覺化
- **Email Alert**: 異常狀況 Email 通知

### 啟動服務

```bash
cd 試題3/docker

# 設定環境變數 (SMTP 等)
cp .env.example .env
# 編輯 .env 填入 Gmail App Password (選填，用於 Email 告警)

docker compose up -d
```

### 確認服務狀態

```bash
docker compose ps
```

**預期輸出**:
```
NAME           STATUS         PORTS
ris_postgres   Up (healthy)   0.0.0.0:5432->5432/tcp
ris_api        Up             0.0.0.0:8000->8000/tcp
ris_loki       Up             0.0.0.0:3100->3100/tcp
ris_grafana    Up             0.0.0.0:3000->3000/tcp
ris_pgadmin    Up             0.0.0.0:5050->80/tcp
```

### 服務端口

| 服務 | 端口 | 用途 |
|------|------|------|
| PostgreSQL | 5432 | 資料庫 |
| API | 8000 | FastAPI 服務 |
| Grafana | 3000 | 監控面板 |
| Loki | 3100 | 日誌收集 |
| pgAdmin | 5050 | 資料庫管理 |

### Grafana 登入

- URL: http://localhost:3000
- 帳號: admin
- 密碼: admin

> Loki Data Source 已自動設定，無需手動配置

### 預設 Dashboard

系統已自動載入 **RIS Scraper Logs** Dashboard，包含:

| Panel | 說明 |
|-------|------|
| Scraper Logs (24h) | 爬蟲 Log 數量統計 |
| API Logs (24h) | API Log 數量統計 |
| Warnings (24h) | 警告數量 (黃色警示) |
| Errors (24h) | 錯誤數量 (紅色警示) |
| Scraper Logs (Live) | 爬蟲即時 Log |
| API Logs (Live) | API 即時 Log |
| Errors & Warnings | 錯誤與警告彙整 |

**存取方式**:
1. 登入 Grafana (admin / admin)
2. 左側選單 → **Dashboards**
3. 點選 **RIS Scraper Logs**

### Log 查詢 (Explore)

若需自訂查詢，可使用 Explore:

1. 登入 Grafana (admin / admin)
2. 左側選單 → **Explore**
3. 上方選擇 Data Source: **Loki**
4. 輸入查詢語法，點擊 **Run query**:

```logql
# 查詢爬蟲 Log
{job="scraper"}

# 查詢 API Log
{job="api"}

# 查詢錯誤 Log
{job="scraper"} |= "ERROR"
```

> **注意**: 需先執行爬蟲程式 (試題1) 才會有 Log 資料

### 異常通報設定

系統會在以下情況發送 Email 通知:

1. **爬蟲執行失敗**: 網站無法連線、驗證碼持續失敗等
2. **API 查詢結果為空**: 使用者查詢但資料庫無符合資料

### Email 設定

編輯 `試題3/docker/.env`:

```env
SMTP_HOST=smtp.gmail.com:587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_FROM=your_email@gmail.com
```

> Gmail 需使用 App Password，請至 https://myaccount.google.com/apppasswords 產生

---

## 試題4: 系統架構圖

詳見 `試題4/architecture.md`

### 架構總覽

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

---

## 加分題: 自動化排程

### APScheduler 排程

系統內建 APScheduler 支援定期執行爬蟲，排程程式位於 `試題1/scheduler.py`。

### 環境變數設定

編輯 `.env` 設定排程參數:

```env
SCHEDULER_ENABLED=true
SCHEDULER_CRON=0 2 * * 1    # 每週一凌晨 2:00
SCHEDULER_TIMEZONE=Asia/Taipei
```

### 啟動排程服務

```bash
cd 試題1
python scheduler.py
```

### Cron 表達式說明

```
┌───────────── 分 (0-59)
│ ┌───────────── 時 (0-23)
│ │ ┌───────────── 日 (1-31)
│ │ │ ┌───────────── 月 (1-12)
│ │ │ │ ┌───────────── 週 (0-6, 0=週日)
│ │ │ │ │
0 2 * * 1  = 每週一凌晨 2:00
```

---

## 常見問題

### Q: 驗證碼辨識失敗怎麼辦?

A: 系統會自動刷新驗證碼重試。若持續失敗，可設定 `CAPTCHA_MANUAL_INPUT=true` 手動輸入。

### Q: 資料庫連線失敗?

A: 確認 PostgreSQL 服務已啟動，並檢查 `.env` 中的連線設定。

### Q: Grafana 看不到 Log?

A: 確認 Loki 服務已啟動，並檢查 `LOKI_ENABLED=true`。

---

