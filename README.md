# smart_money_bsc

# 專案環境設置說明

本文檔說明如何設置開發環境，包括創建虛擬環境、激活虛擬環境以及安裝所需的依賴包。

## 前置需求

- Python 3.10.11
- pip (Python 包管理工具)
- Supervisor (進程管理工具)

## 設置步驟

### 1. 創建虛擬環境

虛擬環境可以隔離專案的依賴，避免與系統其他專案產生衝突。在專案根目錄執行：

```bash
python -m venv venv
```

### 2. 激活虛擬環境

#### Windows:

```bash
venv\Scripts\activate
```

#### macOS/Linux:

```bash
source venv/bin/activate
```

激活後，命令提示符前應出現 `(venv)` 字樣，表示已進入虛擬環境。

### 3. 安裝依賴包

使用 pip 從 requirements.txt 安裝所需的依賴包：

```bash
pip install -r requirements.txt
```

### 4. 驗證安裝

安裝完成後，可以檢查已安裝的包：

```bash
pip list
```

## 服務管理

本專案使用 Supervisor 來管理以下三個服務：

- `smart_money_bsc`: 主要的智能錢包分析服務
- `daily_update_bsc_smart_money`: 每日更新智能錢包數據服務
- `bsc_transaction_sync`: BSC 交易同步服務

### 使用 restart.sh 腳本

專案根目錄提供了 `restart.sh` 腳本來方便地管理這些服務：

#### 初始設置

首次使用前，需要給腳本添加執行權限：

```bash
chmod +x restart.sh
```

#### 基本用法

```bash
# 重啟所有服務
sudo ./restart.sh

# 重啟特定服務
./restart.sh bsc_transaction_sync
./restart.sh smart_money_bsc
./restart.sh daily_update_bsc_smart_money

# 查看幫助
./restart.sh help
```

### 日誌查看

服務的日誌文件位於 `logs/` 目錄：

```bash
# 查看實時日誌
tail -f logs/transaction_sync.log
tail -f logs/smart_money_bsc.log
tail -f logs/daily_update_smart_money.log
```

## 退出虛擬環境

完成工作後，可以通過以下命令退出虛擬環境：

```bash
deactivate
```

## 常見問題

- 如果創建虛擬環境時出現錯誤，請確保已正確安裝 Python。
- 如果安裝依賴包時出現錯誤，可能是網絡問題或某些包需要額外的系統依賴，請查看錯誤信息並解決。
- 如果服務無法啟動，請檢查日誌文件中的錯誤信息。
- 確保 Supervisor 已正確安裝並配置。

## 注意事項

- 虛擬環境資料夾 (`venv/`) 已添加到 `.gitignore`，不會被提交到版本控制系統中。
- 每次在新的終端工作時，都需要重新激活虛擬環境。
- `restart.sh` 腳本需要 root 權限運行，請使用 `sudo` 命令。
- 服務配置文件位於 `run/smart_money_bsc.conf`。