from quart import Quart, jsonify, request
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
import logging
import asyncio
import signal
import sys
import uuid
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from functools import lru_cache
import redis.asyncio as aioredis
import json

from models import WalletSummary
from database import get_main_session_factory, get_swap_session_factory, get_main_engine, SessionLocal
from config import DATABASE_URI, DATABASE_URI_SWAP_BSC, BACKEND_HOST, BACKEND_PORT
from models import *  # 假設這裡是你的模型引用
from daily_update_smart_money import WalletAnalyzer
from transaction_sync import process_wallet_batch
from event_processor import EventProcessor

# 假設的異步函數來創建時間
def get_utc8_time():
    return datetime.utcnow()  # 根據你的需求可以使用 UTC+8

# 初始化 Quart 應用
app = Quart(__name__)

# 設定日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 設置數據庫連接
DATABASE_URI_SWAP_BSC = DATABASE_URI_SWAP_BSC
engine = create_async_engine(
    DATABASE_URI_SWAP_BSC,
    isolation_level="REPEATABLE READ",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20
)

# 主要數據庫引擎
main_engine = create_async_engine(
    DATABASE_URI,
    isolation_level="REPEATABLE READ",
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False,
    autocommit=False,  # 明确设置为 False
    autoflush=False   # 禁用自动刷新
)

# 初始化 scheduler
scheduler = AsyncIOScheduler()

# 建立 Redis 連線（可根據實際情況調整 host/port/db）
redis_client = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)

async def set_task_status(request_id: str, status: dict, expire_seconds: int = 3600):
    await redis_client.set(f"task_status:{request_id}", json.dumps(status), ex=expire_seconds)

async def get_task_status(request_id: str):
    data = await redis_client.get(f"task_status:{request_id}")
    if data:
        return json.loads(data)
    return None

# 初始化事件處理器
event_processor = EventProcessor()

def signal_handler(sig, frame):
    print("接收到終止信號，正在關閉應用...")
    # 停止調度器
    scheduler.shutdown(wait=False)
    # 停止事件處理器
    event_processor.stop()
    # 關閉所有連接池
    engine.dispose()
    main_engine.dispose()
    sys.exit(0)

# 心跳接口，用於檢查服務是否正常運行
@app.route('/heartbeat', methods=['GET'])
async def heartbeat():
    """
    檢查服務是否運行，返回服務狀態。
    """
    try:
        return jsonify({"status": "OK", "message": "服務正常運行", "timestamp": get_utc8_time().isoformat()}), 200
    except Exception as e:
        logging.error(f"心跳檢查失敗: {e}")
        return jsonify({"status": "ERROR", "message": f"服務檢查失敗: {str(e)}"}), 500

# 設置定期的心跳檢查任務（例如每分鐘檢查一次）
@scheduler.scheduled_job('interval', minutes=60)
async def scheduled_heartbeat_check():
    try:
        # 使用 test_client 來發送心跳請求
        async with app.test_client() as client:
            response = await client.get('/heartbeat')
            if response.status_code != 200:
                logging.warning("心跳檢查失敗！")
    except Exception as e:
        logging.error(f"定期心跳檢查失敗: {e}")

# 數據模型
@dataclass
class WalletAnalysisRequest:
    chain: str
    type: str
    addresses: List[str]
    twitter_name: Optional[str] = None
    twitter_username: Optional[str] = None

@dataclass
class WalletBatchResponse:
    request_id: str
    ready_results: Dict
    pending_addresses: List[str]

# 異步處理錢包分析
async def process_wallet_analysis(request_id: str, addresses: List[str], chain: str, 
                                twitter_names: Optional[List[Union[str, None]]] = None, 
                                twitter_usernames: Optional[List[Union[str, None]]] = None):
    try:
        status = {
            "status": "processing",
            "start_time": int(time.time()),
            "addresses": addresses,
            "processed": 0,
            "total": len(addresses)
        }
        await set_task_status(request_id, status)

        batch_size = 10
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i + batch_size]
            batch_twitter_names = twitter_names[i:i + batch_size] if twitter_names else [None] * len(batch)
            batch_twitter_usernames = twitter_usernames[i:i + batch_size] if twitter_usernames else [None] * len(batch)
            try:
                results = await process_wallet_batch(
                    batch,
                    get_main_session_factory(),
                    get_swap_session_factory(),
                    get_main_engine(),
                    logging.getLogger(__name__),
                    batch_twitter_names,
                    batch_twitter_usernames
                )
                status["processed"] += len(batch)
                status["results"] = results
                await set_task_status(request_id, status)
            except Exception as e:
                logging.error(f"處理批次時發生錯誤: {str(e)}")
                status.setdefault("errors", []).append(str(e))
                await set_task_status(request_id, status)
        status["status"] = "completed"
        status["end_time"] = int(time.time())
        await set_task_status(request_id, status)
    except Exception as e:
        logging.error(f"處理錢包分析時發生錯誤: {str(e)}")
        status = {"status": "failed", "error": str(e)}
        await set_task_status(request_id, status)

# Webhook 更新接口
@app.route('/robots/smartmoney/webhook/update-addresses/BSC', methods=['POST'])
async def update_addresses():
    """
    異步處理錢包地址更新和分析，twitter_name、twitter_username 支援 list
    """
    try:
        data = await request.json
        chain = data.get("chain")
        type = data.get("type")
        addresses = data.get("addresses", [])
        twitter_names = data.get("twitter_names", [])
        twitter_usernames = data.get("twitter_usernames", [])

        # 添加日誌記錄
        logging.info(f"接收到的地址數量: {len(addresses)}")
        # logging.info(f"接收到的twitter_names數量: {len(twitter_names)}")
        # logging.info(f"接收到的twitter_usernames數量: {len(twitter_usernames)}")
        
        # 打印前10個地址及其對應的twitter信息
        # for i in range(min(10, len(addresses))):
        #     logging.info(f"地址 {i+1}: {addresses[i]}")
        #     logging.info(f"  - Twitter Name: {twitter_names[i] if i < len(twitter_names) else 'None'}")
        #     logging.info(f"  - Twitter Username: {twitter_usernames[i] if i < len(twitter_usernames) else 'None'}")

        if not addresses:
            return jsonify({"code": 400, "message": "請提供有效的地址列表"}), 400

        # 生成請求ID
        request_id = str(uuid.uuid4())
        
        # 驗證鏈類型
        valid_chains = ["BSC"]
        if chain not in valid_chains:
            return jsonify({
                "code": 400,
                "message": f"不支持的區塊鏈類型。支持的類型為: {', '.join(valid_chains)}"
            }), 400

        # 驗證操作類型
        valid_types = ["add", "remove"]
        if type not in valid_types:
            return jsonify({
                "code": 400,
                "message": f"不支持的操作類型。支持的類型為: {', '.join(valid_types)}"
            }), 400

        # 限制地址數量
        max_addresses = 300
        if len(addresses) > max_addresses:
            return jsonify({
                "code": 400,
                "message": f"地址數量超過限制。最大允許 {max_addresses} 個地址，請求包含 {len(addresses)} 個地址"
            }), 400

        # 移除重複地址
        unique_addresses = list(dict.fromkeys(addresses))
        if len(unique_addresses) < len(addresses):
            logging.info(f"已移除 {len(addresses) - len(unique_addresses)} 個重複地址")
            # 也要同步移除 twitter 對應資訊
            if isinstance(twitter_names, list) and isinstance(twitter_usernames, list):
                idx_map = [addresses.index(addr) for addr in unique_addresses]
                twitter_names = [twitter_names[i] if i < len(twitter_names) else None for i in idx_map]
                twitter_usernames = [twitter_usernames[i] if i < len(twitter_usernames) else None for i in idx_map]

        async with SessionLocal() as session:
            if type == "add":
                # await add_wallets_to_db(chain, session, unique_addresses)
                asyncio.create_task(process_wallet_analysis(
                    request_id,
                    unique_addresses,
                    chain,
                    twitter_names,
                    twitter_usernames
                ))
                return jsonify({
                    "code": 200,
                    "message": "地址添加成功，開始分析",
                    "request_id": request_id,
                    "pending_addresses": unique_addresses
                }), 200
            elif type == "remove":
                await remove_wallets_from_db(chain, session, unique_addresses)
                return jsonify({
                    "code": 200,
                    "message": "地址刪除成功！"
                }), 200
    except Exception as e:
        logging.error(f"更新地址失敗: {str(e)}")
        return jsonify({"code": 500, "message": f"更新地址失敗: {str(e)}"}), 500

# 添加查詢任務狀態的端點
@app.route('/robots/smartmoney/webhook/task-status/<request_id>', methods=['GET'])
async def get_task_status_endpoint(request_id: str):
    """
    查詢任務處理狀態
    """
    status = await get_task_status(request_id)
    if not status:
        return jsonify({"code": 404, "message": "找不到指定的任務"}), 404
    return jsonify({"code": 200, "data": status}), 200

class WalletMonitor:
    def __init__(self):
        self.is_processing = False
        self.analyzer = WalletAnalyzer(DATABASE_URI, DATABASE_URI_SWAP_BSC)  # 從 daily_update_smart_money.py 導入
        self.batch_size = 100  # 每批處理的錢包數量
        
    async def start_monitoring(self, addresses: List[str], chain: str, session: AsyncSession):
        """
        開始監控新加入的錢包地址
        """
        try:            
            # 如果沒有其他分析進程在運行，才啟動新的分析進程
            if not self.is_processing:
                self.is_processing = True
                await self._start_analysis_loop(chain, session)
        except Exception as e:
            logging.error(f"啟動錢包監控時發生錯誤: {str(e)}")
            raise

    async def _start_analysis_loop(self, chain: str, session: AsyncSession):
        """
        開始週期性分析循環
        """
        try:
            while True:
                # 獲取要分析的活躍錢包
                addresses = await get_active_wallets(chain, session)
                    
                if not addresses:
                    # logging.info("暫時沒有需要分析的錢包")
                    await asyncio.sleep(60)
                    continue

                try:
                    # 使用 WalletAnalyzer 的 process_wallet_batch 方法處理錢包
                    results = await self.analyzer.process_wallet_batch(addresses, 700)
                except Exception as e:
                    logging.error(f"處理錢包批次時發生錯誤: {str(e)}")

                # 等待1分鐘後再處理下一批
                await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"分析循環中發生錯誤: {str(e)}")
            self.is_processing = False
            raise
        finally:
            self.is_processing = False

wallet_monitor = WalletMonitor()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def immediate_sync():
    """立即執行同步任務"""
    from transaction_sync import sync_new_transactions
    try:
        logging.info("立即執行同步任務開始...")
        await sync_new_transactions(SessionLocal, SessionLocal, engine)
        logging.info("立即執行同步任務完成")
    except Exception as e:
        logging.error(f"立即執行同步任務出錯: {e}")

@app.before_serving
async def startup():
    """
    在 Quart 启动时启动 APScheduler 和其他任务。
    """
    # 原有代碼
    scheduler.start()  # 啟動 scheduler
    
    # 啟動事件處理器
    event_processor.start()
    logging.info("事件處理器已啟動")
    
    # 新增：設置交易同步任務，傳遞引擎實例
    # setup_sync_task(scheduler, SessionLocal, SessionLocal, engine)
    logging.info("交易同步任務已設置")
    
    # 立即執行一次同步任務
    # app.add_background_task(immediate_sync)

async def run_wallet_monitor():
    async with SessionLocal() as session:
        await wallet_monitor._start_analysis_loop("BSC", session)

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=5000, startup_timeout=60)