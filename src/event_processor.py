import asyncio
import time
import logging
import aiohttp
from typing import List, Dict, Any
from decimal import Decimal
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class EventProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, event_batch_size: int = 100,
                 event_process_interval: int = 60):
        if not hasattr(self, 'initialized'):
            BACKEND_HOST = os.getenv('BACKEND_HOST', 'localhost')
            BACKEND_PORT = os.getenv('BACKEND_PORT', '4200')
            self.api_endpoint = f"http://{BACKEND_HOST}:{BACKEND_PORT}/internal/smart_token_event"
            
            self.event_batch_size = event_batch_size
            self.event_process_interval = event_process_interval
            self.pending_events = []
            self.running = True
            self.last_send_time = time.time()
            self.event_task = None
            self.initialized = True

    def start(self):
        """啟動事件處理循環"""
        if not self.event_task:
            self.event_task = asyncio.create_task(self._process_events_loop())
            logger.info("事件處理器已啟動")

    def stop(self):
        """停止事件處理循環"""
        self.running = False
        if self.event_task:
            self.event_task.cancel()
            self.event_task = None
            logger.info("事件處理器已停止")

    async def add_event(self, transaction: Dict[str, Any], wallet_address: str, 
                       token_address: str, pool_address: str = "", network: str = "BSC"):
        """添加新的交易事件到隊列"""
        try:
            # 構建事件數據
            smart_token_event_data = {
                "network": network,
                "tokenAddress": token_address,
                "poolAddress": pool_address,
                "smartAddress": wallet_address,
                "transactionType": transaction.get("transaction_type", ""),
                "transactionFromAmount": str(transaction.get("from_token_amount", "0")),
                "transactionFromToken": transaction.get("from_token_address", ""),
                "transactionToAmount": str(transaction.get("dest_token_amount", "0")),
                "transactionToToken": transaction.get("dest_token_address", ""),
                "transactionPrice": str(transaction.get("price", "0")),
                "totalPnl": str(transaction.get("realized_profit", "0")),
                "transactionTime": str(transaction.get("transaction_time")),
                "brand": "BYD"
            }

            self.pending_events.append(smart_token_event_data)
            logger.debug(f"已添加新事件到隊列，當前隊列大小: {len(self.pending_events)}")

        except Exception as e:
            logger.error(f"添加事件到隊列時發生錯誤: {str(e)}")

    async def _process_events_loop(self):
        """事件處理循環 - 定時發送批量事件"""
        try:
            while self.running:
                current_time = time.time()
                time_since_last_send = current_time - self.last_send_time

                if (len(self.pending_events) >= self.event_batch_size or 
                    (self.pending_events and time_since_last_send >= self.event_process_interval)):
                    await self._send_events_batch(self.pending_events)
                    self.pending_events = []
                    self.last_send_time = current_time

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("事件處理循環被取消")
            # 確保在關閉前發送所有待處理事件
            if self.pending_events:
                await self._send_events_batch(self.pending_events)
            raise
        except Exception as e:
            logger.exception(f"事件處理循環發生錯誤: {e}")

    async def _send_events_batch(self, events: List[Dict[str, Any]]):
        """批量發送事件到API"""
        if not events:
            return

        try:
            logger.info(f"開始批量發送 {len(events)} 個事件")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_endpoint,
                    json=events,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        logger.info(f"成功發送 {len(events)} 個事件")
                    else:
                        response_text = await response.text()
                        logger.error(f"批量發送事件失敗: {response.status}, {response_text}")
        except Exception as e:
            logger.error(f"批量發送事件時發生錯誤: {str(e)}") 