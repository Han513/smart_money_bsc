import os
import time
import asyncio
import logging
import psutil
import traceback
import json
from typing import Dict, List, Any, Set, Optional
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy import select, text, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from models import *
from token_info import TokenInfoFetcher, redis_client
from balance import fetch_wallet_balances, get_price
from eth_utils import to_checksum_address as eth_to_checksum_address
import hashlib
import web3
from web3 import Web3
from functools import lru_cache
from aiokafka import AIOKafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, RPC_URL
from sqlalchemy.dialects.postgresql import insert
from collections import defaultdict
from token_cache import token_cache  # 导入 Redis 缓存实例

logger = logging.getLogger(__name__)
now_utc_plus_8 = datetime.utcnow() + timedelta(hours=8)

# 初始化 Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
print(RPC_URL)

class KafkaConsumer:
    def __init__(self, main_session_factory, swap_session_factory, main_engine):
        self.main_session_factory = main_session_factory
        self.swap_session_factory = swap_session_factory
        self.main_engine = main_engine
        self.consumer = None
        self.wallet_address_set = set()
        self.running = True
        self.session = None  # 初始化 session 属性为 None

    async def refresh_wallet_address_set(self):
        """刷新钱包地址集合"""
        try:
            async with self.swap_session_factory() as session:
                addresses = await get_active_wallets("BSC", session)
                self.wallet_address_set = set(addresses)
                logger.info(f"[CACHE] 已刷新 wallet_address_set, 共 {len(self.wallet_address_set)} 筆")
        except Exception as e:
            logger.error(f"刷新钱包地址集合时出错: {str(e)}")

    async def start(self):
        """启动 Kafka 消费者"""
        try:
            logger.info("Starting Kafka consumer...")
            
            # 创建数据库会话
            self.session = self.swap_session_factory()
            
            # 初始化消费者
            self.consumer = AIOKafkaConsumer(
                'web3_trade_sm_events',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='smart_money_group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            logger.info(f"Connecting to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
            await self.consumer.start()
            logger.info("Kafka consumer started successfully, listening to topic: web3_trade_sm_events")

            # 刷新钱包地址集合
            await self.refresh_wallet_address_set()

            logger.info("Starting to consume messages...")
            try:
                async for msg in self.consumer:
                    if not self.running:
                        break
                    try:
                        await self.process_message(msg.value)
                    except Exception as e:
                        logger.error(f"处理消息时出错: {str(e)}")
            finally:
                await self.stop()

        except Exception as e:
            logger.error(f"Kafka consumer error: {str(e)}")
            if self.consumer:
                await self.consumer.stop()
            if self.session:
                await self.session.close()

    async def stop(self):
        """停止 Kafka 消费者"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        if self.session:
            await self.session.close()
            self.session = None

    async def process_message(self, message: Dict):
        """處理 Kafka 消息"""
        try:
            # 檢查消息類型
            if message.get('type') != 'com.zeroex.web3.core.event.data.TradeEvent':
                return

            event = message.get('event', {})
            if not event or event.get('network') != 'BSC':
                return

            tx_hash = event.get('hash')
            wallet_address = event.get('address')
            token_address = event.get('tokenAddress')
            
            # 计算 amount
            if event.get('side', '').lower() == 'buy':
                amount = Decimal(str(event.get('toTokenAmount', '0')))
            else:
                amount = Decimal(str(event.get('fromTokenAmount', '0')))
                
            value_usd = Decimal(str(event.get('volumeUsd', '0')))
            timestamp = int(event.get('timestamp', 0))
            side = event.get('side', '').lower()

            # 只处理在 wallet_address_set 中的钱包
            if wallet_address not in self.wallet_address_set:
                return

            # 获取代币信息
            token_info = await TokenInfoFetcher.get_token_info(token_address)
            if not token_info:
                return

            # 获取当前持仓数据
            token_data = token_cache.get_token_data(wallet_address, token_address)
            current_holding = token_data['total_amount']

            # 更新缓存中的数据
            token_data = token_cache.update_token_data(
                wallet_address,
                token_address,
                side,
                amount,
                value_usd,
                timestamp
            )

            # 确定交易类型
            transaction_type = token_cache.determine_transaction_type(side, amount, current_holding)

            # 获取钱包余额
            try:
                wallet_balance = await get_wallet_balance(wallet_address)
            except Exception as e:
                logger.warning(f"获取钱包余额失败: {str(e)}")
                wallet_balance = Decimal('0')

            # 准备交易数据
            # 根据交易方向设置 from_token 和 dest_token
            if side == 'buy':
                from_token_address = event.get('quoteMint', '')
                dest_token_address = event.get('baseMint', '')
                from_token_symbol = event.get('quoteSymbol', '')
                dest_token_symbol = event.get('baseSymbol', '')
                from_token_amount = event.get('fromTokenAmount', 0)
                dest_token_amount = event.get('toTokenAmount', 0)
            else:  # sell
                from_token_address = event.get('baseMint', '')
                dest_token_address = event.get('quoteMint', '')
                from_token_symbol = event.get('baseSymbol', '')
                dest_token_symbol = event.get('quoteSymbol', '')
                from_token_amount = event.get('fromTokenAmount', 0)
                dest_token_amount = event.get('toTokenAmount', 0)

            tx_data = {
                "wallet_address": wallet_address,
                "wallet_balance": float(wallet_balance),
                "token_address": token_address,
                "token_icon": token_info.get('token_icon', ''),
                "token_name": token_info.get('token_name', ''),
                "price": float(value_usd / amount) if amount > 0 else 0,
                "amount": float(amount),
                "marketcap": token_info.get('marketcap', 0),
                "value": float(value_usd),
                "chain": "BSC",
                "chain_id": 9006,
                "realized_profit": float(token_data['realized_profit']),
                "realized_profit_percentage": float(token_data['realized_profit_percentage']),
                "transaction_type": transaction_type,
                "transaction_time": timestamp,
                "time": get_utc8_time(),
                "from_token_address": from_token_address,
                "dest_token_address": dest_token_address,
                "from_token_symbol": from_token_symbol,
                "dest_token_symbol": dest_token_symbol,
                "from_token_amount": float(from_token_amount) if from_token_amount else 0.0,
                "dest_token_amount": float(dest_token_amount) if dest_token_amount else 0.0,
                "signature": tx_hash
            }

            # 保存交易记录
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    success = await save_past_transaction(
                        self.session,
                        tx_data,
                        wallet_address,
                        tx_hash,
                        "BSC",
                        auto_commit=True
                    )
                    if success:
                        break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"保存交易记录失败，已重试 {max_retries} 次: {str(e)}")
                        raise
                    logger.warning(f"保存交易记录失败，正在重试 ({retry_count}/{max_retries}): {str(e)}")
                    await asyncio.sleep(0.5)  # 等待500ms后重试

            # 检查是否需要刷新缓存到数据库
            await token_cache.maybe_flush_to_db(self.session)

            return True

        except Exception as e:
            logger.error(f"处理消息时出错: {str(e)}")
            return False

async def with_timeout(coro, timeout=60, description="操作"):
    try:
        start_time = time.time()
        result = await asyncio.wait_for(coro, timeout)
        end_time = time.time()
        duration = end_time - start_time
        if duration > 5:  # 只記錄執行時間超過5秒的操作
            logger.info(f"{description}完成，耗時: {duration:.2f}秒")
        return result
    except asyncio.TimeoutError:
        logger.error(f"{description}超時（{timeout}秒）")
        raise
        
# 簡單的資源使用監控
def log_resource_usage(operation=""):
    process = psutil.Process()
    mem_info = process.memory_info()
    cpu_percent = process.cpu_percent(interval=0.1)
    logger.info(f"資源使用 [{operation}]: CPU {cpu_percent}%, 記憶體 {mem_info.rss / (1024 * 1024):.1f} MB")

def get_update_time():
    # 获取当前时间，回傳 datetime 物件（UTC+8）
    now_utc = datetime.utcnow()
    utc_plus_8 = now_utc + timedelta(hours=8)
    return utc_plus_8

async def fetch_token_info_for_wallets(remaining_tokens, batch_size=30):
    """
    批量獲取代幣信息，分批处理以避免超时
    """
    all_results = {}
    tokens_list = list(remaining_tokens)
    
    # 分批处理
    for i in range(0, len(tokens_list), batch_size):
        batch = tokens_list[i:i+batch_size]
        tasks = [TokenInfoFetcher.get_token_info(token) for token in batch]
        batch_results = await asyncio.gather(*tasks)
        batch_dict = dict(zip(batch, batch_results))
        all_results.update(batch_dict)
        
        # 添加短暂暂停，避免过快请求
        await asyncio.sleep(0.1)
    
    return all_results

def to_checksum_address(address):
    """將地址轉換為 checksum 格式"""
    if not address:
        return None
    try:
        return web3.Web3.to_checksum_address(address)
    except Exception as e:
        logger.error(f"Error converting address to checksum: {str(e)}")
        return None

# 添加緩存裝飾器
@lru_cache(maxsize=1000)
def get_cached_token_info(address: str) -> Optional[Dict]:
    """緩存單個代幣信息"""
    return None

async def get_token_info_from_Ian(token_addresses: List[str], engine) -> Dict:
    """从数据库批量获取代币信息，使用緩存和優化查詢"""    
    try:
        async_session = sessionmaker(
            bind=engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        async with async_session() as token_session:
            if isinstance(token_addresses, str):
                token_addresses = [token_addresses]
            
            # 轉換為 checksum 地址
            checksum_addresses = [to_checksum_address(addr) for addr in token_addresses]
            
            # 檢查緩存
            cached_results = {}
            addresses_to_fetch = []
            
            for addr in checksum_addresses:
                cached_data = get_cached_token_info(addr)
                if cached_data is not None:
                    cached_results[addr] = cached_data
                else:
                    addresses_to_fetch.append(addr)
            
            if not addresses_to_fetch:
                return cached_results
            
            # 修正 IN 子句的語法
            query = """
            SELECT address, symbol, name, decimals, uri, supply
            FROM dex_query_v1.tokens 
            WHERE address = ANY(:token_addresses)
            """
            
            # 使用參數化查詢
            result = await token_session.execute(
                text(query), 
                {'token_addresses': addresses_to_fetch}
            )
            
            # 處理查詢結果
            token_data_dict = cached_results.copy()
            for row in result:
                token_dict = dict(row._mapping)
                addr = token_dict['address']
                token_data_dict[addr] = token_dict
                # 更新緩存
                get_cached_token_info.cache_clear()  # 清除舊緩存
                get_cached_token_info(addr)  # 添加新緩存
            
            return token_data_dict
            
    except Exception as e:
        logger.error(f"Error getting token info batch: {str(e)}")
        logger.error(traceback.format_exc())
        return {}

def _process_buy_token_data(token_data, amount, value):
    """處理買入時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    total_amount = Decimal(str(token_data["total_amount"]))
    total_cost = Decimal(str(token_data["total_cost"]))
    amount = Decimal(str(amount)) if not isinstance(amount, Decimal) else amount
    value = Decimal(str(value)) if not isinstance(value, Decimal) else value
    
    # 更新總量和總成本
    new_total_amount = total_amount + amount
    new_total_cost = total_cost + value
    
    # 計算平均買入價格
    avg_buy_price = new_total_cost / new_total_amount if new_total_amount > Decimal('0') else Decimal('0')
    
    # 更新代幣數據
    token_data.update({
        "total_amount": new_total_amount,
        "total_cost": new_total_cost,
        "avg_buy_price": avg_buy_price
    })
    
    return token_data

def _process_sell_token_data(token_data, amount, value):
    """處理賣出時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    # 確保所有數值都是 Decimal 類型
    total_amount = Decimal(str(token_data["total_amount"]))
    amount = Decimal(str(amount)) if not isinstance(amount, Decimal) else amount
    value = Decimal(str(value)) if not isinstance(value, Decimal) else value
    total_cost = Decimal(str(token_data["total_cost"]))
    
    if total_amount <= Decimal('0'):
        token_data.update({
            "total_amount": Decimal('0'),
            "total_cost": Decimal('0'),
            "pnl": value,
            "pnl_percentage": Decimal('100'),
            "sell_percentage": Decimal('100')
        })
        return token_data
        
    # 計算賣出百分比
    sell_percentage = min((amount / (amount + total_amount)) * Decimal('100'), Decimal('100'))
    
    # 計算平均買入價格
    avg_buy_price = total_cost / total_amount if total_amount > Decimal('0') else Decimal('0')
    
    # 計算賣出價格
    sell_price = value / amount if amount > Decimal('0') else Decimal('0')
    
    # 計算新的總量
    new_total_amount = max(Decimal('0'), total_amount - amount)
    
    # 計算利潤
    pnl = (sell_price - avg_buy_price) * amount
    
    # 計算利潤百分比
    pnl_percentage = ((sell_price / avg_buy_price) - Decimal('1')) * Decimal('100') if avg_buy_price > Decimal('0') else Decimal('0')
    
    # 更新代幣數據
    token_data.update({
        "pnl": pnl,
        "pnl_percentage": pnl_percentage,
        "sell_percentage": sell_percentage,
        "total_amount": new_total_amount,
        "total_cost": Decimal('0') if new_total_amount <= amount else total_cost
    })
    
    return token_data

async def get_wallet_addresses_from_summary(session):
    """從 WalletSummary 表獲取所有錢包地址"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        WalletSummary.__table__.schema = "dex_query_v1"  # 根據您的實際 schema 設置
        
        # 查詢所有錢包地址
        query = select(WalletSummary.wallet_address).distinct()
        result = await session.execute(query)
        wallet_addresses = result.scalars().all()

        return list(wallet_addresses)
    except Exception as e:
        logger.error(f"獲取錢包地址時發生錯誤: {str(e)}")
        return []

async def get_existing_signatures(session, wallet_addresses):
    """獲取指定錢包的現有交易簽名"""
    await session.execute(text("SET search_path TO dex_query_v1;"))
    Transaction.__table__.schema = "dex_query_v1"  # 根據您的實際 schema 設置
    
    query = select(Transaction.signature).where(
        Transaction.wallet_address.in_(wallet_addresses)
    )
    result = await session.execute(query)
    signatures = set(result.scalars().all())
    
    return signatures

async def get_wallet_trades_batch(session, wallet_addresses: List[str]):
    """批量獲取多個錢包的交易記錄（逐個查詢，並加強debug日誌，並將地址轉為checksum格式）"""
    await session.execute(text("SET search_path TO dex_query_v1;"))
    # 1. 查詢 search_path
    result = await session.execute(text("SHOW search_path"))
    print("[DEBUG] Current search_path:", result.fetchall())
    # 2. 查詢 trades 表前5筆
    result = await session.execute(text("SELECT * FROM dex_query_v1.trades LIMIT 5"))
    print("[DEBUG] Sample trades:", result.mappings().all())
    trades_by_wallet = {}
    for wallet in wallet_addresses:
        try:
            checksum_wallet = to_checksum_address(wallet)
        except Exception:
            checksum_wallet = wallet  # fallback, 若非EVM格式
        print(f"[DEBUG] Querying trades for wallet: {wallet} (checksum: {checksum_wallet})")
        # 3. 查詢指定錢包的trades前5筆
        result = await session.execute(text("SELECT * FROM dex_query_v1.trades WHERE chain_id = 9006 AND signer = :wallet_address LIMIT 5"), {"wallet_address": checksum_wallet})
        print(f"[DEBUG] Sample trades for {wallet}:", result.mappings().all())
        # 4. 查詢所有交易
        result = await session.execute(text("SELECT * FROM dex_query_v1.trades WHERE chain_id = 9006 AND signer = :wallet_address ORDER BY timestamp ASC"), {"wallet_address": checksum_wallet})
        trades = result.mappings().all()
        print(f"[DEBUG] Got {len(trades)} trades for {wallet}")
        if trades:
            trades_by_wallet[wallet] = trades
    return trades_by_wallet

async def get_wallet_token_buy_data(wallet_address, session):
    """獲取指定錢包的所有代幣購買數據"""
    try:
        TokenBuyData.__table__.schema = "dex_query_v1"
        
        query = select(TokenBuyData).where(
            TokenBuyData.wallet_address == wallet_address
        )
        result = await session.execute(query)
        token_buy_data_list = result.scalars().all()
        
        token_buy_data_dict = {}
        for data in token_buy_data_list:
            token_buy_data_dict[data.token_address] = {
                "token_address": data.token_address,
                "avg_buy_price": Decimal(str(data.avg_buy_price)) if data.avg_buy_price else Decimal('0'),
                "total_amount": Decimal(str(data.total_amount)) if data.total_amount else Decimal('0'),
                "total_cost": Decimal(str(data.total_cost)) if data.total_cost else Decimal('0'),
                "historical_total_buy_amount": Decimal(str(data.historical_total_buy_amount)) if data.historical_total_buy_amount else Decimal('0'),
                "historical_total_buy_cost": Decimal(str(data.historical_total_buy_cost)) if data.historical_total_buy_cost else Decimal('0'),
                "historical_avg_buy_price": Decimal(str(data.historical_avg_buy_price)) if data.historical_avg_buy_price else Decimal('0'),
                "historical_total_sell_amount": Decimal(str(data.historical_total_sell_amount)) if data.historical_total_sell_amount else Decimal('0'),
                "historical_total_sell_value": Decimal(str(data.historical_total_sell_value)) if data.historical_total_sell_value else Decimal('0'),
                "historical_avg_sell_price": Decimal(str(data.historical_avg_sell_price)) if data.historical_avg_sell_price else Decimal('0'),
                "total_buy_count": data.total_buy_count if data.total_buy_count else 0,
                "total_sell_count": data.total_sell_count if data.total_sell_count else 0,
                "last_transaction_time": data.last_transaction_time if data.last_transaction_time else 0
            }
        
        return token_buy_data_dict
    
    except Exception as e:
        logger.error(f"獲取錢包 {wallet_address} 的代幣購買數據時發生錯誤: {str(e)}")
        return {}

async def process_trades_and_update_wallets(wallet, trades, session, bnb_price, main_engine, logger, balance_usd):
    """處理交易並更新錢包數據"""
    try:
        wallet_address = wallet.wallet_address
        transactions_to_save = []

        for trade in trades:
            try:
                # 獲取代幣信息
                token_address = trade.token_address.lower()
                
                # 從緩存獲取代幣數據
                token_data = token_buy_data_cache.get(wallet_address, token_address, trade.transaction_time.date())
                
                # 處理交易
                action = trade.transaction_type
                amount = Decimal(str(trade.amount))
                value = Decimal(str(trade.value))
                
                if action == 'buy':
                    token_data = _process_buy_token_data(token_data, amount, value)
                elif action == 'sell':
                    token_data = _process_sell_token_data(token_data, amount, value)
                
                # 更新最後交易時間
                token_data['last_transaction_time'] = int(trade.transaction_time.timestamp())
                
                # 更新緩存
                token_buy_data_cache.update(wallet_address, token_address, trade.transaction_time.date(), action, amount, value, int(trade.transaction_time.timestamp()))
                
                # 構建交易記錄
                tx_data = {
                    "wallet_address": wallet_address,
                    "wallet_balance": 0.0,  # 添加默认值为0.0
                    "token_address": token_address,
                    "token_icon": token_info_results.get(token, {}).get('token_icon', ''),
                    "token_name": token_info_results.get(token, {}).get('token_name', ''),
                    "price": float(value / amount) if amount > 0 else 0,
                    "amount": float(amount),
                    "marketcap": token_info_results.get(token, {}).get('marketcap', 0),
                    "value": float(value),
                    "chain": "BSC",
                    "chain_id": 9006,
                    "realized_profit": float(token_data['realized_profit']),
                    "realized_profit_percentage": float(token_data['realized_profit_percentage']),
                    "transaction_type": action,
                    "transaction_time": trade.transaction_time,
                    "time": get_update_time(),
                    "signature": trade.signature,
                }
                
                # 获取钱包余额
                try:
                    wallet_balance = await get_wallet_balance(wallet_address)
                    tx_data["wallet_balance"] = float(wallet_balance)
                except Exception as e:
                    logger.warning(f"获取钱包余额失败: {str(e)}")
                    # 保持默认值 0.0
                
                # 添加到待保存列表
                transactions_to_save.append(tx_data)
                
                # 每處理一定數量的交易就嘗試同步到資料庫
                if len(transactions_to_save) >= 50:
                    await token_buy_data_cache.maybe_flush(session)
                
            except Exception as e:
                logger.error(f"處理交易時出錯: {str(e)}")
                continue

        # 最後一次同步到資料庫
        await token_buy_data_cache.maybe_flush(session)
        
        return transactions_to_save

    except Exception as e:
        logger.error(f"處理錢包 {wallet_address} 的交易時出錯: {str(e)}")
        return []

async def analyze_wallets_data(wallet_stats, bnb_price):
    """
    分析钱包数据，确保正确处理类型转换，避免 float 和 Decimal 混用
    """
    wallet_analysis = {}
    current_time = int(time.time())  # 当前时间戳（毫秒）
    total_wallets = len(wallet_stats)
    processed_wallets = 0

    async def process_wallet(wallet, stats):
        nonlocal processed_wallets
        logger.info(f"開始分析錢包 {wallet} ({processed_wallets+1}/{total_wallets})的數據")
        wallet_start_time = time.time()

        total_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'])
        total_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'])
        pnl = stats['pnl']
        num_buy = len(stats['buy'])
        num_sell = len(stats['sell'])
        total_transactions = num_buy + num_sell

        # 盈利代币数量和胜率
        profitable_tokens = Decimal('0')  # 使用 Decimal
        for token in stats['tokens']:
            token_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'] if tx['token'] == token)
            token_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'] if tx['token'] == token)
            
            if token_sell > token_buy:
                profitable_tokens += Decimal('1')  # 使用 Decimal

        total_tokens = Decimal(str(len(stats['tokens'])))  # 使用 Decimal
        win_rate = min((profitable_tokens / total_tokens) * Decimal('100'), Decimal('100')) if total_tokens > 0 else Decimal('0')

        # 时间范围
        one_day_ago = current_time - 24 * 60 * 60
        seven_days_ago = current_time - 7 * 24 * 60 * 60
        thirty_days_ago = current_time - 30 * 24 * 60 * 60

        # 计算函数
        def calculate_metrics(buy, sell, start_time):
            # 確保所有數值操作使用 Decimal 類型
            buy_filtered = [tx for tx in buy if tx['timestamp'] >= start_time]
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            if not buy_filtered and not sell_filtered:
                return {
                    "win_rate": 0,
                    "total_cost": 0,
                    "avg_cost": 0,
                    "total_transaction_num": 0,
                    "buy_num": 0,
                    "sell_num": 0,
                    "pnl": 0,
                    "pnl_percentage": 0,
                    "avg_realized_profit": 0,
                    "unrealized_profit": 0
                }
            
            total_buy_value = sum(Decimal(str(tx['value'])) for tx in buy_filtered)
            total_sell_value = sum(Decimal(str(tx['value'])) for tx in sell_filtered)
            
            num_buy = len(buy_filtered)
            num_sell = len(sell_filtered)
            
            pnl_value = total_sell_value - total_buy_value
            
            # 避免除以零
            pnl_percentage = Decimal('0')
            if total_buy_value > Decimal('0'):
                pnl_percentage = max((pnl_value / total_buy_value) * Decimal('100'), Decimal('-100'))
            
            avg_cost = Decimal('0')
            if num_buy > 0:
                avg_cost = total_buy_value / Decimal(num_buy)
                
            avg_realized_profit = Decimal('0')
            if num_sell > 0:
                avg_realized_profit = pnl_value / Decimal(num_sell)
                
            total_transaction_num = num_buy + num_sell

            # 返回結果時轉換為 float，以便於存儲到數據庫
            return {
                "win_rate": float(win_rate),
                "total_cost": float(total_buy_value),
                "avg_cost": float(avg_cost),
                "total_transaction_num": total_transaction_num,
                "buy_num": num_buy,
                "sell_num": num_sell,
                "pnl": float(pnl_value),
                "pnl_percentage": float(pnl_percentage),
                "avg_realized_profit": float(avg_realized_profit),
                "unrealized_profit": 0
            }

        # 按时间范围计算
        metrics_1d = calculate_metrics(stats['buy'], stats['sell'], one_day_ago)
        metrics_7d = calculate_metrics(stats['buy'], stats['sell'], seven_days_ago)
        metrics_30d = calculate_metrics(stats['buy'], stats['sell'], thirty_days_ago)

        # 计算 asset_multiple
        asset_multiple = metrics_30d['pnl_percentage'] / 100 if metrics_30d['pnl_percentage'] != 0 else 0
        
        # 计算最后活跃的前三个代币
        all_tokens = stats['buy'] + stats['sell']
        sorted_tokens = sorted(all_tokens, key=lambda tx: tx['timestamp'], reverse=True)
        token_list = ",".join(
            list(dict.fromkeys(tx['token'] for tx in sorted_tokens))[:3]
        )

        # 计算 pnl_pic
        def calculate_daily_pnl(buy, sell, days, start_days_ago=None):
            """
            计算每日 PNL 数据。
            参数:
                buy: 买入交易记录
                sell: 卖出交易记录
                days: 总天数
                start_days_ago: 从多少天前开始计算（可选）
            """
            daily_pnl = []
            start_days_ago = start_days_ago or days  # 默认为整个时间范围
            for day in range(start_days_ago):
                start_time = current_time - (day + 1) * 24 * 60 * 60 * 1000
                end_time = current_time - day * 24 * 60 * 60 * 1000
                
                # 確保所有數值操作使用 Decimal 類型
                daily_buy = sum(Decimal(str(tx['value'])) for tx in buy if start_time <= tx['timestamp'] < end_time)
                daily_sell = sum(Decimal(str(tx['value'])) for tx in sell if start_time <= tx['timestamp'] < end_time)
                
                daily_pnl.append(float(daily_sell - daily_buy))  # 轉換回 float 用於輸出
                
            return ",".join(map(str, daily_pnl[::-1]))  # 按从过去到现在排列

        pnl_pic_1d = calculate_daily_pnl(stats['buy'], stats['sell'], 1)
        pnl_pic_7d = calculate_daily_pnl(stats['buy'], stats['sell'], 7)
        pnl_pic_30d = calculate_daily_pnl(stats['buy'], stats['sell'], 30)

        # 计算分布和分布百分比
        def calculate_distribution(sell, start_time):
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            distribution = {"lt50": 0, "0to50": 0, "0to200": 0, "200to500": 0, "gt500": 0}
            
            # 避免 total_buy 为零的情况
            total_buy_dec = total_buy if total_buy != Decimal('0') else Decimal('1')  # 使用 1 作为默认值，防止零除

            for tx in sell_filtered:
                tx_value = Decimal(str(tx['value']))  # 確保值是 Decimal 類型
                
                if num_buy > 0:  # 检查 num_buy 是否为有效值
                    pnl_percentage = ((tx_value - total_buy_dec) / total_buy_dec) * Decimal('100')
                else:
                    pnl_percentage = Decimal('0')  # 如果 num_buy 为 0，则直接设置 pnl_percentage 为 0

                if pnl_percentage < Decimal('-50'):
                    distribution["lt50"] += 1
                elif Decimal('-50') <= pnl_percentage < Decimal('0'):
                    distribution["0to50"] += 1
                elif Decimal('0') <= pnl_percentage <= Decimal('200'):
                    distribution["0to200"] += 1
                elif Decimal('200') < pnl_percentage <= Decimal('500'):
                    distribution["200to500"] += 1
                else:
                    distribution["gt500"] += 1

            return distribution

        def calculate_distribution_percentage(distribution, total_tokens):
            total_distribution = sum(distribution.values())
            return {key: float((Decimal(value) / Decimal(total_distribution) * Decimal('100'))) 
                    if total_distribution > 0 else 0 
                    for key, value in distribution.items()}

        distribution_7d = calculate_distribution(stats['sell'], seven_days_ago)
        distribution_30d = calculate_distribution(stats['sell'], thirty_days_ago)
        distribution_percentage_7d = calculate_distribution_percentage(distribution_7d, total_tokens)
        distribution_percentage_30d = calculate_distribution_percentage(distribution_30d, total_tokens)

        # 获取钱包余额
        balances = await with_timeout(
            fetch_wallet_balances([wallet], bnb_price),
            timeout=30,
            description=f"獲取錢包 {wallet} 的餘額"
        )

        remaining_tokens = stats['remaining_tokens']
        tokens_count = len(remaining_tokens)
        if tokens_count > 10:
            logger.info(f"獲取錢包 {wallet} 的 {tokens_count} 個代幣信息")
            
        token_info_results = await with_timeout(
            fetch_token_info_for_wallets(remaining_tokens),
            timeout=60,
            description=f"獲取錢包 {wallet} 的代幣信息"
        )

        tx_data_list = []
        
        for token, data in remaining_tokens.items():
            # 獲取查詢結果
            token_info = token_info_results.get(token, {})
            supply = Decimal('0')
            if token_info and token_info.get('supply') is not None and token_info.get('decimals') is not None:
                supply = Decimal(str(token_info['supply'])) / Decimal(str(10 ** token_info['decimals']))

            token_info_Ian = await TokenInfoFetcher.get_token_info(token.lower())
            print(token_info_Ian)
            
            # 確保所有數值操作使用 Decimal 類型
            amount = Decimal(str(data.get("amount", 0)))
            profit = Decimal(str(data.get("profit", 0)))
            cost = Decimal(str(data.get("cost", 0)))

            # 計算最後一筆交易時間
            last_transaction_time = max(
                tx['timestamp'] for tx in (stats['buy'] + stats['sell']) if tx['token'] == token
            )

            # 計算買入均價
            buy_transactions = [
                {
                    'amount': Decimal(str(remaining_tokens[token]['amount'])),
                    'cost': Decimal(str(remaining_tokens[token]['cost']))
                } 
                for token in remaining_tokens
            ]

            total_buy_value = sum(tx['cost'] for tx in buy_transactions)
            total_buy_amount = sum(tx['amount'] for tx in buy_transactions if tx['amount'] > Decimal('0'))
            
            avg_price = Decimal('0')
            if total_buy_amount > Decimal('0'):
                avg_price = total_buy_value / total_buy_amount
                
            # token_info 中的價格是字符串，確保轉換為 Decimal
            token_info_price_native = Decimal(token_info.get("priceNative", "0"))
            token_info_price_usd = Decimal(token_info.get("priceUsd", "0"))

            # 計算 pnl 和 pnl_percentage
            pnl_value = profit - cost
            pnl_percentage = Decimal('0')
            if cost > Decimal('0'):
                pnl_percentage = (profit - cost) / cost * Decimal('100')

            # 統計結果 - 轉換為 float 用於數據庫存儲
            remaining_tokens_summary = {
                "token_address": token,
                "token_name": token_info_Ian.get("symbol", None),
                "token_icon": token_info_Ian.get("uri", None),
                "chain": "BSC",
                "amount": float(amount),
                "value": float(amount * token_info_price_native),  # 以最新價格計算價值
                "value_USDT": float(amount * token_info_price_usd),  # 以最新價格計算價值
                "unrealized_profits": float(amount * token_info_price_usd),  # 以最新價格計算價值
                "pnl": float(pnl_value),
                "pnl_percentage": float(pnl_percentage),
                "marketcap": float(avg_price * supply),
                "is_cleared": 0,
                "cumulative_cost": float(cost),
                "cumulative_profit": float(profit),
                "last_transaction_time": last_transaction_time,  # 最後一筆交易時間
                "time": get_update_time(),
                "avg_price": float(avg_price),  # 該錢包在該代幣上的買入均價
            }
            tx_data_list.append(remaining_tokens_summary)

        # 確保 balance_usd 是 Decimal 類型
        balance_usd = Decimal(str(balances[wallet]['balance_usd']))

        processed_wallets += 1
        wallet_end_time = time.time()
        wallet_duration = wallet_end_time - wallet_start_time
        logger.info(f"完成錢包 {wallet} 的數據分析，耗時: {wallet_duration:.2f}秒")

        return wallet, {
            'balance': float(balances[wallet]['balance']),
            'balance_usd': float(balance_usd),
            'chain': "BSC",
            'chain_id': 9006,
            'tag': None,
            'twitter_name': None,
            'twitter_username': None,
            'is_smart_wallet': True,
            'wallet_type': 0,
            'asset_multiple': asset_multiple,
            'token_list': token_list,
            'total_tokens': total_tokens,
            'total_transactions': total_transactions,            
            'stats_1d': metrics_1d,
            'stats_7d': metrics_7d,
            'stats_30d': metrics_30d,
            'pnl_pic_1d': pnl_pic_1d,
            'pnl_pic_7d': pnl_pic_7d,
            'pnl_pic_30d': pnl_pic_30d,
            'distribution_7d': distribution_7d,
            'distribution_30d': distribution_30d,
            'distribution_percentage_7d': distribution_percentage_7d,
            'distribution_percentage_30d': distribution_percentage_30d,
            'update_time': get_update_time(),
            'last_transaction': stats['last_transaction'],
            'is_active': True,
            'remaining_tokens': tx_data_list,
        }

    tasks = [process_wallet(wallet, stats) for wallet, stats in wallet_stats.items()]
    results = await asyncio.gather(*tasks)

    wallet_analysis = {wallet: analysis for wallet, analysis in results}
    return wallet_analysis

async def process_wallet_batch(wallet_addresses, main_session_factory, swap_session_factory, main_engine, logger, twitter_name: Optional[str] = None, twitter_username: Optional[str] = None):
    """處理一批錢包的交易並更新錢包摘要信息"""
    batch_start_time = time.time()
    results = {"processed": 0, "new": 0, "updated_wallets": 0}
    
    try:
        # 1. 批量獲取所有錢包的餘額
        bnb_price = await get_price()
        all_balances = await fetch_wallet_balances(wallet_addresses, bnb_price)
        
        # 2. 批量獲取現有交易記錄
        async with swap_session_factory() as session:
            await session.execute(text("SET search_path TO dex_query_v1;"))
            existing_signatures = await with_timeout(
                get_existing_signatures(session, wallet_addresses),
                timeout=60,
                description="獲取現有交易簽名"
            )
            logger.info(f"獲取了 {len(existing_signatures)} 筆現有交易簽名")
        
        # 3. 批量獲取交易記錄
        async with main_session_factory() as session:
            try:
                trades_by_wallet = await with_timeout(
                    get_wallet_trades_batch(session, wallet_addresses),
                    timeout=120,
                    description="獲取錢包交易記錄"
                )
                total_trades = sum(len(trades) for trades in trades_by_wallet.values())
                logger.info(f"從 dex_query_v1.trades 獲取了 {total_trades} 筆交易記錄")
            except Exception as e:
                logger.error(f"獲取錢包交易記錄時發生錯誤: {str(e)}")
                if "relation \"dex_query_v1.trades\" does not exist" in str(e):
                    logger.error("資料表 dex_query_v1.trades 不存在，請檢查資料庫設置")
                raise
        
        # 4. 篩選新交易並分組
        new_trades_by_wallet = {}
        processed_count = 0
        
        for wallet, wallet_trades in trades_by_wallet.items():
            processed_count += len(wallet_trades)
            new_trades = [trade for trade in wallet_trades if trade['tx_hash'] not in existing_signatures]
            if new_trades:
                new_trades_by_wallet[wallet] = new_trades
        
        total_new_trades = sum(len(trades) for trades in new_trades_by_wallet.values())
        logger.info(f"發現 {total_new_trades} 筆新交易記錄")
        results["processed"] = processed_count
        results["new"] = total_new_trades
        
        if total_new_trades == 0:
            return results
        
        # 5. 並行處理新交易
        async def process_wallet_trades(wallet, trades):
            try:
                async with swap_session_factory() as wallet_session:
                    sorted_trades = sorted(trades, key=lambda x: x['timestamp'] / 1000)
                    async with wallet_session.begin():
                        await process_trades_and_update_wallets(
                            wallet, 
                            sorted_trades, 
                            wallet_session, 
                            bnb_price, 
                            main_engine, 
                            logger,
                            all_balances.get(wallet, {}).get('balance_usd', Decimal('0'))
                        )
                return True
            except Exception as e:
                logger.error(f"處理錢包 {wallet} 交易時發生錯誤: {str(e)}")
                return False
        
        # 使用 asyncio.gather 並行處理多個錢包
        wallet_tasks = [
            process_wallet_trades(wallet, trades)
            for wallet, trades in new_trades_by_wallet.items()
        ]
        process_results = await asyncio.gather(*wallet_tasks)
        # 6. 批量更新錢包摘要
        async def update_wallet_summary(wallet):
            try:
                async with swap_session_factory() as history_session:
                    # 獲取交易歷史
                    await session.execute(text("SET search_path TO dex_query_v1;"))
                    Transaction.__table__.schema = "dex_query_v1"

                    query = select(Transaction).where(
                        Transaction.wallet_address == wallet
                    ).order_by(Transaction.transaction_time)
                    
                    result = await history_session.execute(query)
                    all_transactions = result.scalars().all()

                    if not all_transactions:
                        return False
                    
                    # 分析錢包數據
                    wallet_stats = await analyze_wallet_transactions(all_transactions)
                    print(wallet_stats)
                    wallet_analysis = await analyze_wallets_data({wallet: wallet_stats}, bnb_price)
                    
                    if wallet_analysis and wallet in wallet_analysis:
                        wallet_data = dict(wallet_analysis[wallet])
                        wallet_data['wallet_address'] = wallet
                        
                        pnl_percentage_30d = wallet_data['stats_30d']['pnl_percentage']
                        if pnl_percentage_30d < -50:
                            wallet_data['is_active'] = False
                        
                        async with swap_session_factory() as update_session:
                            async with update_session.begin():
                                await update_session.execute(text("SET search_path TO dex_query_v1;"))
                                logger.info(f"[WALLET] 準備寫入 WalletSummary: {wallet_data['wallet_address']}")
                                await write_wallet_data_to_db(
                                    update_session, 
                                    wallet_data, 
                                    "BSC", 
                                    twitter_name, 
                                    twitter_username
                                )
                                logger.info(f"[WALLET] 已寫入 WalletSummary: {wallet_data['wallet_address']}")
                        return True
            except Exception as e:
                logger.error(f"更新錢包 {wallet} 摘要時發生錯誤: {str(e)}")
                return False
        
        # 並行更新錢包摘要
        summary_tasks = [
            update_wallet_summary(wallet)
            for wallet in new_trades_by_wallet.keys()
        ]
        summary_results = await asyncio.gather(*summary_tasks)
        
        results["updated_wallets"] = sum(1 for r in summary_results if r)
        
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time
        logger.info(f"完成處理 {len(wallet_addresses)} 個錢包的批次，耗時: {batch_duration:.2f}秒")
        
    except Exception as e:
        logger.error(f"處理錢包批次時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
    
    return results

async def analyze_wallet_transactions(transactions):
    """分析錢包交易歷史，返回統計數據"""
    wallet_stats = {
        'buy': [],
        'sell': [],
        'pnl': Decimal(0),
        'last_transaction': None,
        'tokens': set(),
        'remaining_tokens': {},
    }
    
    for tx in transactions:
        action = tx.transaction_type
        token = tx.token_address
        value = Decimal(str(tx.value))
        amount = Decimal(str(tx.amount))
        timestamp = tx.transaction_time
        
        if action == 'buy':
            wallet_stats['remaining_tokens'].setdefault(token, {
                'amount': Decimal(0), 
                'cost': Decimal(0), 
                'profit': Decimal(0)
            })
            wallet_stats['remaining_tokens'][token]['amount'] += amount
            wallet_stats['remaining_tokens'][token]['cost'] += value
        elif action == 'sell':
            if token in wallet_stats['remaining_tokens']:
                wallet_stats['remaining_tokens'][token]['amount'] -= amount
                wallet_stats['remaining_tokens'][token]['profit'] += value
        
        wallet_stats[action].append({
            'token': token, 
            'value': float(value), 
            'timestamp': timestamp
        })
        wallet_stats['tokens'].add(token)
        wallet_stats['last_transaction'] = (
            timestamp if wallet_stats['last_transaction'] is None
            else max(wallet_stats['last_transaction'], timestamp)
        )
        wallet_stats['pnl'] += value if action == 'sell' else -value
    
    # 清理剩餘代幣
    wallet_stats['remaining_tokens'] = {
        token: data
        for token, data in wallet_stats['remaining_tokens'].items()
        if data['amount'] > 0
    }
    
    return wallet_stats

def setup_tasks(scheduler, main_session_factory, swap_session_factory, main_engine):
    """設置定時任務"""
    
    # 創建 Kafka 消費者實例
    kafka_consumer = KafkaConsumer(main_session_factory, swap_session_factory, main_engine)
    
    # 啟動 Kafka 消費者
    scheduler.add_job(
        kafka_consumer.start,
        'date',  # 立即執行一次
        id='kafka_consumer',
        replace_existing=True
    )
    logger.info("已設置 Kafka 消費者任務")

async def get_wallet_balance(wallet_address: str) -> Decimal:
    """使用 eth_getBalance 獲取錢包 BNB 餘額"""
    try:
        # 確保地址是 checksum 格式
        checksum_address = eth_to_checksum_address(wallet_address)
        # 獲取餘額（以 wei 為單位）
        balance_wei = w3.eth.get_balance(checksum_address)
        # 轉換為 BNB（除以 10^18）
        balance_bnb = Decimal(balance_wei) / Decimal(10**18)
        return balance_bnb
    except Exception as e:
        logger.error(f"獲取錢包 {wallet_address} 餘額時發生錯誤: {str(e)}")
        return Decimal('0')

class TokenBuyDataCache:
    def __init__(self):
        # {wallet_address: {token_address: {date: token_data}}}
        self._cache = {}
        self._last_flush_time = time.time()
        self._flush_interval = 300  # 5分鐘同步一次到資料庫
        self._dirty = set()  # 記錄需要更新的 (wallet_address, token_address, date)

    def get(self, wallet_address: str, token_address: str, date: datetime.date) -> dict:
        """獲取指定日期的代幣數據，如果不存在則返回默認值"""
        wallet_cache = self._cache.setdefault(wallet_address, {})
        token_cache = wallet_cache.setdefault(token_address, {})
        return token_cache.get(date, {
            "token_address": token_address,
            "date": date,
            "total_amount": Decimal('0'),
            "total_cost": Decimal('0'),
            "avg_buy_price": Decimal('0'),
            "position_opened_at": None,
            "historical_total_buy_amount": Decimal('0'),
            "historical_total_buy_cost": Decimal('0'),
            "historical_total_sell_amount": Decimal('0'),
            "historical_total_sell_value": Decimal('0'),
            "historical_avg_buy_price": Decimal('0'),
            "historical_avg_sell_price": Decimal('0'),
            "last_transaction_time": None,
            "realized_profit": Decimal('0'),
            "realized_profit_percentage": Decimal('0'),
            "total_buy_count": 0,
            "total_sell_count": 0,
            "total_holding_seconds": 0,
            "updated_at": get_utc8_time()
        })

    def update(self, wallet_address: str, token_address: str, date: datetime.date, action: str, amount: Decimal, value: Decimal, transaction_time: int):
        """更新緩存中的代幣數據"""
        token_data = self.get(wallet_address, token_address, date)
        current_time = transaction_time

        if action == 'buy':
            # 更新當前持倉數據
            token_data['total_amount'] += amount
            token_data['total_cost'] += value
            token_data['avg_buy_price'] = token_data['total_cost'] / token_data['total_amount'] if token_data['total_amount'] > 0 else Decimal('0')
            
            # 更新歷史買入數據
            token_data['historical_total_buy_amount'] += amount
            token_data['historical_total_buy_cost'] += value
            token_data['historical_avg_buy_price'] = (
                token_data['historical_total_buy_cost'] / token_data['historical_total_buy_amount']
                if token_data['historical_total_buy_amount'] > 0 else Decimal('0')
            )
            
            # 更新買入次數
            token_data['total_buy_count'] += 1
            
            # 更新開倉時間
            if token_data['position_opened_at'] is None:
                token_data['position_opened_at'] = current_time

        elif action == 'sell':
            # 計算賣出部分的成本和利潤
            if token_data['total_amount'] > 0:
                sell_ratio = min(amount / token_data['total_amount'], Decimal('1'))
                cost_basis = token_data['total_cost'] * sell_ratio
                profit = value - cost_basis
                profit_percentage = ((value / cost_basis) - Decimal('1')) * Decimal('100') if cost_basis > 0 else Decimal('-100')
                
                # 更新當前持倉數據
                token_data['total_amount'] -= amount
                if token_data['total_amount'] <= 0:
                    token_data['total_cost'] = Decimal('0')
                    token_data['avg_buy_price'] = Decimal('0')
                    token_data['position_opened_at'] = None
                else:
                    token_data['total_cost'] *= (1 - sell_ratio)
                    token_data['avg_buy_price'] = token_data['total_cost'] / token_data['total_amount']
            else:
                # 超賣情況
                token_data['total_amount'] -= amount
                profit = Decimal('0')
                profit_percentage = Decimal('-100')
            
            # 更新歷史賣出數據
            token_data['historical_total_sell_amount'] += amount
            token_data['historical_total_sell_value'] += value
            token_data['historical_avg_sell_price'] = (
                token_data['historical_total_sell_value'] / token_data['historical_total_sell_amount']
                if token_data['historical_total_sell_amount'] > 0 else Decimal('0')
            )
            
            # 更新已實現利潤
            token_data['realized_profit'] += profit
            token_data['realized_profit_percentage'] = max(profit_percentage, Decimal('-100'))
            
            # 更新賣出次數
            token_data['total_sell_count'] += 1

        # 更新最後交易時間
        token_data['last_transaction_time'] = current_time
        token_data['updated_at'] = get_utc8_time()

        # 更新緩存
        wallet_cache = self._cache.setdefault(wallet_address, {})
        token_cache = wallet_cache.setdefault(token_address, {})
        token_cache[date] = token_data
        self._dirty.add((wallet_address, token_address, date))

    async def maybe_flush(self, session):
        """如果距離上次同步超過間隔時間，則同步到資料庫"""
        current_time = time.time()
        if current_time - self._last_flush_time >= self._flush_interval and self._dirty:
            await self.flush(session)

    async def flush(self, session):
        """強制同步所有更改到資料庫"""
        try:
            for wallet_address, token_address, date in self._dirty:
                token_data = self._cache[wallet_address][token_address][date]
                await save_wallet_buy_data(
                    wallet_address=wallet_address,
                    token_address=token_address,
                    tx_data={**token_data, "date": date},
                    session=session,
                    chain="BSC",
                    auto_commit=True
                )
            self._dirty.clear()
            self._last_flush_time = time.time()
        except Exception as e:
            print(f"Error flushing cache to database: {e}")
            import traceback
            traceback.print_exc()

    def clear(self):
        """清空緩存"""
        self._cache.clear()
        self._dirty.clear()

# 創建全局緩存實例
token_buy_data_cache = TokenBuyDataCache()

if __name__ == "__main__":
    from config import DATABASE_URI, DATABASE_URI_SWAP_BSC
    import asyncio
    
    # 設置日誌格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger.info("Starting transaction sync service...")
    
    # 創建數據庫引擎和會話工廠
    main_engine = create_async_engine(DATABASE_URI, echo=False)
    swap_engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
    
    main_session_factory = sessionmaker(
        bind=main_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    swap_session_factory = sessionmaker(
        bind=swap_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    logger.info("Database connections established")
    
    # 創建調度器
    scheduler = AsyncIOScheduler()
    setup_tasks(scheduler, main_session_factory, swap_session_factory, main_engine)
    
    # 定義主要的異步函數
    async def main():
        # 啟動調度器
        scheduler.start()
        logger.info("Scheduler started")
        
        # 保持程序運行，直到按下 Ctrl+C
        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # 關閉調度器
            logger.info("Received shutdown signal, stopping scheduler...")
            scheduler.shutdown()
            logger.info("Scheduler stopped")
    
    # 運行主函數
    logger.info("Starting main loop...")
    asyncio.run(main())