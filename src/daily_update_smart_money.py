import os
import asyncio
import time
import json
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from functools import wraps
from decimal import Decimal
from balance import *
from models import *
from config import *
from sqlalchemy import text
from typing import List, Dict, Any
from token_info import *
from dotenv import load_dotenv
from daily_update_wallet import update_bsc_smart_money_data

load_dotenv()

# 获取 API 端点
API_ENDPOINT = os.getenv('WALLET_SYNC_API_ENDPOINT')

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wallet_analyzer.log')
    ]
)
logger = logging.getLogger(__name__)

now_utc_plus_8 = datetime.utcnow() + timedelta(hours=8)

def ensure_decimal(value):
    """確保返回 Decimal 類型值"""
    if value is None:
        return Decimal('0')
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))

# 交易時間修復函數
def fix_transaction_time(transaction_time):
    """確保交易時間在有效的分區範圍內"""
    # 將未來日期轉換為當前日期-1天（避免未來日期）
    current_time = int(time.time())
    if transaction_time > current_time:
        logger.debug(f"修正異常的未來時間戳: {transaction_time} -> {current_time - 86400}")
        return current_time - 86400  # 返回當前時間減去1天
    
    # 確保時間不早於分區範圍起始時間（假設2020年初）
    min_time = int(datetime(2020, 1, 1).timestamp())
    if transaction_time < min_time:
        logger.debug(f"修正異常的過去時間戳: {transaction_time} -> {min_time}")
        return min_time
    
    return transaction_time

async def clean_bsc_wallet_for_addresses_starting_with_zeros(session: AsyncSession):
    """清理BSC鏈中所有以"0x00"開頭的錢包地址"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        logger.info("開始清理 BSC 鏈數據...")

        # 清理所有地址以 "0x00" 開頭的錢包地址或交易量異常的錢包
        query = text("""
            DELETE FROM dex_query_v1.wallet
            WHERE wallet_address LIKE '0x00%' 
            OR total_transaction_num_30d > 2000
            OR pnl_percentage_30d > 10000
            OR pnl_percentage_7d > 10000
        """)
        result = await session.execute(query)
        await session.commit()

        # 獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logger.info(f"成功清理了 {rows_deleted} 條數據。")
        except Exception as e:
            logger.info("成功清理了以 '0x00' 開頭的錢包地址和異常錢包 (BSC)。")
    
    except Exception as e:
        logger.error(f"清理BSC異常錢包地址時出錯: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await session.rollback()
        except:
            pass

async def clean_bsc_wallet_transaction_for_deleted_wallets(session: AsyncSession):
    """清理BSC鏈中已刪除錢包的交易記錄"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        logger.info("開始清理 BSC 鏈已刪除錢包的交易記錄...")

        # 清理不存在於錢包表的交易記錄
        query = text("""
            DELETE FROM dex_query_v1.wallet_transaction
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
        """)
        result = await session.execute(query)
        await session.commit()

        # 獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logger.info(f"成功清理了 {rows_deleted} 條已刪除錢包的交易記錄 (BSC)。")
        except:
            logger.info("成功清理了已刪除錢包的交易記錄 (BSC)。")
    
    except Exception as e:
        logger.error(f"清理BSC已刪除錢包的交易記錄時出錯: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await session.rollback()
        except:
            pass

async def clean_bsc_inactive_wallets_holdings(session: AsyncSession):
    """清理BSC鏈中已刪除錢包的持倉記錄"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        logger.info("開始清理 BSC 鏈已刪除錢包的持倉記錄...")

        # 清理不存在於錢包表的持倉記錄
        query = text("""
            DELETE FROM dex_query_v1.wallet_holding
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
        """)
        result = await session.execute(query)
        await session.commit()

        # 獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logger.info(f"成功清理了 {rows_deleted} 條已刪除錢包的持倉記錄 (BSC)。")
        except:
            logger.info("成功清理了已刪除錢包的持倉記錄 (BSC)。")
    
    except Exception as e:
        logger.error(f"清理BSC已刪除錢包的持倉記錄時出錯: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await session.rollback()
        except:
            pass

async def clean_bsc_wallet_buy_data_for_deleted_wallets(session: AsyncSession):
    """清理BSC鏈中已刪除錢包的代幣購買數據"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        logger.info("開始清理 BSC 鏈已刪除錢包的代幣購買數據...")

        # 清理不存在於錢包表的代幣購買數據
        query = text("""
            DELETE FROM dex_query_v1.wallet_buy_data
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
        """)
        result = await session.execute(query)
        await session.commit()

        # 獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logger.info(f"成功清理了 {rows_deleted} 條已刪除錢包的代幣購買數據 (BSC)。")
        except:
            logger.info("成功清理了已刪除錢包的代幣購買數據 (BSC)。")
    
    except Exception as e:
        logger.error(f"清理BSC已刪除錢包的代幣購買數據時出錯: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await session.rollback()
        except:
            pass

async def clean_all_bsc_related_data():
    """執行所有BSC鏈相關的清理任務"""
    try:
        logger.info("開始執行BSC鏈相關數據的清理...")
        
        # 創建一個連接到BSC數據庫的會話
        engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
        async_session = sessionmaker(
            bind=engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        
        async with async_session() as session:
            # 依次執行各清理任務
            # await clean_bsc_wallet_for_addresses_starting_with_zeros(session)
            await clean_bsc_inactive_wallets_holdings(session)
            await clean_bsc_wallet_transaction_for_deleted_wallets(session)
            await clean_bsc_wallet_buy_data_for_deleted_wallets(session)
        
        logger.info("BSC鏈相關數據的清理已完成。")
    
    except Exception as e:
        logger.error(f"執行BSC鏈數據清理時發生錯誤: {e}")
        import traceback
        logger.error(traceback.format_exc())

# ----------------------------------------------------------代幣緩存邏輯-----------------------------------------------------------

class TokenBuyDataCache:
    def __init__(self, max_size=10000):
        self._cache = {}
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    async def get_token_data(self, wallet_address, token_address, session, chain):
        cache_key = f"{wallet_address}_{token_address}_{chain}"
        if cache_key in self._cache:
            self._hits += 1
            return self._cache[cache_key]
        self._misses += 1
        
        try:
            # 檢查會話狀態
            if not session.is_active:
                logger.warning(f"傳入的 session 不是活動狀態。使用新會話獲取代幣數據: wallet: {wallet_address}, token: {token_address}")
                # 如果會話不活動，創建一個新的會話
                engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
                async_session_factory = sessionmaker(
                    bind=engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session_factory() as new_session:
                    async with new_session.begin():
                        token_data = await get_token_buy_data(wallet_address, token_address, new_session, chain)
            else:
                # 使用原始會話
                token_data = await get_token_buy_data(wallet_address, token_address, session, chain)
            
            if len(self._cache) >= self._max_size:
                # 移除最舊的項目
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]

            self._cache[cache_key] = token_data
            return token_data
        except Exception as e:
            logger.error(f"獲取代幣數據失敗 - wallet: {wallet_address}, token: {token_address}: {str(e)}")
            return None
            
    def get_stats(self):
        """返回緩存統計信息"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate
        }

token_buy_data_cache = TokenBuyDataCache()

def log_execution_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"函數 '{func.__name__}' 執行耗時: {execution_time:.4f} 秒")
        return result
    return wrapper

def get_update_time():
    """獲取當前時間（UTC+8）格式化為字符串"""
    now_utc = now_utc_plus_8
    utc_plus_8 = now_utc + timedelta(hours=8)
    return utc_plus_8.strftime("%Y-%m-%d %H:%M:%S")

def make_naive_time(dt):
    """将带时区的时间转换为无时区时间"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

# ------------------------------------------------------------------------------------------------------------
class PerformanceConfig:
    MAX_CONCURRENT_TASKS = 10  # 並行處理的錢包數量
    CHUNK_SIZE = 200  # 每批處理的錢包數
    LOGGING_INTERVAL = 50  # 日誌輸出間隔
    TASK_TIMEOUT = 300  # 處理單個錢包的超時時間（秒）
    RETRY_DELAY = 0.1  # 批次間的延遲時間（秒）
    MAX_TX_COUNT = 2000  # 交易記錄數量上限

class WalletAnalyzer:
    def __init__(self, database_uri, swap_database_uri):
        """初始化錢包分析器"""
        # 創建數據庫引擎和連接池
        self.engine = create_async_engine(
            database_uri, 
            pool_size=100,
            max_overflow=50,
            pool_timeout=120,
            pool_recycle=1800,
            echo=False
        )
        self.swap_engine = create_async_engine(
            swap_database_uri,
            pool_size=100,
            max_overflow=50,
            pool_timeout=120,
            pool_recycle=1800,
            echo=False
        )
        
        # 創建會話工廠
        self.async_session_factory = sessionmaker(
            bind=self.engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        self.swap_async_session_factory = sessionmaker(
            bind=self.swap_engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        
        # 初始化信號量和日誌
        self.semaphore = asyncio.Semaphore(PerformanceConfig.MAX_CONCURRENT_TASKS)
        self.cache_hit_stats = []
        
    async def get_wallet_trades_count(self, wallet_address, session):
        """獲取錢包交易記錄數量"""
        await session.execute(text("SET search_path TO dex_query;"))
        query = """
        SELECT COUNT(*) 
        FROM dex_query.trades
        WHERE chain_id = 9006
        AND dex ILIKE '%pancake%'
        AND signer = :wallet_address
        AND (
            token_in IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                        '0x55d398326f99059fF775485246999027B3197955', 
                        '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            OR 
            token_out IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                         '0x55d398326f99059fF775485246999027B3197955', 
                         '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
        )
        """
        result = await session.execute(text(query), {'wallet_address': wallet_address})
        return result.scalar()

    async def get_wallet_trades(self, session, wallet_address):
        """獲取錢包交易記錄"""
        await session.execute(text("SET search_path TO dex_query;"))
        query = """
        SELECT * 
        FROM dex_query.trades
        WHERE chain_id = 9006
        AND dex ILIKE '%pancake%'
        AND signer = :wallet_address
        AND (
            token_in IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                        '0x55d398326f99059fF775485246999027B3197955', 
                        '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            OR 
            token_out IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                         '0x55d398326f99059fF775485246999027B3197955', 
                         '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
        )
        ORDER BY created_at ASC
        """
        result = await session.execute(text(query), {'wallet_address': wallet_address})
        return result.mappings().all()

    async def get_unique_wallets(self, session):
        """獲取所有唯一錢包地址"""
        await session.execute(text("SET search_path TO dex_query;"))
        query = """
        SELECT DISTINCT signer 
        FROM dex_query.trades
        WHERE chain_id = 9006
        AND dex ILIKE '%pancake%'
        AND signer NOT LIKE '0x00%'
        AND (
            token_in IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                        '0x55d398326f99059fF775485246999027B3197955', 
                        '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            OR 
            token_out IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                         '0x55d398326f99059fF775485246999027B3197955', 
                         '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
        )
        """
        result = await session.execute(text(query))
        return [row[0] for row in result]

    @log_execution_time
    async def process_wallet(self, wallet_address, bnb_price):
        """處理單個錢包的主要邏輯"""
        try:
            # 檢查交易數量
            async with self.async_session_factory() as session:
                async with session.begin():
                    await session.execute(text("SET search_path TO dex_query;"))
                    tx_count = await self.get_wallet_trades_count(wallet_address, session)
                    
                    # 如果交易數量超過限制，則跳過
                    if tx_count > PerformanceConfig.MAX_TX_COUNT:
                        logger.info(f"跳過處理錢包 {wallet_address} - 交易數量 ({tx_count}) 超過限制")
                        return None
                    
                    # 如果沒有交易，則跳過
                    if tx_count == 0:
                        return None
                    
                    # 獲取交易記錄
                    logger.info(f"處理錢包 {wallet_address} - 交易數量: {tx_count}")
                    wallet_trades = await self.get_wallet_trades(session, wallet_address)
            
            # 計算錢包統計數據
            wallet_stats = await daily_calculate_wallet_stats(
                wallet_trades, 
                bnb_price, 
                self.swap_async_session_factory
            )
                        
            # 分析錢包資料
            wallet_analysis = await analyze_wallets_data(wallet_stats, bnb_price)
            
            # 篩選聰明錢包
            filtered_analysis = {}
            for wallet, data in wallet_analysis.items():
                if (data['total_tokens'] >= 15 and
                    data['stats_30d']['win_rate'] > 50 and
                    data['stats_30d']['win_rate'] != 100 and
                    data['stats_30d']['pnl'] > 0 and
                    float(data['asset_multiple']) > 0.3 and
                    data['stats_30d']['total_transaction_num'] > 20 and
                    float(data['balance_usd']) > 0 and
                    data['stats_30d']['sell_num'] > 0 and
                    data['stats_7d']['sell_num'] > 0):
                    filtered_analysis[wallet] = data
                    logger.info(f"發現聰明錢包: {wallet}")
            
            # 保存聰明錢包數據
            if filtered_analysis:
                async with self.swap_async_session_factory() as save_session:
                    await process_and_save_wallets(filtered_analysis, save_session, "BSC")
                return filtered_analysis
            else:
                return None
            
        except asyncio.TimeoutError:
            logger.error(f"處理錢包 {wallet_address} 超時")
            return None
        except Exception as e:
            logger.error(f"處理錢包 {wallet_address} 錯誤: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return None

    async def process_wallet_with_semaphore(self, wallet, bnb_price, processed_count, total_count):
        """使用信號量限制並行處理錢包"""
        try:
            async with self.semaphore:
                start_time = time.time()
                
                # 第一步：獲取並處理錢包的交易資料
                async with self.async_session_factory() as session:
                    async with session.begin():
                        await session.execute(text("SET search_path TO dex_query;"))
                        
                        # 先檢查交易數量
                        tx_count = await self.get_wallet_trades_count(wallet, session)
                        
                        # 如果交易數量超過限制或沒有交易，則跳過
                        if tx_count == 0 or tx_count > PerformanceConfig.MAX_TX_COUNT:
                            logger.debug(f"跳過錢包 {wallet}: 交易數量 = {tx_count}")
                            return processed_count
                        
                        # 獲取交易記錄
                        wallet_trades = await self.get_wallet_trades(session, wallet)
                
                # 沒有交易記錄，跳過處理
                if not wallet_trades:
                    return processed_count
                
                # 第二步：分析錢包數據
                try:
                    wallet_stats = await daily_calculate_wallet_stats(
                        wallet_trades, 
                        bnb_price, 
                        self.swap_async_session_factory
                    )
                    
                    wallet_analysis = await analyze_wallets_data(wallet_stats, bnb_price)
                    
                    # 若分析成功
                    if wallet_analysis:
                        # 應用聰明錢包篩選條件
                        filtered_wallet_analysis = {}
                        for wallet_addr, data in wallet_analysis.items():
                            if (data['total_tokens'] >= 15 and 
                                data['stats_30d']['win_rate'] > 50 and
                                data['stats_30d']['win_rate'] != 100 and
                                data['stats_30d']['pnl'] > 0 and
                                float(data['asset_multiple']) > 0.3 and
                                data['stats_30d']['total_transaction_num'] > 20 and
                                float(data['balance_usd']) > 0 and
                                data['stats_30d']['sell_num'] > 0 and
                                data['stats_7d']['sell_num'] > 0):
                                
                                filtered_wallet_analysis[wallet_addr] = data
                                logger.info(f"發現聰明錢包: {wallet_addr}")
                        
                        # 第三步：如果是聰明錢包，保存數據
                        if filtered_wallet_analysis:
                            # 保存錢包摘要數據
                            try:
                                async with self.swap_async_session_factory() as save_session:
                                    await process_and_save_wallets(
                                        filtered_wallet_analysis, 
                                        save_session, 
                                        chain="BSC"
                                    )
                            except Exception as e:
                                logger.error(f"保存錢包摘要失敗: {wallet} - {str(e)}")
                except Exception as e:
                    logger.error(f"分析錢包數據失敗: {wallet} - {str(e)}")
                    import traceback
                    logger.debug(traceback.format_exc())
                
                # 計算處理時間
                end_time = time.time()
                processing_time = end_time - start_time
                
                # 更新處理計數
                processed_count += 1
                
                # 定期輸出進度
                if processed_count % PerformanceConfig.LOGGING_INTERVAL == 0:
                    # 獲取緩存統計
                    cache_stats = token_buy_data_cache.get_stats()
                    self.cache_hit_stats.append(cache_stats)
                    
                    # 計算進度百分比和預估剩餘時間
                    progress_pct = processed_count / total_count * 100
                    remaining_wallets = total_count - processed_count
                    avg_time_per_wallet = processing_time / PerformanceConfig.LOGGING_INTERVAL
                    est_remaining_time = avg_time_per_wallet * remaining_wallets / 60
                    
                    logger.info(
                        f"進度: {processed_count}/{total_count} ({progress_pct:.2f}%) | "
                        f"平均處理時間: {avg_time_per_wallet:.3f}秒/錢包 | "
                        f"預計剩餘時間: {est_remaining_time:.2f}分鐘 | "
                        f"緩存命中率: {cache_stats['hit_rate']*100:.2f}%"
                    )
                
                return processed_count
                
        except asyncio.TimeoutError:
            logger.warning(f"處理錢包超時: {wallet}")
            return processed_count
        except Exception as e:
            logger.error(f"處理錢包錯誤: {wallet} - {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return processed_count

    @log_execution_time
    async def analyze_wallets(self, bnb_price):
        """主分析流程"""
        start_time = time.time()
        
        # 獲取所有錢包地址
        async with self.async_session_factory() as session:
            async with session.begin():
                await session.execute(text("SET search_path TO dex_query;"))
                unique_wallets = await self.get_unique_wallets(session)
        
        total_wallets = len(unique_wallets)
        logger.info(f"開始分析 {total_wallets} 個錢包")
        
        # 初始化計數器
        processed_wallets = 0
        successful_wallets = 0
        smart_money_wallets = 0
        
        # 分批處理錢包
        for i in range(0, total_wallets, PerformanceConfig.CHUNK_SIZE):
            batch = unique_wallets[i:i + PerformanceConfig.CHUNK_SIZE]
            batch_num = i // PerformanceConfig.CHUNK_SIZE + 1
            total_batches = (total_wallets + PerformanceConfig.CHUNK_SIZE - 1) // PerformanceConfig.CHUNK_SIZE
            
            logger.info(f"處理批次 {batch_num}/{total_batches} ({len(batch)} 個錢包)")
            
            # 創建任務列表
            tasks = [
                self.process_wallet_with_semaphore(wallet, bnb_price, processed_wallets + idx, total_wallets)
                for idx, wallet in enumerate(batch)
            ]
            
            # 收集結果
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 更新處理進度
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"批處理錯誤: {str(result)}")
                    elif result is not None:
                        processed_wallets = max(processed_wallets, result)
                
                # 批次間短暫暫停，避免資源過度使用
                await asyncio.sleep(PerformanceConfig.RETRY_DELAY)
                
            except Exception as e:
                logger.error(f"批處理整體失敗: {str(e)}")
        
        # 計算總耗時
        end_time = time.time()
        total_time = end_time - start_time
        
        # 輸出最終統計
        logger.info(
            f"\n==== 任務完成 ====\n"
            f"總處理時間: {total_time/60:.2f} 分鐘\n"
            f"處理錢包數: {processed_wallets} 個\n"
            f"緩存統計: {token_buy_data_cache.get_stats()}"
        )

async def daily_calculate_wallet_stats(data, bnb_price, session_factory):
    """處理交易數據並計算錢包統計信息，使用 session_factory 而不是單一會話"""
    wallet_stats = {}

    # 主要處理邏輯移到獨立函數，以便更好地控制事務範圍
    async def process_transactions_batch(batch_data):
        nonlocal wallet_stats
        
        for row in batch_data:
            try:
                # 為每個交易使用單獨的會話
                async with session_factory() as session:
                    async with session.begin():
                        await session.execute(text("SET search_path TO dex_query;"))
                        transaction_result = await process_single_transaction(row, bnb_price, session)
                        
                        if transaction_result is None:
                            continue

                        wallet_address = row['signer']
                        if wallet_address not in wallet_stats:
                            wallet_stats[wallet_address] = {
                                'buy': [],
                                'sell': [],
                                'pnl': Decimal(0),
                                'last_transaction': None,
                                'tokens': set(),
                                'remaining_tokens': {},
                            }

                        # 更新錢包統計數據 - 純計算操作，不涉及數據庫
                        await update_wallet_stats(wallet_stats[wallet_address], transaction_result)
                    
            except Exception as e:
                logger.error(f"交易處理錯誤: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
                # 繼續處理下一個交易，不中斷整個批次
        
        # 清理剩餘代幣 - 在事務外部進行純計算操作
        for stats in wallet_stats.values():
            stats['remaining_tokens'] = {
                token: data
                for token, data in stats['remaining_tokens'].items()
                if data['amount'] > 0
            }

    # 將數據分批處理，避免事務過長
    BATCH_SIZE = 50
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        await process_transactions_batch(batch)

    return wallet_stats

async def update_wallet_stats(stats, transaction_result):
    """更新錢包統計數據，純計算函數，不涉及數據庫操作"""
    action = transaction_result['action']
    token = transaction_result['token']
    value = transaction_result['value']  # 已經是 Decimal
    amount = transaction_result['amount']  # 已經是 Decimal
    timestamp = int(transaction_result['timestamp']) if transaction_result['timestamp'] > 0 else 0

    # 更新剩餘代幣數據
    if action == 'buy':
        stats['remaining_tokens'].setdefault(token, {
            'amount': Decimal(0), 'cost': Decimal(0), 'profit': Decimal(0)
        })
        stats['remaining_tokens'][token]['amount'] += amount
        stats['remaining_tokens'][token]['cost'] += value
    elif action == 'sell':
        if token in stats['remaining_tokens']:
            stats['remaining_tokens'][token]['amount'] -= amount
            stats['remaining_tokens'][token]['profit'] += value

    stats[action].append({'token': token, 'value': float(value), 'timestamp': timestamp})
    stats['tokens'].add(token)
    stats['last_transaction'] = (
        timestamp if stats['last_transaction'] is None
        else max(stats['last_transaction'], timestamp)
    )
    stats['pnl'] += value if action == 'sell' else -value

# 修改過的 process_single_transaction 函數
async def process_single_transaction(row, bnb_price, session):
    """處理單筆交易，確保不創建新的事務"""
    try:
        # 檢查會話狀態
        if not session.is_active:
            logger.warning(f"在 process_single_transaction 中，會話不是活動狀態")
            return None
            
        # 直接調用事務邏輯
        return await process_transaction_logic(row, bnb_price, session)
    except Exception as e:
        logger.error(f"交易處理錯誤: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None

async def process_transaction_logic(row, bnb_price, session):
    """處理單個交易的邏輯，並修復時間戳問題"""
    try:
        # 1. 獲取基礎數據（非數據庫操作）
        wallet_address = row['signer']
        
        # 確保 bnb_price 是 Decimal 類型
        if not isinstance(bnb_price, Decimal):
            bnb_price = ensure_decimal(bnb_price)
            
        # 獲取餘額數據 - balance.py 返回的已經是 Decimal 類型，所以不需要轉換
        balances = await fetch_wallet_balances([wallet_address], bnb_price)
        
        token_in = row['token_in']
        token_out = row['token_out']
        amount_in = Decimal(str(row['amount_in'])) / Decimal(str(10 ** row['decimals_in']))
        amount_in = ensure_decimal(amount_in)
        amount_out = Decimal(str(row['amount_out'])) / Decimal(str(10 ** row['decimals_out']))
        amount_out = ensure_decimal(amount_out)
        price_usd = Decimal(str(row['price_usd'])) if row['price_usd'] else Decimal('0')
        price_usd = ensure_decimal(price_usd)
        transaction_time = row.get('created_at')
        
        if transaction_time is None:
            logger.error(f"缺少交易時間: {row}")
            return None  # 跳过无效数据
            
        # 處理時間戳並確保在有效的分區範圍內
        transaction_time = int(transaction_time / 1000) if transaction_time > 9999999999 else int(transaction_time)
        transaction_time = fix_transaction_time(transaction_time)  # 修復時間戳
        
        tx_hash = row['tx_hash']
        holding_percentage = Decimal('100')

        # 2. 確定交易類型和代幣地址
        # 穩定幣和BNB地址
        stablecoin_addresses = (
            '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',  # WBNB
            '0x55d398326f99059fF775485246999027B3197955',  # USDT
            '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'   # USDC
        )
        
        if token_in in stablecoin_addresses:
            if token_in == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c':
                value = amount_in * bnb_price
            else:
                value = amount_in * Decimal('1')
            action = 'buy'
            token_address = token_out
        else:
            if token_out == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c':
                value = amount_out * bnb_price
            else:
                value = amount_out * Decimal('1')
            action = 'sell'
            token_address = token_in

        # 3. 從緩存或數據庫獲取代幣數據
        token_buy_data = await token_buy_data_cache.get_token_data(wallet_address, token_address, session, "BSC")
            
        if token_buy_data is None:
            # 創建初始數據
            token_buy_data = {
                "token_address": token_address,
                "avg_buy_price": Decimal('0'),
                "total_amount": Decimal('0'),
                "total_cost": Decimal('0')
            }

        # 4. 確保 token_buy_data 中的數值是 Decimal 類型
        for key in ["total_amount", "total_cost", "avg_buy_price"]:
            if key in token_buy_data and token_buy_data[key] is not None:
                if not isinstance(token_buy_data[key], Decimal):
                    token_buy_data[key] = ensure_decimal(token_buy_data[key])

        # 5. 處理代幣數據 - 純計算操作
        if action == 'buy':
            token_data = _process_buy_token_data(token_buy_data, amount_out, value)
        else:
            token_data = _process_sell_token_data(token_buy_data, amount_in, value)
        
        # 6. 準備 tx_data - 確保這裡定義 tx_data
        tx_data = {
            "avg_buy_price": token_data.get("avg_buy_price", Decimal('0')),
            "token_address": token_address,
            "total_amount": token_data["total_amount"],
            "total_cost": token_data["total_cost"]
        }

        # 7. 計算持有百分比
        if action == 'buy':
            # balance['balance_usd'] 直接從 balance.py 返回，已經是 Decimal 類型
            balance_usd = ensure_decimal(balances[wallet_address]['balance_usd'])
            if balance_usd > Decimal('0'):
                holding_percentage = min((value / (value + balance_usd)) * Decimal('100'), Decimal('100'))
        else:
            if token_data["total_amount"] > Decimal('0'):
                holding_percentage = min((amount_out / (amount_out + token_data["total_amount"])) * Decimal('100'), Decimal('100'))

        # 8. 保存代幣購買數據
        tx_save_result = await save_wallet_buy_data(wallet_address, tx_data, token_address, session, "BSC")
        if not tx_save_result:
            logger.warning(f"保存代幣購買數據失敗 - wallet: {wallet_address}, token: {token_address}")
            
        # 9. 獲取代幣信息
        token_info = await TokenInfoFetcher.get_token_info(token_address.lower())
        name = token_info[0].get("name", None)
        symbol = token_info[0].get("symbol", None)
        icon = token_info[0].get("uri", None)
        supply = await get_token_supply(token_address.lower())
        # 確保 supply 是 Decimal 類型
        supply_decimal = ensure_decimal(supply) if supply else Decimal('0')
        marketcap = price_usd * supply_decimal
        
        # 檢查 marketcap 是否超過異常值，如果超過則設置為 0
        if marketcap > Decimal('10000000000'):
            logger.warning(f"代币 {token_address} 的 marketcap 值異常: {marketcap}，設置為 0")
            marketcap = Decimal('0')

        # 10. 準備交易數據 - balance['balance_usd'] 已經是 Decimal 類型
        balance_usd = balances.get(wallet_address, {}).get('balance_usd', Decimal('0'))
        wallet_balance = value + balance_usd

        # 轉換為 float 用於數據庫存儲
        transaction = {
            "wallet_address": wallet_address,
            "wallet_balance": float(wallet_balance),
            "token_address": token_address,
            "token_icon": icon,
            "token_name": symbol,
            "price": float(price_usd) if price_usd else 0,
            "amount": float(amount_out if action == 'buy' else amount_in),
            "marketcap": float(marketcap),
            "value": float(value),
            "holding_percentage": float(holding_percentage),
            "chain": "BSC",
            "realized_profit": float(token_data.get("pnl", Decimal('0'))) if action == 'sell' else 0,
            "realized_profit_percentage": float(token_data.get("pnl_percentage", Decimal('0'))) if action == 'sell' else 0,
            "transaction_type": action,
            "transaction_time": transaction_time,  # 已修復的時間戳
            "time": now_utc_plus_8,
            "from_token_address": token_in,
            "from_token_symbol": '',
            "from_token_amount": float(amount_in),
            "dest_token_address": token_out,
            "dest_token_symbol": '',
            "dest_token_amount": float(amount_out)
        }

        # 11. 保存交易記錄
        tx_result = await save_past_transaction(session, transaction, wallet_address, tx_hash, 'BSC')   
        if not tx_result:
            logger.warning(f"保存交易記錄失敗 - wallet: {wallet_address}, tx_hash: {tx_hash}")

        # 12. 返回結果用於統計
        result = {
            'action': action,
            'token': token_out if action == 'buy' else token_in,
            'value': value,  # 保持為 Decimal 給下面的計算使用
            'amount': amount_out if action == 'buy' else amount_in,  # 保持為 Decimal
            'timestamp': transaction_time  # 已修復的時間戳
        }
        
        return result
        
    except Exception as e:
        logger.error(f"處理交易邏輯錯誤: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None

def _process_sell_token_data(token_data, amount, value):
    """處理賣出時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    # 確保所有數值都是 Decimal 類型
    total_amount = ensure_decimal(token_data["total_amount"])
    amount = ensure_decimal(amount)
    value = ensure_decimal(value)
    total_cost = ensure_decimal(token_data["total_cost"])
    
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

def _process_buy_token_data(token_data, amount, value):
    """處理買入時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    # 確保所有數值都是 Decimal 類型
    total_amount = ensure_decimal(token_data["total_amount"])
    total_cost = ensure_decimal(token_data["total_cost"])
    amount = ensure_decimal(amount)
    value = ensure_decimal(value)
    
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

async def fetch_token_info_for_wallets(remaining_tokens):
    """批量獲取代幣信息，並確保數值字段可以安全地轉換為 Decimal"""
    tasks = [TokenInfoFetcher.get_token_info(token) for token in remaining_tokens]
    token_info_results = await asyncio.gather(*tasks)
    return dict(zip(remaining_tokens, [result.get(token, {}) for token, result in zip(remaining_tokens, token_info_results)]))

async def process_and_save_wallets(filtered_wallet_analysis, session, chain):
    """處理和保存錢包數據到數據庫"""
    for wallet_address, wallet_data in filtered_wallet_analysis.items():
        wallet_data['wallet_address'] = wallet_address
        tx_data_list = wallet_data.get('remaining_tokens', [])
        try:
            # 保存錢包資料
            async with session.begin():
                await session.execute(text("SET search_path TO dex_query;"))
                await write_wallet_data_to_db(session, wallet_data, chain)
            
            # 保存持倉資料（如果有）
            if tx_data_list:
                async with session.begin():
                    await session.execute(text("SET search_path TO dex_query;"))
                    await save_holding(tx_data_list, wallet_address, session, chain)
                    
        except Exception as e:
            logger.error(f"保存錢包資料失敗: {wallet_address} - {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())

async def analyze_wallets_data(wallet_stats, bnb_price):
    """
    分析钱包数据，确保正确处理类型转换，避免 float 和 Decimal 混用
    """
    wallet_analysis = {}
    current_time = int(time.time())  # 当前时间戳（秒）

    async def process_wallet(wallet, stats):
        # 確保所有數值操作使用同一類型
        # 將輸入數據轉換為 Decimal 類型
        total_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'])
        total_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'])
        pnl = stats['pnl']
        num_buy = len(stats['buy'])
        num_sell = len(stats['sell'])
        total_transactions = num_buy + num_sell

        # 盈利代币数量和胜率
        profitable_tokens = 0
        for token in stats['tokens']:
            token_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'] if tx['token'] == token)
            token_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'] if tx['token'] == token)
            
            if token_sell > token_buy:
                profitable_tokens += 1

        total_tokens = len(stats['tokens'])
        win_rate = min((profitable_tokens / total_tokens) * 100, Decimal('100')) if total_tokens > 0 else Decimal('0')

        # 时间范围
        one_day_ago = current_time - 24 * 60 * 60
        seven_days_ago = current_time - 7 * 24 * 60 * 60
        thirty_days_ago = current_time - 30 * 24 * 60 * 60

        # 计算函数
        def calculate_metrics(buy, sell, start_time):
            # 確保所有數值操作使用 Decimal 類型
            buy_filtered = [tx for tx in buy if tx['timestamp'] >= start_time]
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            
            total_buy_value = sum(Decimal(str(tx['value'])) for tx in buy_filtered)
            total_sell_value = sum(Decimal(str(tx['value'])) for tx in sell_filtered)
            
            num_buy = len(buy_filtered)
            num_sell = len(sell_filtered)
            
            pnl_value = total_sell_value - total_buy_value
            
            # 避免除以零
            pnl_percentage = Decimal('0')
            if total_buy_value > Decimal('0'):
                pnl_percentage = max((pnl_value / total_buy_value) * 100, Decimal('-100'))
            
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
        asset_multiple = metrics_30d['pnl_percentage'] if metrics_30d['pnl_percentage'] != 0 else 0
        
        # 计算最后活跃的前三个代币
        all_tokens = stats['buy'] + stats['sell']
        sorted_tokens = sorted(all_tokens, key=lambda tx: tx['timestamp'], reverse=True)
        token_list = ",".join(
            list(dict.fromkeys(tx['token'] for tx in sorted_tokens))[:3]
        )

        # 计算 pnl_pic
        def calculate_daily_pnl(buy, sell, days):
            """
            计算每日 PNL 数据。
            """
            daily_pnl = []
            for day in range(days):
                start_time = current_time - (day + 1) * 24 * 60 * 60
                end_time = current_time - day * 24 * 60 * 60
                
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
        balances = await fetch_wallet_balances([wallet], bnb_price)

        # 处理当前持仓代币
        remaining_tokens = stats['remaining_tokens']
        token_info_results = await fetch_token_info_for_wallets(remaining_tokens)
        tx_data_list = []
        
        for token, data in remaining_tokens.items():
            # 獲取查詢結果
            token_info = token_info_results.get(token, {})
            
            # 確保所有數值操作使用 Decimal 類型
            amount = ensure_decimal(data.get("amount", 0))
            profit = ensure_decimal(data.get("profit", 0))
            cost = ensure_decimal(data.get("cost", 0))

            # 計算最後一筆交易時間
            token_transactions = [tx for tx in (stats['buy'] + stats['sell']) if tx['token'] == token]
            if not token_transactions:
                continue
                
            last_transaction_time = max(tx['timestamp'] for tx in token_transactions)

            # 計算買入均價
            buy_transactions = [
                {
                    'amount': ensure_decimal(remaining_tokens[token]['amount']),
                    'cost': ensure_decimal(remaining_tokens[token]['cost'])
                } 
                for token in remaining_tokens
            ]

            total_buy_value = sum(tx['cost'] for tx in buy_transactions)
            total_buy_amount = sum(tx['amount'] for tx in buy_transactions if tx['amount'] > Decimal('0'))
            
            avg_price = Decimal('0')
            if total_buy_amount > Decimal('0'):
                avg_price = total_buy_value / total_buy_amount
                
            token_info_price_native = ensure_decimal(token_info.get("priceNative", "0"))
            token_info_price_usd = ensure_decimal(token_info.get("priceUsd", "0"))
            token_info_marketcap = ensure_decimal(token_info.get("marketcap", "0"))

            # 計算 pnl 和 pnl_percentage
            pnl_value = profit - cost
            pnl_percentage = Decimal('0')
            if cost > Decimal('0'):
                pnl_percentage = (profit - cost) / cost * 100

            # 統計結果 - 轉換為 float 用於數據庫存儲
            remaining_tokens_summary = {
                "token_address": token,
                "token_name": token_info.get("symbol", None),
                "token_icon": token_info.get("url", None),
                "chain": "BSC",
                "amount": float(amount),
                "value": float(amount * token_info_price_native),  # 以最新價格計算價值
                "value_USDT": float(amount * token_info_price_usd),  # 以最新價格計算價值
                "unrealized_profits": float(amount * token_info_price_usd),  # 以最新價格計算價值
                "pnl": float(pnl_value),
                "pnl_percentage": float(pnl_percentage),
                "marketcap": float(token_info_marketcap),
                "is_cleared": 0,
                "cumulative_cost": float(cost),
                "cumulative_profit": float(profit),
                "last_transaction_time": last_transaction_time,
                "time": make_naive_time(datetime.now()),
                "avg_price": float(avg_price),
            }
            tx_data_list.append(remaining_tokens_summary)

        balance_usd = ensure_decimal(balances[wallet]['balance_usd'])
        
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

    wallet_analysis = {wallet: analysis for wallet, analysis in results if wallet and analysis}
    return wallet_analysis

async def get_token_supply(token_address: str) -> Decimal:
    """获取代币供应量"""
    try:
        token_info = await TokenInfoFetcher.get_token_info(token_address)
        if token_info and token_address in token_info:
            supply = token_info[token_address].get("totalSupply", "0")
            return Decimal(str(supply))
        return Decimal('1000000000')
    except Exception as e:
        print(f"Error getting token supply: {e}")
        return Decimal('1000000000')

# 創建獨立的會話工廠函數
def create_async_session_for_supply():
    """創建用於查詢供應量的會話"""
    engine = create_async_engine(DATABASE_URI, echo=False, pool_recycle=1800)
    async_session = sessionmaker(
        bind=engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    return async_session()

def create_async_session_for_token():
    """創建用於查詢代幣信息的會話"""
    engine = create_async_engine(DATABASE_URI, echo=False, pool_recycle=1800)
    async_session = sessionmaker(
        bind=engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    return async_session()

# --------------------------------------------------------------------------------------daily_update_wallet---------------------------------------------------------------------------------------------------
async def _push_wallet_data_to_api(wallet_data):
    """推送钱包数据到后端 API"""
    if not API_ENDPOINT:
        logging.error("未配置 API 端点，无法推送数据")
        return False

    headers = {"Content-Type": "application/json"}
    logging.info(f"准备推送 {len(wallet_data)} 个钱包数据到 API: {API_ENDPOINT}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(API_ENDPOINT, headers=headers, json=wallet_data) as response:
                if response.status == 200:
                    logging.info("成功推送数据到 API")
                    return True
                else:
                    logging.error(f"推送失败，状态码: {response.status}")
                    return False
    except Exception as e:
        logging.error(f"推送到 API 时发生错误: {e}")
        return False

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

async def main():
    """主程式入口點"""
    # 配置日誌
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    #     handlers=[
    #         logging.StreamHandler(),
    #         logging.FileHandler(f'smart_money_{datetime.now().strftime("%Y%m%d")}.log')
    #     ]
    # )
    
    logger.info("開始執行智能錢包分析任務")
    
    # 獲取 BNB 價格
    try:
        bnb_price = await get_price()
        logger.info(f"獲取到 BNB 價格: ${bnb_price}")
    except Exception as e:
        logger.error(f"獲取 BNB 價格失敗: {str(e)}")
        return
        
    # 清理資料庫中的無效資料
    try:
        logger.info("開始清理資料庫中的無效數據")
        await clean_all_bsc_related_data()
    except Exception as e:
        logger.error(f"清理資料庫失敗: {str(e)}")
    
    # 初始化錢包分析器並執行分析
    # analyzer = WalletAnalyzer(DATABASE_URI, DATABASE_URI_SWAP_BSC)
    # await analyzer.analyze_wallets(bnb_price)
    
    # 更新 BSC 智能钱包数据
    try:
        logger.info("開始更新 BSC 智能錢包數據")
        await update_bsc_smart_money_data()
        logger.info("BSC 智能錢包數據更新完成")
    except Exception as e:
        logger.error(f"更新 BSC 智能錢包數據失敗: {str(e)}")
    
    logger.info("智能錢包分析任務完成")

async def execute_daily_task():
    """執行每日更新任務"""
    try:
        logger.info("=== 開始執行每日智能錢包分析任務 ===")
        
        # 獲取 BNB 價格
        try:
            bnb_price = await get_price()
            logger.info(f"獲取到 BNB 價格: ${bnb_price}")
        except Exception as e:
            logger.error(f"獲取 BNB 價格失敗: {str(e)}")
            return
        
        # 清理資料庫中的無效資料
        try:
            logger.info("開始清理資料庫中的無效數據")
            await clean_all_bsc_related_data()
        except Exception as e:
            logger.error(f"清理資料庫失敗: {str(e)}")
        
        # 更新 BSC 智能錢包數據
        try:
            logger.info("開始更新 BSC 智能錢包數據")
            await update_bsc_smart_money_data()
            logger.info("BSC 智能錢包數據更新完成")
        except Exception as e:
            logger.error(f"更新 BSC 智能錢包數據失敗: {str(e)}")
        
        logger.info("=== 每日智能錢包分析任務完成 ===")
        
    except Exception as e:
        logger.error(f"執行每日任務時發生錯誤: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def get_next_midnight_utc8():
    """獲取下一個UTC+8的凌晨00:00時間"""
    # 獲取當前UTC+8時間
    now_utc8 = datetime.utcnow() + timedelta(hours=8)
    
    # 計算下一個凌晨00:00
    next_midnight = now_utc8.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 如果當前時間已經過了今天的凌晨，則計算明天的凌晨
    if now_utc8 >= next_midnight:
        next_midnight += timedelta(days=1)
    
    # 轉換回UTC時間
    next_midnight_utc = next_midnight - timedelta(hours=8)
    
    return next_midnight_utc

async def run_with_scheduler():
    """帶定時任務的主程序"""
    logger.info("啟動智能錢包分析服務，支持定時執行")
    
    # 立即執行一次
    logger.info("立即執行一次更新任務...")
    await execute_daily_task()
    
    while True:
        try:
            # 計算下一個執行時間
            next_run = get_next_midnight_utc8()
            now_utc = datetime.utcnow()
            
            # 計算等待時間（秒）
            wait_seconds = (next_run - now_utc).total_seconds()
            
            if wait_seconds > 0:
                logger.info(f"下次執行時間: {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                logger.info(f"等待時間: {wait_seconds/3600:.2f} 小時")
                
                # 等待到下次執行時間
                await asyncio.sleep(wait_seconds)
            
            # 執行每日任務
            await execute_daily_task()
            
        except asyncio.CancelledError:
            logger.info("收到取消信號，正在關閉服務...")
            break
        except Exception as e:
            logger.error(f"定時任務執行錯誤: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            # 發生錯誤時等待1小時後重試
            logger.info("等待1小時後重試...")
            await asyncio.sleep(3600)

# 程式入口點
if __name__ == "__main__":
    try:
        # 使用新的定時任務模式
        asyncio.run(run_with_scheduler())
    except KeyboardInterrupt:
        logger.info("收到中斷信號，程序正在退出...")
    except Exception as e:
        logger.critical(f"程式執行過程中發生嚴重錯誤: {str(e)}")
        import traceback
        logger.critical(traceback.format_exc())