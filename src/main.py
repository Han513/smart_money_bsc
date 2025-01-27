import asyncio
import time
import json
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
from typing import List, Dict
from token_info import *

def log_execution_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)  # 适用于异步函数
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {execution_time:.4f} seconds.")
        return result
    return wrapper

def get_update_time():
    # 获取当前时间
    now_utc = datetime.now(timezone.utc)
    # 转换为 UTC+8
    utc_plus_8 = now_utc + timedelta(hours=8)
    # 格式化时间为字符串，格式可根据需要调整
    return utc_plus_8.strftime("%Y-%m-%d %H:%M:%S")

def make_naive_time(dt):
    """将带时区的时间转换为无时区时间"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

async def calculate_wallet_stats(data, bnb_price):
    wallet_stats = {}

    for row in data:
        wallet = row['signer']
        token_in = row['token_in']
        token_out = row['token_out']
        amount_in = Decimal(row['amount_in']) / (10 ** row['decimals_in'])
        amount_out = Decimal(row['amount_out']) / (10 ** row['decimals_out'])
        price_usd = Decimal(row['price_usd'])
        timestamp = int(row['created_at'])

        if token_in in ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                        '0x55d398326f99059fF775485246999027B3197955', 
                        '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'):
            value = amount_in * (bnb_price if token_in == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' else Decimal(1))
            action = 'buy'
        else:
            value = amount_out * (bnb_price if token_out == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' else Decimal(1))
            action = 'sell'

        if wallet not in wallet_stats:
            wallet_stats[wallet] = {
                'buy': [],
                'sell': [],
                'pnl': Decimal(0),
                'last_transaction': None,
                'tokens': set(),
                'remaining_tokens': {},  # 用於統計剩餘代幣
            }

        # 更新剩餘代幣的數量和價值
        token = token_out if action == 'buy' else token_in
        if action == 'buy':
            # 確保 token 有正確初始化
            wallet_stats[wallet]['remaining_tokens'].setdefault(token, {
                'amount': Decimal(0), 'cost': Decimal(0), 'profit': Decimal(0)
            })
            wallet_stats[wallet]['remaining_tokens'][token]['amount'] += amount_in
            wallet_stats[wallet]['remaining_tokens'][token]['cost'] += value

        elif action == 'sell':
            if token in wallet_stats[wallet]['remaining_tokens']:
                wallet_stats[wallet]['remaining_tokens'][token]['amount'] -= amount_out
                wallet_stats[wallet]['remaining_tokens'][token]['profit'] += value

        # 更新其他字段
        wallet_stats[wallet][action].append({'token': token, 'value': value, 'timestamp': timestamp})
        wallet_stats[wallet]['tokens'].add(token)
        wallet_stats[wallet]['last_transaction'] = (
            timestamp if wallet_stats[wallet]['last_transaction'] is None
            else max(wallet_stats[wallet]['last_transaction'], timestamp)
        )

        # 計算 PNL
        if action == 'sell':
            wallet_stats[wallet]['pnl'] += value
        else:
            wallet_stats[wallet]['pnl'] -= value

    # 清理剩餘代幣中數量為 0 的代幣
    for wallet, stats in wallet_stats.items():
        stats['remaining_tokens'] = {
            token: data
            for token, data in stats['remaining_tokens'].items()
            if data['amount'] > 0
        }

    return wallet_stats

# @log_execution_time
async def analyze_wallets_data(wallet_stats, bnb_price):
    wallet_analysis = {}
    current_time = int(time.time() * 1000)  # 当前时间戳（毫秒）

    async def process_wallet(wallet, stats):
        total_buy = sum(tx['value'] for tx in stats['buy'])
        total_sell = sum(tx['value'] for tx in stats['sell'])
        pnl = stats['pnl']
        num_buy = len(stats['buy'])
        num_sell = len(stats['sell'])
        total_transactions = num_buy + num_sell

        # 盈利代币数量和胜率
        profitable_tokens = len([tx for tx in stats['sell'] if tx['value'] > total_buy / (num_buy or 1)])
        total_tokens = len(stats['tokens'])
        win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0

        # 时间范围
        one_day_ago = current_time - 24 * 60 * 60 * 1000
        seven_days_ago = current_time - 7 * 24 * 60 * 60 * 1000
        thirty_days_ago = current_time - 30 * 24 * 60 * 60 * 1000

        # 计算函数
        def calculate_metrics(buy, sell, start_time):
            buy_filtered = [tx for tx in buy if tx['timestamp'] >= start_time]
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            total_buy_value = sum(tx['value'] for tx in buy_filtered)
            total_sell_value = sum(tx['value'] for tx in sell_filtered)
            num_buy = len(buy_filtered)
            num_sell = len(sell_filtered)
            pnl_value = total_sell_value - total_buy_value
            pnl_percentage = (pnl_value / total_buy_value * 100) if total_buy_value > 0 else 0
            avg_cost = total_buy_value / (num_buy or 1)
            avg_realized_profit = pnl_value / (num_sell or 1)
            total_transaction_num = num_buy + num_sell

            return {
                "win_rate": win_rate,
                "total_cost": total_buy_value,
                "avg_cost": avg_cost,
                "total_transaction_num": total_transaction_num,
                "buy_num": num_buy,
                "sell_num": num_sell,
                "pnl": pnl_value,
                "pnl_percentage": pnl_percentage,
                "avg_realized_profit": avg_realized_profit,
                "unrealized_profit": 0
            }

        # 按时间范围计算
        metrics_1d = calculate_metrics(stats['buy'], stats['sell'], one_day_ago)
        metrics_7d = calculate_metrics(stats['buy'], stats['sell'], seven_days_ago)
        metrics_30d = calculate_metrics(stats['buy'], stats['sell'], thirty_days_ago)
        # metrics_30d['total_tokens'] = total_tokens

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
                daily_buy = sum(tx['value'] for tx in buy if start_time <= tx['timestamp'] < end_time)
                daily_sell = sum(tx['value'] for tx in sell if start_time <= tx['timestamp'] < end_time)
                daily_pnl.append(daily_sell - daily_buy)
            return ",".join(map(str, daily_pnl[::-1]))  # 按从过去到现在排列

        pnl_pic_1d = calculate_daily_pnl(stats['buy'], stats['sell'], 1)
        pnl_pic_7d = calculate_daily_pnl(stats['buy'], stats['sell'], 7)
        pnl_pic_30d = calculate_daily_pnl(stats['buy'], stats['sell'], 30)

        # 计算分布和分布百分比
        def calculate_distribution(sell, start_time):
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            distribution = {"lt50": 0, "0to50": 0, "0to200": 0, "200to500": 0, "gt500": 0}
            
            # 避免 total_buy 为零的情况
            total_buy_dec = Decimal(total_buy) if total_buy != 0 else Decimal(1)  # 使用 1 作为默认值，防止零除

            for tx in sell_filtered:
                if num_buy > 0:  # 检查 num_buy 是否为有效值
                    pnl_percentage = ((tx['value'] - total_buy_dec) / total_buy_dec) * Decimal(100)
                else:
                    pnl_percentage = Decimal(0)  # 如果 num_buy 为 0，则直接设置 pnl_percentage 为 0

                if pnl_percentage < Decimal(-50):
                    distribution["lt50"] += 1
                elif Decimal(-50) <= pnl_percentage < Decimal(0):
                    distribution["0to50"] += 1
                elif Decimal(0) <= pnl_percentage <= Decimal(200):
                    distribution["0to200"] += 1
                elif Decimal(200) < pnl_percentage <= Decimal(500):
                    distribution["200to500"] += 1
                else:
                    distribution["gt500"] += 1

            return distribution

        def calculate_distribution_percentage(distribution, total_tokens):
            total_distribution = sum(distribution.values())
            return {key: (value / total_distribution * 100 if total_distribution > 0 else 0) for key, value in distribution.items()}

        distribution_7d = calculate_distribution(stats['sell'], seven_days_ago)
        distribution_30d = calculate_distribution(stats['sell'], thirty_days_ago)
        distribution_percentage_7d = calculate_distribution_percentage(distribution_7d, total_tokens)
        distribution_percentage_30d = calculate_distribution_percentage(distribution_30d, total_tokens)

        # 获取钱包余额
        balances = await fetch_wallet_balances([wallet], bnb_price)

        remaining_tokens = stats['remaining_tokens']
        fetcher = TokenInfoFetcher()

        # 查詢代幣即時信息
        with ThreadPoolExecutor() as executor:
            token_info_results = {
                token: executor.submit(fetcher.get_token_info, token) for token in remaining_tokens
            }
        remaining_tokens_summary = {}
        for token, data in remaining_tokens.items():
            # 獲取查詢結果
            token_info = token_info_results[token].result()
            
            amount = data.get("amount", Decimal(0))
            profit = data.get("profit", Decimal(0))
            cost = data.get("cost", Decimal(0))

            # 計算最後一筆交易時間
            last_transaction_time = max(
                tx['timestamp'] for tx in (stats['buy'] + stats['sell']) if tx['token'] == token
            )

            # 計算買入均價
            buy_transactions = [
                {
                    'amount': remaining_tokens[token]['amount'],
                    'cost': remaining_tokens[token]['cost']
                } 
                for token in remaining_tokens
            ]

            total_buy_value = sum(tx['cost'] for tx in buy_transactions)
            total_buy_amount = sum(tx['amount'] for tx in buy_transactions if tx['amount'] > 0)
            avg_price = total_buy_value / total_buy_amount if total_buy_amount > 0 else 0
            token_info_price_native = Decimal(str(token_info.get("priceNative", 0)))
            token_info_price_usd = Decimal(str(token_info.get("priceUsd", 0)))

            # 統計結果
            remaining_tokens_summary[token] = {
                "token_name": token_info.get("symbol", "Unknown"),
                "token_icon": token_info.get("url", "no url"),
                "chain": "BSC",
                "amount": amount,
                "value": amount  * token_info_price_native,  # 以最新價格計算價值
                "value_USDT": amount  * token_info_price_usd,  # 以最新價格計算價值
                "unrealized_profits": amount  * token_info_price_usd,  # 以最新價格計算價值
                "pnl": profit - data["cost"],
                "pnl_percentage": (profit - data["cost"]) / data["cost"] * 100 if data["cost"] > 0 else 0,
                "marketcap": token_info.get("marketcap", 0),
                "is_cleared": 0,
                "cumulative_cost": data["cost"],
                "cumulative_profit": profit,
                "last_transaction_time": last_transaction_time,  # 最後一筆交易時間
                "time": make_naive_time(datetime.now()),
                "avg_price": avg_price,  # 該錢包在該代幣上的買入均價
            }

        return wallet, {
            'balance': balances[wallet]['balance'],
            'balance_USD': balances[wallet]['balance_USD'],
            'chain': "BSC",
            'tag': None,
            'twitter_name': None,
            'twitter_username': None,
            'is_smart_wallet': True,
            'wallet_type': 0,
            'asset_multiple': asset_multiple,
            'token_list': token_list,
            'total_tokens': total_tokens,
            # 'total_buy': total_buy,
            # 'total_sell': total_sell,
            # 'pnl': pnl,
            # 'win_rate': win_rate,
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
            'remaining_tokens': remaining_tokens_summary,
        }

    tasks = [process_wallet(wallet, stats) for wallet, stats in wallet_stats.items()]
    results = await asyncio.gather(*tasks)

    wallet_analysis = {wallet: analysis for wallet, analysis in results}
    return wallet_analysis

async def process_and_save_wallets(filtered_wallet_analysis, session, chain):
    """
    Process filtered_wallet_analysis data and save to database
    """
    for wallet_address, wallet_data in filtered_wallet_analysis.items():
        wallet_data['wallet_address'] = wallet_address
        try:
            await write_wallet_data_to_db(session, wallet_data, chain)
        except Exception as e:
            print(f"Failed to save wallet: {wallet_address} - {str(e)}")

# 性能優化配置
class PerformanceConfig:
    MAX_CONCURRENT_TASKS = 20  # 並發任務數
    CHUNK_SIZE = 1000  # 批次處理數量
    LOGGING_INTERVAL = 100  # 日誌記錄間隔

class WalletAnalyzer:
    def __init__(self, database_uri, swap_database_uri):
        # 創建連接池
        self.engine = create_async_engine(
            database_uri, 
            pool_size=20,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False
        )
        self.swap_engine = create_async_engine(
            swap_database_uri,
            pool_size=20,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False
        )
        
        self.async_session = sessionmaker(
            bind=self.engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        self.swap_async_session = sessionmaker(
            bind=self.swap_engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        
        # 設置日誌
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def get_unique_wallets(self, session) -> List[str]:
        """獲取所有唯一錢包地址"""
        query = """
        SELECT DISTINCT signer 
        FROM bsc_dex.trades 
        WHERE (
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

    async def process_wallet(self, wallet: str, bnb_price: Decimal) -> Dict:
        """處理單個錢包"""
        async with self.async_session() as session:
            async with session.begin():
                await session.execute(text("SET search_path TO bsc_dex;"))
                wallet_trades = await self.get_wallet_trades(session, wallet)
        
        if not wallet_trades:
            return None

        async with self.async_session() as session:
            async with session.begin():
                wallet_stats = await calculate_wallet_stats(wallet_trades, bnb_price)
                wallet_analysis = await analyze_wallets_data(wallet_stats, bnb_price)

        return wallet_analysis

    # async def save_wallet_data(self, wallet_analysis: Dict):
    #     """保存錢包分析結果"""
    #     async with self.swap_async_session() as db_session:
    #         async with db_session.begin():
    #             filtered_wallet_analysis = {
    #                 wallet: data for wallet, data in wallet_analysis.items()
    #                 if data['total_tokens'] >= 5 and data['stats_30d']['win_rate'] > 40
    #             }
                
    #             if filtered_wallet_analysis:
    #                 await process_and_save_wallets(
    #                     filtered_wallet_analysis, 
    #                     db_session, 
    #                     chain="BSC"
    #                 )

    async def analyze_wallets(self, bnb_price: Decimal):
        """主分析流程"""
        start_time = time.time()
        
        # 獲取唯一錢包
        async with self.async_session() as session:
            async with session.begin():
                unique_wallets = await self.get_unique_wallets(session)
        
        total_wallets = len(unique_wallets)
        self.logger.info(f"開始分析 {total_wallets} 個錢包")

        # 創建信號量控制並發
        semaphore = asyncio.Semaphore(PerformanceConfig.MAX_CONCURRENT_TASKS)
        processed_wallets = 0

        async def process_wallet_with_semaphore(wallet):
            nonlocal processed_wallets
            async with semaphore:
                try:
                    wallet_analysis = await self.process_wallet(wallet, bnb_price)
                    if wallet_analysis:
                        # 為每個錢包創建新的會話
                        async with self.swap_async_session() as db_session:
                            async with db_session.begin():
                                filtered_wallet_analysis = {
                                    wallet: data for wallet, data in wallet_analysis.items()
                                    if data['total_tokens'] >= 5 and data['stats_30d']['win_rate'] > 40
                                }
                                
                                if filtered_wallet_analysis:
                                    await process_and_save_wallets(
                                        filtered_wallet_analysis, 
                                        db_session, 
                                        chain="BSC"
                                    )
                    
                    processed_wallets += 1
                    if processed_wallets % PerformanceConfig.LOGGING_INTERVAL == 0:
                        self.logger.info(f"已處理 {processed_wallets}/{total_wallets} 個錢包")
                except Exception as e:
                    self.logger.error(f"錢包 {wallet} 處理失敗: {str(e)}")

        # 分批處理錢包
        for i in range(0, total_wallets, PerformanceConfig.CHUNK_SIZE):
            chunk = unique_wallets[i:i + PerformanceConfig.CHUNK_SIZE]
            tasks = [process_wallet_with_semaphore(wallet) for wallet in chunk]
            await asyncio.gather(*tasks)

        end_time = time.time()
        self.logger.info(f"總處理時間: {end_time - start_time:.2f} 秒")

    async def get_wallet_trades(self, session, wallet_address):
        """獲取特定錢包的所有交易紀錄"""
        query = """
        SELECT * 
        FROM bsc_dex.trades 
        WHERE signer = :wallet_address
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

async def main():
    bnb_price = await get_price()

    analyzer = WalletAnalyzer(DATABASE_URI, DATABASE_URI_SWAP_BSC)
    await analyzer.analyze_wallets(bnb_price)

if __name__ == "__main__":
    asyncio.run(main())