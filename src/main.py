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
from sqlalchemy import text, table, select
from typing import List, Dict, Any

# 初始化 PostgreSQL 連接
engine = create_async_engine(DATABASE_URI, echo=False)
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
trades_table = table("trades")

my_engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
my_async_session = sessionmaker(bind=my_engine, class_=AsyncSession, expire_on_commit=False)

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

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # 将 Decimal 转换为 float
        return super(DecimalEncoder, self).default(obj)

# 格式化输出结果
def pretty_print(data):
    print(json.dumps(data, cls=DecimalEncoder, indent=4))

# 數據清理函數
async def clean_data(session, limit, offset):
    query = (
        select("*")
        .select_from(trades_table)  # 使用声明的表结构
        .where(
            text("""
            token_in IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', '0x55d398326f99059fF775485246999027B3197955', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            OR token_out IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', '0x55d398326f99059fF775485246999027B3197955', '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
            """)
        )
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(query)
    return result.mappings().all()  # 将结果映射为字典

# 統計每個錢包的交易紀錄
async def calculate_wallet_stats(data, bnb_price):
    wallet_stats = {}
    
    for row in data:
        wallet = row['signer']  # 現在可以用字典鍵訪問
        token_in = row['token_in']
        token_out = row['token_out']
        amount_in = Decimal(row['amount_in']) / (10 ** row['decimals_in'])
        amount_out = Decimal(row['amount_out']) / (10 ** row['decimals_out'])
        price_usd = Decimal(row['price_usd'])
        timestamp = int(row['created_at'])  # 確保 timestamp 是整數格式

        # 判斷交易方向
        if token_in in ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                        '0x55d398326f99059fF775485246999027B3197955', 
                        '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'):
            value = amount_in * (bnb_price if token_in == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' else Decimal(1))
            action = 'buy'
        else:
            value = amount_out * (bnb_price if token_out == '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' else Decimal(1))
            action = 'sell'

        if wallet not in wallet_stats:
            wallet_stats[wallet] = {'buy': [], 'sell': [], 'pnl': Decimal(0), 'last_transaction': None, 'tokens': set()}

        wallet_stats[wallet][action].append({'token': token_out if action == 'buy' else token_in, 'value': value, 'timestamp': timestamp})
        wallet_stats[wallet]['tokens'].add(token_out if action == 'buy' else token_in)
        wallet_stats[wallet]['last_transaction'] = (
            timestamp if wallet_stats[wallet]['last_transaction'] is None
            else max(wallet_stats[wallet]['last_transaction'], timestamp)
        )

        # 計算 PNL
        if action == 'sell':
            wallet_stats[wallet]['pnl'] += value
        else:
            wallet_stats[wallet]['pnl'] -= value
    return wallet_stats

@log_execution_time
async def analyze_wallets(wallet_stats, bnb_price):
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
                    pnl_percentage = ((tx['value'] - (total_buy_dec / Decimal(num_buy))) / total_buy_dec) * Decimal(100)
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
            'is_active': True
        }

    tasks = [process_wallet(wallet, stats) for wallet, stats in wallet_stats.items()]
    results = await asyncio.gather(*tasks)

    wallet_analysis = {wallet: analysis for wallet, analysis in results}
    print(wallet_analysis)
    return wallet_analysis

async def process_and_save_wallets(filtered_wallet_analysis, session, chain):
    """
    处理 filtered_wallet_analysis 数据并保存到数据库
    """
    for wallet_address, wallet_data in filtered_wallet_analysis.items():
        wallet_data['wallet_address'] = wallet_address  # 将字典键添加为 `wallet_address`
        success = await write_wallet_data_to_db(session, wallet_data, chain)
        if not success:
            print(f"Failed to save wallet: {wallet_address}")

async def get_time_range(session) -> tuple:
    """获取数据库中最早和最新的created_at时间戳"""
    query = """
    SELECT 
        MIN(created_at) as min_time, 
        MAX(created_at) as max_time 
    FROM bsc_dex.trades 
    WHERE created_at > 0
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
    row = result.first()
    if row.min_time is None or row.max_time is None:
        raise ValueError("No valid data found in the specified time range")
    return row.min_time, row.max_time

async def get_data_chunk(session, start_time: int, end_time: int, batch_size: int = 1000) -> List[Dict]:
    """获取指定created_at时间范围内的数据块"""
    query = f"""
    SELECT *
    FROM bsc_dex.trades
    WHERE created_at > 0
    AND (
        token_in IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                    '0x55d398326f99059fF775485246999027B3197955', 
                    '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
        OR 
        token_out IN ('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 
                     '0x55d398326f99059fF775485246999027B3197955', 
                     '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d')
    )
    AND created_at BETWEEN :start_time AND :end_time
    ORDER BY created_at ASC
    LIMIT :limit
    """
    result = await session.execute(
        text(query),
        {"start_time": start_time, "end_time": end_time, "limit": batch_size}
    )
    return result.mappings().all()

async def process_time_chunk(session, start_time: int, end_time: int, bnb_price: Decimal) -> Dict:
    """处理特定created_at时间范围内的数据"""
    all_chunk_data = []
    batch_size = 1000
    last_timestamp = start_time - 1
    
    while True:
        chunk_data = await get_data_chunk(session, last_timestamp + 1, end_time, batch_size)
        if not chunk_data:
            break
        
        all_chunk_data.extend(chunk_data)
        
        # 更新最后处理的时间戳
        last_timestamp = chunk_data[-1]['created_at']
        
        # 如果获取的数据量小于batch_size或已经达到结束时间，退出循环
        if len(chunk_data) < batch_size or last_timestamp >= end_time:
            break
            
        # 添加进度日志
        print(f"Processed up to {datetime.fromtimestamp(last_timestamp/1000)} ({len(all_chunk_data)} records)")

    if not all_chunk_data:
        return {}

    # 处理数据块
    wallet_stats = await calculate_wallet_stats(all_chunk_data, bnb_price)
    return await analyze_wallets(wallet_stats, bnb_price)

async def merge_wallet_stats(existing_stats: Dict, new_stats: Dict):
    """合并钱包统计数据"""
    # 更新基本统计信息
    existing_stats['total_tokens'] = len(
        set(existing_stats['token_list'].split(',')) | 
        set(new_stats['token_list'].split(','))
    )
    
    # 更新时间相关统计
    existing_stats['last_transaction'] = max(
        existing_stats['last_transaction'],
        new_stats['last_transaction']
    )
    
    # 合并其他统计数据
    for period in ['1d', '7d', '30d']:
        stats_key = f'stats_{period}'
        if stats_key in existing_stats and stats_key in new_stats:
            existing_stats[stats_key]['total_cost'] += new_stats[stats_key]['total_cost']
            existing_stats[stats_key]['pnl'] += new_stats[stats_key]['pnl']
            existing_stats[stats_key]['total_transaction_num'] += new_stats[stats_key]['total_transaction_num']
            
            # 重新计算平均值和百分比
            existing_stats[stats_key]['avg_cost'] = (
                existing_stats[stats_key]['total_cost'] / 
                existing_stats[stats_key]['total_transaction_num']
                if existing_stats[stats_key]['total_transaction_num'] > 0 else 0
            )
            existing_stats[stats_key]['pnl_percentage'] = (
                existing_stats[stats_key]['pnl'] / 
                existing_stats[stats_key]['total_cost'] * 100
                if existing_stats[stats_key]['total_cost'] > 0 else 0
            )

# 主程序
async def main():
    chunk_size = timedelta(days=7).total_seconds() * 1000
    async with async_session() as session:
        async with session.begin():
            bnb_price = await get_price()
            await session.execute(text("SET search_path TO bsc_dex;"))
            
            # # 分页获取数据
            # limit = 1000
            # offset = 0
            # all_data = []

            # while True:
            #     cleaned_data = await clean_data(session, limit=limit, offset=offset)
            #     print(cleaned_data)
            #     if not cleaned_data:  # 没有更多数据时结束循环
            #         break
            #     all_data.extend(cleaned_data)
            #     offset += limit

            # # 统计每个钱包的交易记录
            # wallet_stats = await calculate_wallet_stats(all_data, bnb_price)
            
            # # 分析每个钱包
            # wallet_analysis = await analyze_wallets(wallet_stats, bnb_price)

            # # 筛选分析结果
            # filtered_wallet_analysis = {
            #     wallet: data for wallet, data in wallet_analysis.items()
            #     if 'stats_30d' in data  # 确保 metrics_30d 存在
            #     and data['total_tokens'] >= 5  # 条件1: 交易过的币种需至少大于或等于 5 种
            #     and data['stats_30d']['win_rate'] > 40  # 条件3: 胜率大于 40
            # }

            min_time, max_time = await get_time_range(session)
            print(min_time)
            print(max_time)
            
            # 创建时间块
            current_start = min_time
            all_wallet_analysis = {}
            
            while current_start < max_time:
                current_end = min(current_start + chunk_size, max_time)
                print(f"Processing data from {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(current_end/1000)}")
                
                # 处理当前时间块
                chunk_analysis = await process_time_chunk(session, current_start, current_end, bnb_price)
                
                # 合并结果
                for wallet, data in chunk_analysis.items():
                    if wallet not in all_wallet_analysis:
                        all_wallet_analysis[wallet] = data
                    else:
                        # 更新现有钱包的统计数据
                        await merge_wallet_stats(all_wallet_analysis[wallet], data)
                
                current_start = current_end + 1
                
            # 筛选最终结果
            filtered_wallet_analysis = {
                wallet: data for wallet, data in all_wallet_analysis.items()
                if data['total_tokens'] >= 5 and data['stats_30d']['win_rate'] > 40
            }

            # 打印筛选后的结果
            pretty_print(filtered_wallet_analysis)

        # 保存筛选后的数据到数据库
        async with my_async_session() as db_session:
            await process_and_save_wallets(filtered_wallet_analysis, db_session, chain="BSC")
        print("All filtered wallet data saved to the database.")

# 運行程式
if __name__ == "__main__":
    asyncio.run(main())

