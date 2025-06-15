import os
import logging
import traceback
import asyncio
from models import *
from config import *
from balance import *
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta
from collections import defaultdict
from token_info import TokenInfoFetcher
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BUSD_MINT = "0x55d398326f99059fF775485246999027B3197955"
WBNB_MINT = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"

async def reset_wallet_buy_data(wallet_address: str, session: AsyncSession, chain):
    """
    重置指定錢包的所有代幣購買數據，但保留歷史累計數據
    """
    try:
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        
        # 獲取該錢包的所有代幣記錄
        stmt = select(TokenBuyData).filter(TokenBuyData.wallet_address == wallet_address)
        result = await session.execute(stmt)
        holdings = result.scalars().all()
        
        # 重置所有代幣的當前持倉數據，但保留歷史累計數據
        for holding in holdings:
            # 在重置前，將當前持倉數據轉移到歷史數據
            if hasattr(holding, 'historical_total_buy_amount') and holding.total_amount > 0:
                # 確保歷史數據欄位存在
                if not holding.historical_total_buy_amount:
                    holding.historical_total_buy_amount = 0
                if not holding.historical_total_buy_cost:
                    holding.historical_total_buy_cost = 0
                if not holding.historical_total_sell_amount:
                    holding.historical_total_sell_amount = 0
                if not holding.historical_total_sell_value:
                    holding.historical_total_sell_value = 0
                
                # 記錄關閉時間
                if holding.total_amount > 0:
                    holding.last_active_position_closed_at = int(get_utc8_time().timestamp())
            
            # 重置當前持倉數據
            holding.total_amount = 0
            holding.total_cost = 0
            holding.avg_buy_price = 0
            holding.position_opened_at = None
            holding.last_transaction_time = None
            holding.updated_at = get_utc8_time()
        
        await asyncio.shield(session.commit())
    except Exception as e:
        await session.rollback()
        print(f"重置錢包 {wallet_address} 的買入數據時發生錯誤: {str(e)}")
        traceback.print_exc()  # 列印完整的錯誤堆疊追蹤

async def get_transactions_for_wallet(session: AsyncSession, chain: str, wallet_address: str, days: int = 30):
    """
    查詢指定 wallet_address 在過去指定天數內的交易記錄。
    :param session: 資料庫會話
    :param chain: 區塊鏈名稱
    :param wallet_address: 要查詢的錢包地址
    :param days: 查詢的天數範圍，預設為 90 天
    :return: 符合條件的交易列表，每條記錄以字典形式返回。
    """
    try:
        cutoff_time = int((datetime.utcnow() - timedelta(days=days)).timestamp())

        schema = 'dex_query_v1'
        Transaction.with_schema(schema)

        query = (
            select(Transaction)
            .where(
                Transaction.chain == chain,
                Transaction.wallet_address == wallet_address,
                Transaction.transaction_time >= cutoff_time
            )
            .order_by(Transaction.transaction_time.asc())
        )

        result = await session.execute(query)
        transactions = result.scalars().all()

        return [
            {
                "id": tx.id,
                "wallet_address": tx.wallet_address,
                "wallet_balance": tx.wallet_balance,
                "token_address": tx.token_address,
                "token_icon": tx.token_icon,
                "token_name": tx.token_name,
                "price": tx.price,
                "amount": tx.amount,
                "marketcap": tx.marketcap,
                "value": tx.value,
                "holding_percentage": tx.holding_percentage,
                "chain": tx.chain,
                "realized_profit": tx.realized_profit,
                "realized_profit_percentage": tx.realized_profit_percentage,
                "transaction_type": tx.transaction_type,
                "transaction_time": tx.transaction_time,
                "time": tx.time,
                "signature": tx.signature
            }
            for tx in transactions
        ]
    except Exception as e:
        logging.error(f"查詢錢包 {wallet_address} 的交易記錄失敗: {e}")
        raise RuntimeError(f"查詢錢包 {wallet_address} 的交易記錄失敗: {str(e)}")

def convert_transaction_format(old_transactions, wallet_address, bnb_usdt_price):
    """
    將原始交易數據轉換為calculate_statistics2需要的格式
    """
    new_transactions = {wallet_address: []}
    
    if isinstance(old_transactions.get(wallet_address), list) and len(old_transactions[wallet_address]) > 0:
        first_tx = old_transactions[wallet_address][0]
        
        if 'token_mint' in first_tx and 'transaction_type' in first_tx and 'token_amount' in first_tx:
            for tx in old_transactions[wallet_address]:
                tx_copy = tx.copy() 
                
                if 'transaction_type' in tx_copy:
                    tx_copy['transaction_type'] = tx_copy['transaction_type'].lower()
                
                new_transactions[wallet_address].append(tx_copy)
            
            return new_transactions
    
    try:
        for tx in old_transactions[wallet_address]:
            timestamp = tx.get('timestamp', 0)
            
            # 處理扁平格式的交易記錄 (token_mint 作為主體)
            if 'token_mint' in tx and 'transaction_type' in tx:
                tx_type = tx['transaction_type'].lower()
                
                new_tx = {
                    'token_mint': tx['token_mint'],
                    'timestamp': timestamp,
                    'transaction_type': tx_type,
                    'token_amount': tx.get('token_amount', 0),
                    'value': tx.get('value', 0),
                    'bnb_price_usd': bnb_usdt_price
                }
                
                if tx_type == 'sell' and 'realized_profit' in tx:
                    new_tx['realized_profit'] = tx['realized_profit']
                
                new_transactions[wallet_address].append(new_tx)
                continue
            
            for token_mint, token_data in tx.items():
                if token_mint == 'timestamp' or not isinstance(token_data, dict):
                    continue
                    
                if not isinstance(token_data, dict):
                    print(f"跳過非字典類型的 token_data: {token_data} for {token_mint}")
                    continue
                    
                if token_data.get('buy_amount', 0) > 0:
                    new_tx = {
                        'token_mint': token_mint,
                        'timestamp': timestamp,
                        'transaction_type': 'buy',
                        'token_amount': token_data['buy_amount'],
                        'value': token_data.get('cost', 0),
                        'bnb_price_usd': bnb_usdt_price
                    }
                    new_transactions[wallet_address].append(new_tx)
                    
                if token_data.get('sell_amount', 0) > 0:
                    new_tx = {
                        'token_mint': token_mint,
                        'timestamp': timestamp,
                        'transaction_type': 'sell',
                        'token_amount': token_data['sell_amount'],
                        'value': token_data.get('profit', 0),
                        'realized_profit': token_data.get('profit', 0),
                        'bnb_price_usd': bnb_usdt_price
                    }
                    new_transactions[wallet_address].append(new_tx)
    except Exception as e:
        print(f"轉換交易格式時出錯: {e}")
        import traceback
        traceback.print_exc()
    
    # print(f"轉換後的交易數據: {new_transactions}")
    return new_transactions

async def get_token_supply(token_address: str) -> Decimal:
    """获取代币供应量"""
    try:
        # 使用 TokenInfoFetcher 获取代币信息
        token_address = token_address.lower()  # 確保地址是小寫
        token_info = await TokenInfoFetcher.get_token_info(token_address)
        if token_info and token_address in token_info:
            supply = token_info[token_address].get("totalSupply", "0")
            return Decimal(str(supply))
        return Decimal('1000000000')  # 默认值
    except Exception as e:
        print(f"Error getting token supply: {e}")
        return Decimal('1000000000')  # 默认值

async def calculate_remaining_tokens_optimized(wallet_address, session, chain, client):
    try:
        # 獲取錢包的所有代幣持倉記錄
        token_buy_data_records = await get_wallet_token_holdings(wallet_address, session, chain)
        
        remaining_tokens = []
        for token_data in token_buy_data_records:
            token_address = token_data.token_address.lower()  # 確保地址是小寫
            
            # 從TokenUtils獲取代幣信息
            token_info = await TokenInfoFetcher.get_token_info(token_address)
            if not token_info or token_address not in token_info:
                continue
                
            token_info = token_info[token_address]
            
            # 獲取代幣供應量
            supply = Decimal(str(await get_token_supply(token_address) or 1000000000))
            
            # 從token_data獲取買入均價
            buy_price = Decimal(str(token_data.avg_buy_price))
            formatted_buy_price = buy_price.quantize(Decimal('0.0000000000'))
            marketcap = formatted_buy_price * supply
            
            # 獲取代幣即時價格
            url = token_info.get('url', {})
            symbol = token_info.get('symbol', "")
            token_price = Decimal(str(token_info.get('priceNative', 0)))
            token_price_USDT = Decimal(str(token_info.get('priceUsd', 0)))
            
            # 當前持倉量
            current_amount = Decimal(str(token_data.total_amount))
            
            # 若 current_amount < 0.001，則當作清倉
            if current_amount < Decimal('0.001'):
                continue
            
            # 計算持倉價值
            value = current_amount * token_price_USDT
            cost = Decimal(str(token_data.total_cost))
            
            # 計算未實現損益
            unrealized_profit = (token_price_USDT - buy_price) * current_amount
            
            # 計算歷史已實現損益
            historical_sell_value = Decimal(str(getattr(token_data, 'historical_total_sell_value', 0)))
            historical_buy_cost = Decimal(str(getattr(token_data, 'historical_total_buy_cost', 0)))
            historical_sell_amount = Decimal(str(getattr(token_data, 'historical_total_sell_amount', 0)))
            historical_buy_amount = Decimal(str(getattr(token_data, 'historical_total_buy_amount', 0)))
            realized_profit = Decimal(str(getattr(token_data, 'realized_profit', 0)))
            
            # 計算最後交易時間
            last_transaction_time = int(time.time())  # 默認使用當前時間
            if hasattr(token_data, 'last_transaction_time'):
                try:
                    if isinstance(token_data.last_transaction_time, datetime):
                        last_transaction_time = int(token_data.last_transaction_time.timestamp())
                    elif isinstance(token_data.last_transaction_time, (int, float)):
                        last_transaction_time = int(token_data.last_transaction_time)
                except (AttributeError, TypeError, ValueError) as e:
                    print(f"Error processing last_transaction_time for token {token_address}: {e}")
            
            remaining_tokens.append({
                'token_address': token_address,
                'token_name': symbol,
                'token_icon': url,
                'chain': chain,
                'amount': float(current_amount),
                'value': float(value),
                'value_USDT': float(value),
                'unrealized_profits': float(unrealized_profit),
                'pnl': float(realized_profit),
                'pnl_percentage': float((realized_profit / cost * 100) if cost > 0 else 0),
                'marketcap': float(marketcap),
                'is_cleared': 0,
                'cumulative_cost': float(cost),
                'cumulative_profit': float(realized_profit),
                'last_transaction_time': last_transaction_time,
                'time': datetime.now(),
                'avg_price': float(buy_price)
            })
            
        return remaining_tokens
        
    except Exception as e:
        print(f"Error while processing and saving holdings: {e}")
        import traceback
        traceback.print_exc()
        return []

async def calculate_token_statistics(transactions, wallet_address, session, chain, client):
    """
    計算錢包中所有代幣的交易統計資料，包括歷史買賣數據和當前持倉，並寫入TokenBuyData表
    """
    try:
        # 初始化字典來存儲每個代幣的交易數據
        token_summary = defaultdict(lambda: {
            # 當前持倉數據
            'total_amount': Decimal('0'),
            'total_cost': Decimal('0'),
            'buys': [],  # 用於FIFO計算的買入批次
            'position_opened_at': None,
            'realized_profit': Decimal('0'),
            
            # 歷史累計數據
            'historical_total_buy_amount': Decimal('0'),
            'historical_total_buy_cost': Decimal('0'),
            'historical_total_sell_amount': Decimal('0'),
            'historical_total_sell_value': Decimal('0'),
            'last_active_position_closed_at': None,
            
            # 其他數據
            'last_transaction_time': 0,
            'has_transactions': False  # 新增標記，表示是否有交易記錄
        })

        # print(f"處理錢包 {wallet_address} 的交易數據，交易數量: {len(transactions[wallet_address])}")

        all_txs = sorted(transactions[wallet_address], key=lambda tx: tx.get('timestamp', 0))

        for tx in all_txs:
            tx_timestamp = tx.get("timestamp", 0)
            token_mint = tx.get("token_mint")
            realized_profit = Decimal(str(tx.get('realized_profit', 0)))
            
            if not token_mint or token_mint.lower() in [BUSD_MINT.lower(), WBNB_MINT.lower()]:
                continue
                
            tx_type = tx.get("transaction_type", "").upper()
            token_amount = Decimal(str(tx.get('token_amount', 0)))
            tx_value = Decimal(str(tx.get('value', 0)))  # 交易價值
            
            # 更新該代幣的最後交易時間
            token_summary[token_mint]['last_transaction_time'] = max(
                token_summary[token_mint]['last_transaction_time'], 
                tx_timestamp
            )
            
            # 處理買入交易
            if tx_type == "BUY":
                token_summary[token_mint]['buys'].append({
                    'amount': token_amount,
                    'cost': tx_value,
                    'timestamp': tx_timestamp
                })
                
                # 更新當前持倉數據
                token_summary[token_mint]['total_amount'] += token_amount
                token_summary[token_mint]['total_cost'] += tx_value
                
                # 更新首次買入時間（如果尚未設置且當前持倉大於0）
                if token_summary[token_mint]['position_opened_at'] is None and token_summary[token_mint]['total_amount'] > 0:
                    token_summary[token_mint]['position_opened_at'] = int(tx_timestamp)
                
                # 更新歷史累計買入數據
                token_summary[token_mint]['historical_total_buy_amount'] += token_amount
                token_summary[token_mint]['historical_total_buy_cost'] += tx_value
                token_summary[token_mint]['has_transactions'] = True
                
            # 處理賣出交易
            elif tx_type == "SELL":
                # 更新歷史累計賣出數據
                token_summary[token_mint]['historical_total_sell_amount'] += token_amount
                token_summary[token_mint]['historical_total_sell_value'] += tx_value
                token_summary[token_mint]['realized_profit'] += realized_profit
                
                # 使用FIFO方法計算持倉變化
                remaining_to_sell = token_amount
                
                # 從最早的買入批次開始賣出
                while remaining_to_sell > 0 and token_summary[token_mint]['buys']:
                    oldest_buy = token_summary[token_mint]['buys'][0]
                    
                    if oldest_buy['amount'] <= remaining_to_sell:
                        # 整個批次都被賣出
                        remaining_to_sell -= oldest_buy['amount']
                        token_summary[token_mint]['buys'].pop(0)
                    else:
                        # 只賣出部分批次
                        sell_ratio = remaining_to_sell / oldest_buy['amount']
                        partial_cost = oldest_buy['cost'] * sell_ratio
                        oldest_buy['cost'] -= partial_cost
                        oldest_buy['amount'] -= remaining_to_sell
                        remaining_to_sell = Decimal('0')
                
                # 更新當前持倉量和成本
                token_summary[token_mint]['total_amount'] = max(Decimal('0'), token_summary[token_mint]['total_amount'] - token_amount)
                token_summary[token_mint]['total_cost'] = sum(buy['cost'] for buy in token_summary[token_mint]['buys'])
                
                # 如果完全賣出，更新相關時間並重置
                if token_summary[token_mint]['total_amount'] <= Decimal('0.000001'):
                    token_summary[token_mint]['total_amount'] = Decimal('0')
                    token_summary[token_mint]['total_cost'] = Decimal('0')
                    token_summary[token_mint]['last_active_position_closed_at'] = int(tx_timestamp)
                    if len(token_summary[token_mint]['buys']) == 0:
                        token_summary[token_mint]['position_opened_at'] = None
                token_summary[token_mint]['has_transactions'] = True

        # 向資料庫寫入每個代幣的統計數據（無論是否有持倉）
        for token_address, stats in token_summary.items():
            if not stats['has_transactions'] and stats['total_amount'] <= Decimal('0.000001'):
                continue
            # 計算平均買入價格
            historical_avg_buy_price = Decimal('0')
            if stats['historical_total_buy_amount'] > 0:
                historical_avg_buy_price = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
            
            # 計算平均賣出價格
            historical_avg_sell_price = Decimal('0')
            if stats['historical_total_sell_amount'] > 0:
                historical_avg_sell_price = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
            
            # 計算當前持倉的平均買入價格
            avg_buy_price = Decimal('0')
            if stats['total_amount'] > 0:
                avg_buy_price = stats['total_cost'] / stats['total_amount']
            
            # 在所有需要寫入 last_active_position_closed_at 的地方，做如下處理：
            last_active_position_closed_at = stats['last_active_position_closed_at']
            if isinstance(last_active_position_closed_at, datetime):
                last_active_position_closed_at = int(last_active_position_closed_at.timestamp())
            elif last_active_position_closed_at is None:
                last_active_position_closed_at = None
            
            # 組裝資料庫所需格式
            token_data = {
                'token_address': token_address,
                
                # 當前持倉數據
                'total_amount': float(stats['total_amount']),
                'total_cost': float(stats['total_cost']),
                'avg_buy_price': float(avg_buy_price),
                'position_opened_at': stats['position_opened_at'],
                'realized_profit': stats['realized_profit'],
                
                # 歷史累計數據
                'historical_total_buy_amount': float(stats['historical_total_buy_amount']),
                'historical_total_buy_cost': float(stats['historical_total_buy_cost']),
                'historical_total_sell_amount': float(stats['historical_total_sell_amount']),
                'historical_total_sell_value': float(stats['historical_total_sell_value']),
                'historical_avg_buy_price': float(historical_avg_buy_price),
                'historical_avg_sell_price': float(historical_avg_sell_price),
                'last_active_position_closed_at': last_active_position_closed_at,
                'last_transaction_time': stats['last_transaction_time']
            }

            try:
                await save_wallet_buy_data(wallet_address, token_data, token_address, session, chain)
            except Exception as e:
                print(f"保存錢包 {wallet_address} 代幣 {token_address} 統計數據時出錯: {e}")
                import traceback
                traceback.print_exc()

        # print(f"成功為錢包 {wallet_address} 保存了 {len(token_summary)} 筆代幣交易統計數據")
        
        # 呼叫原有的 calculate_remaining_tokens 函數處理剩餘代幣的顯示邏輯
        await calculate_remaining_tokens_optimized(wallet_address, session, chain, client)

    except Exception as e:
        print(f"處理錢包 {wallet_address} 交易統計數據時出錯: {e}")
        import traceback
        traceback.print_exc()

async def calculate_distribution(aggregated_tokens, days):
    """
    计算7天或30天内的收益分布
    """
    distribution = {
        'distribution_gt500': 0,
        'distribution_200to500': 0,
        'distribution_0to200': 0,
        'distribution_0to50': 0,
        'distribution_lt50': 0
    }

    # 计算分布
    for stats in aggregated_tokens.values():
        if stats['cost'] > 0:  # 防止除零错误
            pnl_percentage = ((stats['profit'] - stats['cost']) / stats['cost']) * 100
            if pnl_percentage > 500:
                distribution['distribution_gt500'] += 1
            elif 200 <= pnl_percentage <= 500:
                distribution['distribution_200to500'] += 1
            elif 0 <= pnl_percentage < 200:
                distribution['distribution_0to200'] += 1
            elif -50 <= pnl_percentage < 0:
                distribution['distribution_0to50'] += 1
            elif pnl_percentage < -50:
                distribution['distribution_lt50'] += 1

    # 计算分布百分比
    total_distribution = sum(distribution.values())    
    distribution_percentage = {
        'distribution_gt500_percentage': round((distribution['distribution_gt500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_200to500_percentage': round((distribution['distribution_200to500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to200_percentage': round((distribution['distribution_0to200'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to50_percentage': round((distribution['distribution_0to50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_lt50_percentage': round((distribution['distribution_lt50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
    }

    return distribution, distribution_percentage

async def calculate_statistics2(transactions, days, bnb_usdt_price):
    """
    計算統計數據，包括總買賣次數、總成本、平均成本、PNL、每日PNL圖等。
    改進版本：使用與save_transactions_to_db相同的FIFO方法計算利潤
    """
    # 獲取當前時間並調整時間範圍
    now = datetime.now(timezone(timedelta(hours=8)))
    end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_range = end_of_day - timedelta(days=days)
    
    start_timestamp = int(start_of_range.timestamp())

    # 先對所有交易進行排序（按時間順序）
    all_transactions = sorted(transactions, key=lambda tx: tx.get('timestamp', 0))
    
    # 追蹤每個代幣的數據
    token_stats = {}
    current_holdings = {}
    
    # 處理全部交易記錄，使用FIFO方法計算PNL
    for tx in all_transactions:
        token_mint = tx.get('token_mint')
        tx_type = tx.get('transaction_type', '').lower()
        token_amount = tx.get('token_amount', 0)
        
        # 初始化代幣統計和持倉記錄
        if token_mint not in token_stats:
            token_stats[token_mint] = {
                'total_buy_amount': 0,
                'total_buy_cost_usd': 0,
                'total_sell_amount': 0,
                'total_sell_value_usd': 0,
                'realized_pnl_usd': 0,
                'buy_count': 0,
                'sell_count': 0
            }
        
        if token_mint not in current_holdings:
            current_holdings[token_mint] = {
                'total_amount': 0,
                'total_cost': 0,
                'buys': [],
                'avg_buy_price': 0
            }
        
        # 計算交易的USD價值
        tx_value_usd = 0
        if tx.get('bnb_amount', 0) > 0:
            tx_value_usd = tx['bnb_amount'] * bnb_usdt_price
        elif tx.get('usdc_amount', 0) > 0:
            tx_value_usd = tx['usdc_amount']
        elif tx.get('value', 0) > 0:  # 如果交易中直接有value字段，優先使用
            tx_value_usd = tx['value']
        else:
            continue
            
        stats = token_stats[token_mint]
        holding = current_holdings[token_mint]
        
        # 只處理時間範圍內的交易統計
        is_in_timerange = tx.get('timestamp', 0) >= start_timestamp
        
        if tx_type == 'buy':
            # 記錄買入批次(用於FIFO計算)
            holding['buys'].append({
                'amount': token_amount,
                'cost': tx_value_usd,
                'price': tx_value_usd / token_amount if token_amount > 0 else 0
            })
            
            # 更新持倉數據
            holding['total_amount'] += token_amount
            holding['total_cost'] += tx_value_usd
            
            # 更新平均買入價格
            if holding['total_amount'] > 0:
                holding['avg_buy_price'] = holding['total_cost'] / holding['total_amount']
            
            # 更新時間範圍內的統計數據
            if is_in_timerange:
                stats['buy_count'] += 1
                stats['total_buy_amount'] += token_amount
                stats['total_buy_cost_usd'] += tx_value_usd
            
        elif tx_type == 'sell':
            # 計算已實現利潤（使用FIFO方法）
            realized_profit = 0
            cost_basis = 0
            remaining_to_sell = token_amount
            
            if holding['buys']:
                # 創建臨時買入批次列表進行計算，避免影響原始數據
                temp_buys = [dict(buy) for buy in holding['buys']]
                
                # 使用FIFO方法計算賣出成本和利潤
                while remaining_to_sell > 0 and temp_buys:
                    oldest_buy = temp_buys[0]
                    
                    if oldest_buy['amount'] <= remaining_to_sell:
                        # 整個批次都被賣出
                        cost_basis += oldest_buy['cost']
                        remaining_to_sell -= oldest_buy['amount']
                        temp_buys.pop(0)
                    else:
                        # 只賣出部分批次
                        sell_ratio = remaining_to_sell / oldest_buy['amount']
                        partial_cost = oldest_buy['cost'] * sell_ratio
                        cost_basis += partial_cost
                        oldest_buy['amount'] -= remaining_to_sell
                        oldest_buy['cost'] *= (1 - sell_ratio)
                        remaining_to_sell = 0
                
                # 計算已實現利潤
                realized_profit = tx_value_usd - cost_basis
                
                # 更新實際的持倉數據
                holding['buys'] = temp_buys
                holding['total_amount'] = max(0, holding['total_amount'] - token_amount)
                
                # 如果完全賣出，重置成本
                if holding['total_amount'] <= 0:
                    holding['total_cost'] = 0
                    holding['avg_buy_price'] = 0
                else:
                    # 重新計算總成本和平均價格
                    holding['total_cost'] = sum(buy['cost'] for buy in holding['buys'])
                    holding['avg_buy_price'] = holding['total_cost'] / holding['total_amount'] if holding['total_amount'] > 0 else 0
            else:
                # 如果沒有買入記錄（可能是空投），全部視為利潤
                realized_profit = tx_value_usd
            
            # 更新時間範圍內的統計數據
            if is_in_timerange:
                stats['sell_count'] += 1
                stats['total_sell_amount'] += token_amount
                stats['total_sell_value_usd'] += tx_value_usd
                stats['realized_pnl_usd'] += realized_profit
    
    # 只考慮時間範圍內的交易來計算統計數據
    filtered_stats = {token: stats for token, stats in token_stats.items() 
                     if stats['buy_count'] > 0 or stats['sell_count'] > 0}
    
    # 計算總統計數據
    total_buy = sum(stats['buy_count'] for stats in filtered_stats.values())
    total_sell = sum(stats['sell_count'] for stats in filtered_stats.values())
    total_cost = sum(stats['total_buy_cost_usd'] for stats in filtered_stats.values())
    total_sell_value = sum(stats['total_sell_value_usd'] for stats in filtered_stats.values())
    total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in filtered_stats.values())
    
    # 計算勝率：只考慮有買入記錄的代幣
    tokens_with_buys = [token for token, stats in filtered_stats.items() if stats['total_buy_amount'] > 0]
    profitable_tokens = sum(1 for token in tokens_with_buys if filtered_stats[token]['realized_pnl_usd'] > 0)
    total_tokens = len(tokens_with_buys)
    
    # 計算總PNL
    pnl = total_realized_pnl
    
    # 計算PNL百分比：已實現PNL / 總賣出價值
    pnl_percentage = (pnl / total_cost) * 100 if total_cost > 0 else 0
    # 確保不低於-100%
    pnl_percentage = max(pnl_percentage, -100)
    
    # 勝率：盈利代幣數量/總代幣數量
    win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0
    
    # 其他統計指標
    average_cost = total_cost / total_buy if total_buy > 0 else 0
    asset_multiple = pnl_percentage / 100
    
    # 計算每日PNL數據（基於原始交易記錄的realized_profit字段，或使用我們計算的值）
    daily_pnl = {}
    filtered_transactions = [tx for tx in all_transactions if tx.get('timestamp', 0) >= start_timestamp]
    
    for tx in filtered_transactions:
        if tx.get('transaction_type', '').lower() == 'sell':
            date_str = time.strftime("%Y-%m-%d", time.localtime(tx.get('timestamp', 0)))
            
            # 優先使用交易中的realized_profit字段
            if 'realized_profit' in tx:
                realized_profit = tx['realized_profit']
            else:
                # 如果沒有，使用我們之前計算的值（但這可能不準確）
                token_mint = tx.get('token_mint')
                if token_mint in filtered_stats:
                    realized_profit = 0  # 簡化處理
                else:
                    realized_profit = 0
            
            # 累加到當日PNL
            daily_pnl[date_str] = daily_pnl.get(date_str, 0) + realized_profit

    # 生成每日PNL圖表數據
    date_range = []
    current_date = end_of_day
    for i in range(days):
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date = current_date - timedelta(days=1)
    
    # 填充每一天的PNL數據，如果沒有則為0
    filled_daily_pnl = []
    for date_str in date_range:
        if date_str in daily_pnl:
            filled_daily_pnl.append(round(daily_pnl[date_str], 2))
        else:
            filled_daily_pnl.append(0)
    
    # 根據指定的天數範圍生成PNL圖表數據
    if days == 1:
        daily_pnl_chart = [f"{filled_daily_pnl[0]}"] if filled_daily_pnl else ["0"]
    else:
        daily_pnl_chart = [f"{pnl}" for pnl in filled_daily_pnl]

    # 準備代幣分佈數據（假設有calculate_distribution函數）
    aggregated_tokens = {}
    for token_mint, stats in filtered_stats.items():
        if stats['total_buy_amount'] > 0:
            aggregated_tokens[token_mint] = {
                'profit': stats['total_sell_value_usd'],
                'cost': stats['total_buy_cost_usd']
            }

    # 假設calculate_distribution函數存在
    distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)
    
    
    return {
        "asset_multiple": round(asset_multiple, 2),
        "total_buy": total_buy,
        "total_sell": total_sell,
        "buy_num": total_buy,
        "sell_num": total_sell,
        "total_transaction_num": total_buy + total_sell,
        "total_cost": total_cost,
        "average_cost": average_cost,
        "avg_cost": average_cost,
        "total_profit": total_sell_value,
        "pnl": pnl,
        "pnl_percentage": round(pnl_percentage, 2),
        "win_rate": round(win_rate, 2),
        "avg_realized_profit": round(pnl / total_tokens if total_tokens > 0 else 0, 2),
        "daily_pnl_chart": ",".join(daily_pnl_chart),
        "pnl_pic": ",".join(daily_pnl_chart),
        **distribution,
        **distribution_percentage
    }

async def update_smart_wallets_filter(wallet_transactions, bnb_usdt_price, session, client, chain):
    """根據交易記錄計算盈亏、胜率，並篩選聰明錢包"""
    for wallet_address, transactions in wallet_transactions.items():
        if not transactions:
            # print(f"錢包 {wallet_address} 沒有交易記錄，跳過處理")
            return False
            
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        
        for transaction in transactions:
            tx_timestamp = transaction.get("timestamp", 0)  # 獲取交易時間戳
            
            for token_mint, data in transaction.items():
                if token_mint == "timestamp" or not isinstance(data, dict):
                    continue
                    
                # 更新代幣的交易數據
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                
                # 更新代幣的最後交易時間
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), tx_timestamp)

        # 取出所有有 token_mint 的交易
        token_time_pairs = [
            (tx.get("token_mint"), tx.get("timestamp", 0))
            for tx in transactions
            if tx.get("token_mint")
        ]

        # 依照 timestamp 由新到舊排序
        token_time_pairs = sorted(token_time_pairs, key=lambda x: x[1], reverse=True)

        # 取最近三個不同的 token_mint
        seen = set()
        recent_tokens = []
        for token, _ in token_time_pairs:
            if token not in seen:
                recent_tokens.append(token)
                seen.add(token)
            if len(recent_tokens) == 3:
                break

        token_list = ','.join(recent_tokens) if recent_tokens else None
        
        # 計算 1 日、7 日、30 日統計數據
        stats_1d = await calculate_statistics2(transactions, 1, bnb_usdt_price)
        stats_7d = await calculate_statistics2(transactions, 7, bnb_usdt_price)
        stats_30d = await calculate_statistics2(transactions, 30, bnb_usdt_price)

        wallet_balances = await fetch_wallet_balances([wallet_address], bnb_usdt_price)
        balance = wallet_balances.get(wallet_address, {}).get('balance', Decimal('0'))
        balance_usd = wallet_balances.get(wallet_address, {}).get('balance_usd', Decimal('0'))
        
        # 安全處理最後交易時間
        last_transaction_time = 0
        if transactions:
            last_transaction_time = max(tx.get("timestamp", 0) for tx in transactions)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(balance, 3),
            "balance_usd": round(balance_usd, 2),
            "chain": "BSC",
            "tag": "",
            "is_smart_wallet": True,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": token_list,
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": last_transaction_time,
            "distribution_30d": {
                "gt500": stats_30d.get("distribution_gt500", 0),
                "200to500": stats_30d.get("distribution_200to500", 0),
                "0to200": stats_30d.get("distribution_0to200", 0),
                "0to50": stats_30d.get("distribution_0to50", 0),
                "lt50": stats_30d.get("distribution_lt50", 0),
            },
            "distribution_percentage_30d": {
                "gt500": stats_30d.get("distribution_gt500_percentage", 0.0),
                "200to500": stats_30d.get("distribution_200to500_percentage", 0.0),
                "0to200": stats_30d.get("distribution_0to200_percentage", 0.0),
                "0to50": stats_30d.get("distribution_0to50_percentage", 0.0),
                "lt50": stats_30d.get("distribution_lt50_percentage", 0.0),
            },
            "distribution_7d": {
                "gt500": stats_7d.get("distribution_gt500", 0),
                "200to500": stats_7d.get("distribution_200to500", 0),
                "0to200": stats_7d.get("distribution_0to200", 0),
                "0to50": stats_7d.get("distribution_0to50", 0),
                "lt50": stats_7d.get("distribution_lt50", 0),
            },
            "distribution_percentage_7d": {
                "gt500": stats_7d.get("distribution_gt500_percentage", 0.0),
                "200to500": stats_7d.get("distribution_200to500_percentage", 0.0),
                "0to200": stats_7d.get("distribution_0to200_percentage", 0.0),
                "0to50": stats_7d.get("distribution_0to50_percentage", 0.0),
                "lt50": stats_7d.get("distribution_lt50_percentage", 0.0),
            },
        }
        wallet_data["pnl_pic_1d"] = stats_1d.get("pnl_pic", "")
        wallet_data["pnl_pic_7d"] = stats_7d.get("pnl_pic", "")
        wallet_data["pnl_pic_30d"] = stats_30d.get("pnl_pic", "")
        print(f"[DEBUG] stats_1d: buy_num={wallet_data['stats_1d'].get('buy_num')}, sell_num={wallet_data['stats_1d'].get('sell_num')}, total_transaction_num={wallet_data['stats_1d'].get('total_transaction_num')}")
        print(f"[DEBUG] stats_7d: buy_num={wallet_data['stats_7d'].get('buy_num')}, sell_num={wallet_data['stats_7d'].get('sell_num')}, total_transaction_num={wallet_data['stats_7d'].get('total_transaction_num')}")
        print(f"[DEBUG] stats_30d: buy_num={wallet_data['stats_30d'].get('buy_num')}, sell_num={wallet_data['stats_30d'].get('sell_num')}, total_transaction_num={wallet_data['stats_30d'].get('total_transaction_num')}")
        
        await write_wallet_data_to_db(session, wallet_data, chain)
        current_timestamp = int(time.time())
        thirty_days_ago = current_timestamp - (30 * 24 * 60 * 60)
        if (
            stats_30d.get("pnl", 0) > 0 and
            stats_30d.get("win_rate", 0) > 30 and
            stats_30d.get("win_rate", 0) != 100 and
            float(stats_30d.get("asset_multiple", 0)) > 0.3 and
            stats_30d.get("total_transaction_num", 0) < 2000 and
            last_transaction_time >= thirty_days_ago
        ):            
            return True  # 返回 True 表示满足条件
        else:
            return False

async def update_smart_money_data(session, wallet_address, chain, bnb_usdt_price, is_smart_wallet=None, wallet_type=None, days=30, limit=100):
    """查詢指定錢包過去30天內的所有交易數據並分析"""
    client = os.getenv('RPC_URL')
    print(f"正在查詢 {wallet_address} 錢包 {days} 天內的交易數據...")

    wallet_transactions = {wallet_address: []}  # 改用普通字典而不是 defaultdict
    # 确保 bnb_usdt_price 是 Decimal 类型
    if bnb_usdt_price is None:
        bnb_token_info = await TokenInfoFetcher.get_token_info("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
        if bnb_token_info and "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c" in bnb_token_info:
            bnb_usdt_price = Decimal(str(bnb_token_info["0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"].get("priceUsd", "683.8")))
        else:
            bnb_usdt_price = Decimal("683.8")
    else:
        bnb_usdt_price = Decimal(str(bnb_usdt_price))

    try:
        all_transactions = await get_transactions_for_wallet(
            session=session, chain=chain, wallet_address=wallet_address, days=days
        )

        if all_transactions and len(all_transactions) > 0:
            await reset_wallet_buy_data(wallet_address, session, chain)
    except Exception as e:
        print(f"獲取交易紀錄失敗: {e}")
        return

    for tx_data in all_transactions:
        token_address = tx_data["token_address"]
        if not token_address or token_address == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c" or token_address in [BUSD_MINT]:
            continue

        transaction = {
            "token_mint": token_address,
            "transaction_type": tx_data["transaction_type"],
            "token_amount": tx_data["amount"],
            "timestamp": tx_data["transaction_time"],
            "value": tx_data["value"],
            "realized_profit": tx_data["realized_profit"]
        }
        
        wallet_transactions[wallet_address].append(transaction)
        # print(f"Added transaction for {wallet_address}: {transaction}")

    print(f"Total transactions for {wallet_address}: {len(wallet_transactions[wallet_address])}")
    formatted_transactions = convert_transaction_format(wallet_transactions, wallet_address, bnb_usdt_price)
    # print(f"Formatted transactions: {formatted_transactions}")
    
    if formatted_transactions and formatted_transactions.get(wallet_address):
        await calculate_token_statistics(formatted_transactions, wallet_address, session, chain, client)
        is_smart_wallet = await update_smart_wallets_filter(formatted_transactions, bnb_usdt_price, session, client, chain)
        
        if is_smart_wallet:
            await activate_wallets(session, [wallet_address])
        else:
            await deactivate_wallets(session, [wallet_address])
    else:
        # print(f"No valid transactions found for {wallet_address}")
        await deactivate_wallets(session, [wallet_address])

async def process_wallet_with_new_session(session_factory, wallet_address, chain):
    """
    为每个钱包创建独立的 AsyncSession 并处理数据
    """
    try:
        async with session_factory() as session:
            bnb_token_info = await TokenInfoFetcher.get_token_info("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
            bnb_usdt_price = Decimal(str(bnb_token_info.get("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c", {}).get("priceUsd", "683.8")))
            # logging.info(f"Processing wallet: {wallet_address}")
            await update_smart_money_data(session, wallet_address, chain, bnb_usdt_price)
            # logging.info(f"Completed processing wallet: {wallet_address}")
    except Exception as e:
        logging.error(f"Error processing wallet {wallet_address}: {e}")
        logging.error(traceback.format_exc())

async def get_smart_wallets(session, chain):
    """
    获取 WalletSummary 表中 is_smart_wallet 为 True 的钱包地址
    """
    schema = 'dex_query_v1'
    WalletSummary.with_schema(schema)
    result = await session.execute(
        select(WalletSummary.wallet_address).where(WalletSummary.is_smart_wallet == True)
    )
    return [row[0] for row in result]

async def update_bsc_smart_money_data():
    """
    每日更新 bsc 链的活跃钱包数据，并清理数据库中过期或异常数据
    """
    try:
        logging.info("Starting daily update for BSC smart money data and database cleanup...")

        chain = "BSC"
        session_factory = sessions.get(chain.upper())
        if not session_factory:
            logging.error("Session factory for BSC chain is not configured.")
            return

        async with session_factory() as session:

            active_wallets = await get_smart_wallets(session, chain)
            if not active_wallets:
                logging.info("No active wallets found for BSC.")
                return

        batch_size = 5
        for i in range(0, len(active_wallets), batch_size):
            batch_wallets = active_wallets[i:i + batch_size]
            logging.info(f"Processing batch {i // batch_size + 1} with {len(batch_wallets)} wallets.")

            # 为每个钱包创建独立的任务
            tasks = [
                process_wallet_with_new_session(session_factory, wallet_address, chain)
                for wallet_address in batch_wallets
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

            logging.info(f"Completed processing batch {i // batch_size + 1}.")

        logging.info("Daily update and cleanup for BSC smart money data completed successfully.")

    except Exception as e:
        logging.error(f"Error during daily update and cleanup: {e}")
        logging.error(traceback.format_exc())