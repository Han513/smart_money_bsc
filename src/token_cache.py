import redis
import json
import os
from decimal import Decimal
from datetime import datetime, timedelta
import logging
from sqlalchemy import select
from models import TokenBuyData, WalletTokenState
from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB, REDIS_POOL_SIZE

logger = logging.getLogger(__name__)

class TokenCache:
    def __init__(self, host=None, port=None, password=None, db=None, pool_size=None):
        # 从环境变量获取Redis配置，如果没有提供参数则使用默认值
        self.host = host or REDIS_HOST
        self.port = int(port or REDIS_PORT)
        self.password = password or REDIS_PASSWORD
        self.db = db if db is not None else REDIS_DB
        self.pool_size = pool_size if pool_size is not None else REDIS_POOL_SIZE
        
        # 创建Redis连接池
        if self.password:
            self.redis = redis.Redis(
                host=self.host, 
                port=self.port, 
                password=self.password,
                db=self.db, 
                decode_responses=True,
                max_connections=self.pool_size
            )
        else:
            self.redis = redis.Redis(
                host=self.host, 
                port=self.port, 
                db=self.db, 
                decode_responses=True,
                max_connections=self.pool_size
            )
        
        self.flush_interval = 300  # 5分钟刷新一次
        self.last_flush_time = datetime.now()

    def _get_key(self, wallet_address, token_address):
        return f"token:{wallet_address.lower()}:{token_address.lower()}"

    def _decimal_to_str(self, data):
        """将字典中的 Decimal 转换为字符串"""
        return {k: str(v) if isinstance(v, Decimal) else v for k, v in data.items()}

    def _str_to_decimal(self, data):
        """将字典中的字符串转换为 Decimal"""
        decimal_fields = {'total_amount', 'total_cost', 'avg_buy_price', 'realized_profit', 'realized_profit_percentage'}
        for key in decimal_fields:
            if key in data and data[key] is not None:
                try:
                    data[key] = Decimal(str(data[key]))
                except Exception:
                    data[key] = Decimal('0')
            else:
                data[key] = Decimal('0')
        return data

    async def get_token_data(self, wallet_address, token_address, session):
        """获取代币数据，优先Redis，其次DB，最后默认"""
        key = self._get_key(wallet_address, token_address)
        
        # 1. Try Redis cache
        try:
            cached_data_str = self.redis.get(key)
            if cached_data_str:
                return self._str_to_decimal(json.loads(cached_data_str))
        except Exception as e:
            logger.error(f"访问 Redis 失败: {e}")

        # 2. Try DB (from new wallet_token_state table)
        try:
            stmt = select(WalletTokenState).where(
                WalletTokenState.wallet_address == wallet_address,
                WalletTokenState.token_address == token_address,
                WalletTokenState.chain == 'BSC' # Hardcoded for now
            )
            
            result = await session.execute(stmt)
            latest_record = result.scalar_one_or_none()

            if latest_record:
                logger.info(f"缓存未命中，从数据库加载状态: {wallet_address}/{token_address}")
                # We only need the *current* state for calculations
                db_data = {
                    'total_amount': latest_record.current_amount,
                    'total_cost': latest_record.current_total_cost,
                    'avg_buy_price': latest_record.current_avg_buy_price,
                    'realized_profit': latest_record.historical_realized_pnl, # Use historical for tracking
                    'realized_profit_percentage': 0, # This is trade-specific, not stateful
                    'last_transaction_time': latest_record.last_transaction_time
                }
                # Put it back in Redis for next time
                try:
                    self.redis.set(key, json.dumps(self._decimal_to_str(db_data)))
                except Exception as e:
                    logger.error(f"写回 Redis 缓存失败: {e}")
                
                return self._str_to_decimal(db_data)
        except Exception as e:
            logger.error(f"从数据库加载缓存状态失败: {e}")

        # 3. Return default if not found anywhere
        return {
            'total_amount': Decimal('0'),
            'total_cost': Decimal('0'),
            'avg_buy_price': Decimal('0'),
            'realized_profit': Decimal('0'),
            'realized_profit_percentage': Decimal('0'),
            'last_transaction_time': 0
        }

    def update_token_data(self, wallet_address, token_address, side, amount, value, current_time, token_data):
        """更新代币数据 (token_data is the state before this trade)"""
        
        amount = Decimal(str(amount))
        value = Decimal(str(value))
        
        if side == 'buy':
            # 买入逻辑
            new_total_amount = token_data['total_amount'] + amount
            new_total_cost = token_data['total_cost'] + value
            new_avg_price = new_total_cost / new_total_amount if new_total_amount > 0 else Decimal('0')
            
            token_data.update({
                'total_amount': new_total_amount,
                'total_cost': new_total_cost,
                'avg_buy_price': new_avg_price
            })
            
            # logger.info(f"买入更新 - 钱包: {wallet_address}, 代币: {token_address}")
            
        else:
            # 卖出逻辑
            if token_data['total_amount'] > 0:
                # 计算卖出部分的成本
                sell_ratio = min(amount / token_data['total_amount'], Decimal('1'))
                cost_basis = token_data['total_cost'] * sell_ratio
                
                # 计算已实现利润
                realized_profit = value - cost_basis
                if cost_basis > 0:
                    realized_profit_percentage = ((value / cost_basis) - Decimal('1')) * Decimal('100')
                else:
                    realized_profit_percentage = Decimal('0')
                
                # 更新总量和成本
                token_data['total_amount'] -= amount
                if token_data['total_amount'] <= Decimal('1e-9'):  # 清仓
                    token_data['total_amount'] = Decimal('0')
                    token_data['total_cost'] = Decimal('0')
                    token_data['avg_buy_price'] = Decimal('0')
                else:
                    token_data['total_cost'] *= (1 - sell_ratio)
                    token_data['avg_buy_price'] = token_data['total_cost'] / token_data['total_amount']
                
                # 累加已实现利润
                token_data['realized_profit'] = token_data.get('realized_profit', Decimal('0')) + realized_profit
                token_data['realized_profit_percentage'] = realized_profit_percentage
                
                # logger.info(f"卖出更新 - 钱包: {wallet_address}, 代币: {token_address}")
            else:
                # 处理超卖情况
                token_data['total_amount'] -= amount
                token_data['realized_profit'] = Decimal('0')
                token_data['realized_profit_percentage'] = Decimal('0')
                
                # logger.info(f"超卖情况 - 钱包: {wallet_address}, 代币: {token_address}")
        
        key = self._get_key(wallet_address, token_address)
        token_data['last_transaction_time'] = current_time
        self.redis.set(key, json.dumps(self._decimal_to_str(token_data)))
        return token_data

    def determine_transaction_type(self, side, amount, current_holding):
        """判断交易类型"""
        if side == 'buy':
            return "build" if current_holding <= Decimal('1e-9') else "buy"
        else:  # sell
            if current_holding <= Decimal('1e-9'):
                return "sell"
            elif amount >= current_holding - Decimal('1e-9'):
                return "clean"
            else:
                return "sell"

    async def maybe_flush_to_db(self, session):
        """检查是否需要刷新到数据库"""
        now = datetime.now()
        if (now - self.last_flush_time).total_seconds() >= self.flush_interval:
            await self.flush_to_db(session)
            self.last_flush_time = now

    async def flush_to_db(self, session):
        """将缓存数据刷新到数据库(此函數將被廢棄，改為即時更新)"""
        logger.warning("flush_to_db is deprecated and should not be used. State is now persisted in real-time.")
        return

# 创建全局缓存实例
token_cache = TokenCache() 