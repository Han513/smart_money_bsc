import redis
import json
from decimal import Decimal
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TokenCache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
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
        return {k: Decimal(v) if k in decimal_fields and v is not None else v for k, v in data.items()}

    def get_token_data(self, wallet_address, token_address):
        """获取代币数据"""
        key = self._get_key(wallet_address, token_address)
        data = self.redis.get(key)
        if data:
            return self._str_to_decimal(json.loads(data))
        return {
            'total_amount': Decimal('0'),
            'total_cost': Decimal('0'),
            'avg_buy_price': Decimal('0'),
            'realized_profit': Decimal('0'),
            'realized_profit_percentage': Decimal('0'),
            'last_transaction_time': 0
        }

    def update_token_data(self, wallet_address, token_address, side, amount, value, current_time):
        """更新代币数据"""
        key = self._get_key(wallet_address, token_address)
        token_data = self.get_token_data(wallet_address, token_address)
        
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
                'avg_buy_price': new_avg_price,
                'realized_profit': token_data.get('realized_profit', Decimal('0')),
                'realized_profit_percentage': token_data.get('realized_profit_percentage', Decimal('0'))
            })
            
            logger.info(f"买入更新 - 钱包: {wallet_address}, 代币: {token_address}")
            logger.info(f"数量: {amount}, 价值: {value}")
            logger.info(f"新总量: {new_total_amount}, 新总成本: {new_total_cost}, 新均价: {new_avg_price}")
            
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
                
                logger.info(f"卖出更新 - 钱包: {wallet_address}, 代币: {token_address}")
                logger.info(f"卖出数量: {amount}, 卖出价值: {value}")
                logger.info(f"成本基础: {cost_basis}, 已实现利润: {realized_profit}, 利润率: {realized_profit_percentage}%")
            else:
                # 处理超卖情况
                token_data['total_amount'] -= amount
                token_data['realized_profit'] = Decimal('0')
                token_data['realized_profit_percentage'] = Decimal('-100')
                
                logger.info(f"超卖情况 - 钱包: {wallet_address}, 代币: {token_address}")
                logger.info(f"当前持仓: {token_data['total_amount']}, 卖出数量: {amount}")
            
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
        """将缓存数据刷新到数据库"""
        from models import save_wallet_buy_data  # 避免循环导入
        
        keys = self.redis.keys("token:*")
        for key in keys:
            try:
                wallet_address, token_address = key.split(":")[1:3]
                data = self.get_token_data(wallet_address, token_address)
                
                # 准备要保存的数据
                save_data = {
                    "token_address": token_address,
                    "total_amount": float(data['total_amount']),
                    "total_cost": float(data['total_cost']),
                    "avg_buy_price": float(data['avg_buy_price']),
                    "realized_profit": float(data['realized_profit']),
                    "realized_profit_percentage": float(data['realized_profit_percentage']),
                    "last_transaction_time": data['last_transaction_time'],
                    "date": datetime.now().date()
                }
                
                await save_wallet_buy_data(
                    wallet_address,
                    save_data,
                    token_address,
                    session,
                    "BSC",
                    auto_commit=True
                )
                
                logger.info(f"已将缓存数据刷新到数据库 - 钱包: {wallet_address}, 代币: {token_address}")
                logger.info(f"数据: {save_data}")
                
            except Exception as e:
                logger.error(f"刷新缓存数据到数据库时出错: {str(e)}")
                continue

# 创建全局缓存实例
token_cache = TokenCache() 