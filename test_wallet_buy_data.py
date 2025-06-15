#!/usr/bin/env python3
"""
测试 wallet_buy_data 表的数据写入功能
"""

import asyncio
import sys
import os
from decimal import Decimal
from datetime import datetime, date

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.models import save_wallet_buy_data, TokenBuyData
from src.config import DATABASE_URI_SWAP_BSC
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, select

async def test_wallet_buy_data():
    """测试 wallet_buy_data 的写入和查询功能"""
    
    # 创建数据库连接
    engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
    async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    
    test_wallet = "0x1234567890123456789012345678901234567890"
    test_token = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
    
    async with async_session() as session:
        try:
            await session.execute(text("SET search_path TO dex_query_v1;"))
            
            # 测试数据 1: 买入交易
            buy_tx_data = {
                "token_address": test_token,
                "avg_buy_price": 1.5,
                "total_amount": 100.0,
                "total_cost": 150.0,
                "historical_total_buy_amount": 100.0,
                "historical_total_buy_cost": 150.0,
                "historical_avg_buy_price": 1.5,
                "historical_total_sell_amount": 0.0,
                "historical_total_sell_value": 0.0,
                "historical_avg_sell_price": 0.0,
                "total_buy_count": 1,
                "total_sell_count": 0,
                "last_transaction_time": int(datetime.now().timestamp()),
                "date": date.today()
            }
            
            print("测试买入交易数据写入...")
            result = await save_wallet_buy_data(
                test_wallet, 
                buy_tx_data, 
                test_token, 
                session, 
                "BSC", 
                auto_commit=True
            )
            print(f"买入交易写入结果: {result}")
            
            # 测试数据 2: 卖出交易
            sell_tx_data = {
                "token_address": test_token,
                "avg_buy_price": 1.5,
                "total_amount": 50.0,  # 卖出一半
                "total_cost": 75.0,
                "historical_total_buy_amount": 0.0,  # 这次不增加买入
                "historical_total_buy_cost": 0.0,
                "historical_avg_buy_price": 0.0,
                "historical_total_sell_amount": 50.0,  # 卖出50个
                "historical_total_sell_value": 100.0,  # 卖出价值100
                "historical_avg_sell_price": 2.0,
                "total_buy_count": 0,
                "total_sell_count": 1,
                "last_transaction_time": int(datetime.now().timestamp()),
                "date": date.today()
            }
            
            print("测试卖出交易数据写入...")
            result = await save_wallet_buy_data(
                test_wallet, 
                sell_tx_data, 
                test_token, 
                session, 
                "BSC", 
                auto_commit=True
            )
            print(f"卖出交易写入结果: {result}")
            
            # 查询结果验证
            print("查询写入的数据...")
            TokenBuyData.__table__.schema = "dex_query_v1"
            query = select(TokenBuyData).where(
                TokenBuyData.wallet_address == test_wallet,
                TokenBuyData.token_address == test_token
            )
            result = await session.execute(query)
            record = result.scalars().first()
            
            if record:
                print("查询到的记录:")
                print(f"  钱包地址: {record.wallet_address}")
                print(f"  代币地址: {record.token_address}")
                print(f"  当前持仓: amount={record.total_amount}, cost={record.total_cost}, avg_price={record.avg_buy_price}")
                print(f"  历史买入: amount={record.historical_total_buy_amount}, cost={record.historical_total_buy_cost}, avg_price={record.historical_avg_buy_price}")
                print(f"  历史卖出: amount={record.historical_total_sell_amount}, value={record.historical_total_sell_value}, avg_price={record.historical_avg_sell_price}")
                print(f"  交易次数: buy_count={record.total_buy_count}, sell_count={record.total_sell_count}")
                print(f"  最后交易时间: {record.last_transaction_time}")
                print(f"  更新时间: {record.updated_at}")
                
                # 验证数据是否正确
                expected_buy_amount = 100.0  # 原始买入
                expected_buy_cost = 150.0
                expected_sell_amount = 50.0  # 累计卖出
                expected_sell_value = 100.0
                
                if (abs(record.historical_total_buy_amount - expected_buy_amount) < 0.01 and
                    abs(record.historical_total_buy_cost - expected_buy_cost) < 0.01 and
                    abs(record.historical_total_sell_amount - expected_sell_amount) < 0.01 and
                    abs(record.historical_total_sell_value - expected_sell_value) < 0.01):
                    print("✅ 数据验证通过！历史字段正确累加")
                else:
                    print("❌ 数据验证失败！历史字段未正确累加")
                    print(f"  期望买入: amount={expected_buy_amount}, cost={expected_buy_cost}")
                    print(f"  实际买入: amount={record.historical_total_buy_amount}, cost={record.historical_total_buy_cost}")
                    print(f"  期望卖出: amount={expected_sell_amount}, value={expected_sell_value}")
                    print(f"  实际卖出: amount={record.historical_total_sell_amount}, value={record.historical_total_sell_value}")
            else:
                print("❌ 未找到测试记录")
                
            # 清理测试数据
            print("清理测试数据...")
            await session.execute(text("""
                DELETE FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token
            """), {"wallet": test_wallet, "token": test_token})
            await session.commit()
            print("测试数据已清理")
            
        except Exception as e:
            print(f"测试过程中发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            await engine.dispose()

if __name__ == "__main__":
    print("开始测试 wallet_buy_data 功能...")
    asyncio.run(test_wallet_buy_data())
    print("测试完成") 