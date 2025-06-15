#!/usr/bin/env python3
"""
验证 wallet_buy_data 表的数据写入逻辑
"""

import asyncio
import sys
import os
from decimal import Decimal
from datetime import datetime, date, timedelta

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.models import save_wallet_buy_data
from src.config import DATABASE_URI_SWAP_BSC
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

async def test_wallet_buy_data_logic():
    """测试 wallet_buy_data 的写入逻辑"""
    
    # 创建数据库连接
    engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
    async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    
    test_wallet = "0x1234567890123456789012345678901234567890"
    test_token = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdef"
    test_date = date.today()
    
    print(f"🧪 开始测试 wallet_buy_data 写入逻辑")
    print(f"测试钱包: {test_wallet}")
    print(f"测试代币: {test_token}")
    print(f"测试日期: {test_date}")
    
    async with async_session() as session:
        try:
            # 清理测试数据
            await session.execute(text("SET search_path TO dex_query_v1;"))
            await session.execute(text("""
                DELETE FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token
            """), {"wallet": test_wallet, "token": test_token})
            await session.commit()
            print("✅ 清理了旧的测试数据")
            
            # 测试1: 第一笔买入交易
            print("\n📝 测试1: 第一笔买入交易")
            tx_data_1 = {
                "token_address": test_token,
                "avg_buy_price": 1.0,
                "total_amount": 100.0,
                "total_cost": 100.0,
                "total_buy_count": 1,
                "total_sell_count": 0,
                "historical_total_buy_amount": 100.0,
                "historical_total_buy_cost": 100.0,
                "historical_total_sell_amount": 0.0,
                "historical_total_sell_value": 0.0,
                "last_transaction_time": 1700000000,
                "date": test_date
            }
            
            result = await save_wallet_buy_data(test_wallet, tx_data_1, test_token, session, "BSC", auto_commit=True)
            print(f"写入结果: {result}")
            
            # 查询结果
            result = await session.execute(text("""
                SELECT total_amount, total_cost, avg_buy_price, 
                       historical_total_buy_amount, historical_total_buy_cost,
                       total_buy_count, last_transaction_time
                FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token AND date = :date
            """), {"wallet": test_wallet, "token": test_token, "date": test_date})
            row = result.fetchone()
            if row:
                print(f"✅ 第一笔交易写入成功:")
                print(f"   当前持仓: {row[0]}, 成本: {row[1]}, 均价: {row[2]}")
                print(f"   历史买入: {row[3]}, 历史成本: {row[4]}")
                print(f"   买入次数: {row[5]}, 最后交易时间: {row[6]}")
            else:
                print("❌ 第一笔交易写入失败")
                return
            
            # 测试2: 第二笔买入交易（更新时间戳）
            print("\n📝 测试2: 第二笔买入交易")
            tx_data_2 = {
                "token_address": test_token,
                "avg_buy_price": 1.25,  # 新的平均价格
                "total_amount": 200.0,  # 新的总持仓
                "total_cost": 250.0,    # 新的总成本
                "total_buy_count": 1,   # 这笔交易的增量
                "total_sell_count": 0,
                "historical_total_buy_amount": 100.0,  # 这笔交易的增量
                "historical_total_buy_cost": 150.0,    # 这笔交易的增量
                "historical_total_sell_amount": 0.0,
                "historical_total_sell_value": 0.0,
                "last_transaction_time": 1700000100,  # 更新的时间戳
                "date": test_date
            }
            
            result = await save_wallet_buy_data(test_wallet, tx_data_2, test_token, session, "BSC", auto_commit=True)
            print(f"写入结果: {result}")
            
            # 查询结果
            result = await session.execute(text("""
                SELECT total_amount, total_cost, avg_buy_price, 
                       historical_total_buy_amount, historical_total_buy_cost,
                       total_buy_count, last_transaction_time
                FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token AND date = :date
            """), {"wallet": test_wallet, "token": test_token, "date": test_date})
            row = result.fetchone()
            if row:
                print(f"✅ 第二笔交易累加成功:")
                print(f"   当前持仓: {row[0]}, 成本: {row[1]}, 均价: {row[2]}")
                print(f"   历史买入: {row[3]}, 历史成本: {row[4]} (应该是 250.0)")
                print(f"   买入次数: {row[5]} (应该是 2), 最后交易时间: {row[6]}")
            else:
                print("❌ 第二笔交易写入失败")
                return
            
            # 测试3: 重复交易（相同时间戳，应该被跳过）
            print("\n📝 测试3: 重复交易（相同时间戳）")
            tx_data_3 = {
                "token_address": test_token,
                "avg_buy_price": 2.0,
                "total_amount": 300.0,
                "total_cost": 600.0,
                "total_buy_count": 1,
                "total_sell_count": 0,
                "historical_total_buy_amount": 100.0,
                "historical_total_buy_cost": 200.0,
                "historical_total_sell_amount": 0.0,
                "historical_total_sell_value": 0.0,
                "last_transaction_time": 1700000100,  # 相同的时间戳
                "date": test_date
            }
            
            result = await save_wallet_buy_data(test_wallet, tx_data_3, test_token, session, "BSC", auto_commit=True)
            print(f"写入结果: {result}")
            
            # 查询结果，应该没有变化
            result = await session.execute(text("""
                SELECT total_amount, total_cost, avg_buy_price, 
                       historical_total_buy_amount, historical_total_buy_cost,
                       total_buy_count, last_transaction_time
                FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token AND date = :date
            """), {"wallet": test_wallet, "token": test_token, "date": test_date})
            row = result.fetchone()
            if row:
                print(f"✅ 重复交易正确跳过:")
                print(f"   当前持仓: {row[0]} (应该还是 200.0)")
                print(f"   历史买入: {row[3]} (应该还是 250.0)")
                print(f"   买入次数: {row[5]} (应该还是 2)")
            
            # 测试4: 卖出交易
            print("\n📝 测试4: 卖出交易")
            tx_data_4 = {
                "token_address": test_token,
                "avg_buy_price": 1.25,  # 保持不变
                "total_amount": 150.0,  # 卖出50个后剩余150个
                "total_cost": 187.5,    # 按比例减少成本
                "total_buy_count": 0,
                "total_sell_count": 1,   # 这笔交易的增量
                "historical_total_buy_amount": 0.0,
                "historical_total_buy_cost": 0.0,
                "historical_total_sell_amount": 50.0,  # 这笔交易的卖出数量
                "historical_total_sell_value": 100.0,  # 这笔交易的卖出价值
                "last_transaction_time": 1700000200,   # 新的时间戳
                "date": test_date
            }
            
            result = await save_wallet_buy_data(test_wallet, tx_data_4, test_token, session, "BSC", auto_commit=True)
            print(f"写入结果: {result}")
            
            # 查询结果
            result = await session.execute(text("""
                SELECT total_amount, total_cost, avg_buy_price, 
                       historical_total_buy_amount, historical_total_buy_cost,
                       historical_total_sell_amount, historical_total_sell_value,
                       total_buy_count, total_sell_count, last_transaction_time
                FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token AND date = :date
            """), {"wallet": test_wallet, "token": test_token, "date": test_date})
            row = result.fetchone()
            if row:
                print(f"✅ 卖出交易处理成功:")
                print(f"   当前持仓: {row[0]}, 成本: {row[1]}, 均价: {row[2]}")
                print(f"   历史买入: {row[3]}, 历史成本: {row[4]}")
                print(f"   历史卖出: {row[5]}, 历史价值: {row[6]}")
                print(f"   买入次数: {row[7]}, 卖出次数: {row[8]}")
                print(f"   最后交易时间: {row[9]}")
            
            print("\n🎉 所有测试完成！")
            
        except Exception as e:
            print(f"❌ 测试过程中发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理测试数据
            await session.execute(text("""
                DELETE FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address = :wallet AND token_address = :token
            """), {"wallet": test_wallet, "token": test_token})
            await session.commit()
            print("🧹 清理了测试数据")

if __name__ == "__main__":
    asyncio.run(test_wallet_buy_data_logic()) 