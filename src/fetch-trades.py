from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from decimal import Decimal
from datetime import datetime
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.8f' % x)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

DATABASE_URI = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5005/postgres'

engine = create_async_engine(DATABASE_URI, echo=True)
async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

def format_decimal(dec):
    if dec is None:
        return None
    # 將科學計數法轉換為普通數字
    return f"{dec:f}"

async def fetch_and_format_trades():
    async with async_session() as session:
        async with session.begin():
            await session.execute(text("SET search_path TO bsc_dex;"))
            
            result = await session.execute(text("""
                SELECT 
                    id,
                    chain_id,
                    token_in,
                    token_out,
                    amount_in,
                    amount_out,
                    decimals_in,
                    decimals_out,
                    base_amount,
                    quote_amount,
                    base_decimals,
                    quote_decimals,
                    base_balance,
                    quote_balance,
                    price_usd,
                    price,
                    side,
                    dex,
                    pair_address,
                    to_timestamp(created_at/1000) as created_at,
                    signer,
                    tx_hash,
                    block_number,
                    block_timestamp,
                    timestamp,
                    payload
                FROM trades 
                LIMIT 10;
            """))
            
            rows = result.fetchall()
            
            # 轉換成DataFrame
            df = pd.DataFrame(rows, columns=[
                'ID', 'Chain ID', 'Token In', 'Token Out', 'Amount In', 
                'Amount Out', 'Decimals In', 'Decimals Out', 'Base Amount',
                'Quote Amount', 'Base Decimals', 'Quote Decimals', 
                'Base Balance', 'Quote Balance', 'Price USD', 'Price',
                'Side', 'DEX', 'Pair Address', 'Created At', 'Signer',
                'TX Hash', 'Block Number', 'Block Timestamp', 'Timestamp', 'Payload'
            ])
            
            # 格式化所有Decimal類型的列
            decimal_columns = ['Amount In', 'Amount Out', 'Base Amount', 'Quote Amount',
                             'Base Balance', 'Quote Balance', 'Price USD', 'Price']
            
            for col in decimal_columns:
                df[col] = df[col].apply(lambda x: format_decimal(x) if x is not None else None)
            
            # 截短地址顯示
            address_columns = ['Token In', 'Token Out', 'Pair Address', 'Signer']
            for col in address_columns:
                df[col] = df[col].apply(lambda x: x[:10] + '...' if x else x)
            
            # 截短交易哈希
            df['TX Hash'] = df['TX Hash'].apply(lambda x: x[:10] + '...' if x else x)
            
            print("\n=== Trades Data ===\n")
            print(df.to_string())
            return df

# 運行異步任務
import asyncio
asyncio.run(fetch_and_format_trades())
