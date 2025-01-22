import aiohttp
from decimal import Decimal
import asyncio
from main import log_execution_time

# JSON-RPC 配置
BSC_RPC_URL = "https://summer-necessary-diagram.bsc.quiknode.pro/f5a6381fc4834f55d98e9cdef8df8b16b63587bd"

MAX_RETRIES = 10
RETRY_DELAY = 2

async def get_price():
    url = "https://www.binance.com/api/v3/ticker/price?symbol=BNBUSDT"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return Decimal(data['price'])  # 使用 Decimal 处理价格

# 获取 BNB 余额
async def get_bnb_balance(address):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [address, "latest"],
        "id": 1
    }
    for attempt in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(BSC_RPC_URL, json=payload) as response:
                    if response.status == 429:  # Too Many Requests
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))  # 指数级延迟
                        continue
                    response.raise_for_status()
                    result = await response.json()
                    balance_wei = int(result['result'], 16)
                    return Decimal(balance_wei) / Decimal(10 ** 18)
        except aiohttp.ClientError as e:
            print(f"Error fetching BNB balance for {address}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    raise Exception(f"Failed to fetch BNB balance for {address} after {MAX_RETRIES} retries.")

# 获取 BEP20 代币余额
async def get_token_balance(address, token_contract):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": token_contract,
            "data": "0x70a08231000000000000000000000000" + address[2:]
        }, "latest"],
        "id": 1
    }
    for attempt in range(MAX_RETRIES):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(BSC_RPC_URL, json=payload) as response:
                    if response.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    response.raise_for_status()
                    result = await response.json()
                    balance_wei = int(result['result'], 16)
                    return Decimal(balance_wei) / Decimal(10 ** 18)
        except aiohttp.ClientError as e:
            print(f"Error fetching token balance for {address}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    raise Exception(f"Failed to fetch token balance for {address} after {MAX_RETRIES} retries.")

# 并发获取多钱包余额
async def fetch_wallet_balances(wallets, bnb_price):
    semaphore = asyncio.Semaphore(10)  # 限制并发数为 10

    async def fetch_wallet(wallet):
        async with semaphore:
            bnb_balance = await get_bnb_balance(wallet)
            # token_balances = await asyncio.gather(
            #     *[get_token_balance(wallet, token_address) for token_address in token_contracts.values()]
            # )
            return {
                "balance": bnb_balance,
                "balance_USD": bnb_balance * bnb_price,
                # "tokens": {
                #     token_name: token_balances[idx] for idx, token_name in enumerate(token_contracts)
                # }
            }

    balances = await asyncio.gather(*[fetch_wallet(wallet) for wallet in wallets])
    return {wallets[idx]: balances[idx] for idx in range(len(wallets))}