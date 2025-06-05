import aiohttp
import backoff
import asyncio
from decimal import Decimal
from typing import Dict, List
from aiohttp import ClientTimeout
from contextlib import asynccontextmanager
from config import RPC_URL

# JSON-RPC 配置
# BSC_RPC_URL = "https://frosty-wild-knowledge.bsc.quiknode.pro/9a7d28ef93c08d9afdfd5fbb1cc51d6998deed04"

MAX_RETRIES = 3
RETRY_DELAY = 2
CONCURRENT_REQUESTS = 5
TIMEOUT_SECONDS = 30
BACKOFF_MAX_TIME = 60

# 获取 BNB 余额
class RPCClient:
    def __init__(self, rpc_url: str, timeout: int = TIMEOUT_SECONDS):
        self.rpc_url = rpc_url
        self.timeout = ClientTimeout(total=timeout)
        self.session = None
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=MAX_RETRIES,
        max_time=BACKOFF_MAX_TIME
    )
    async def get_bnb_balance(self, address: str) -> Decimal:
        """使用指數退避的重試機制獲取 BNB 餘額"""
        async with self.semaphore:
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [address, "latest"],
                "id": 1
            }
            
            try:
                async with self.session.post(self.rpc_url, json=payload) as response:
                    if response.status == 429:  # Too Many Requests
                        raise aiohttp.ClientError("Rate limit exceeded")
                    
                    response.raise_for_status()
                    result = await response.json()
                    
                    if 'error' in result:
                        raise aiohttp.ClientError(f"RPC Error: {result['error']}")
                    
                    balance_wei = int(result['result'], 16)
                    return Decimal(balance_wei) / Decimal(10 ** 18)
                    
            except asyncio.TimeoutError:
                print(f"Timeout while fetching balance for {address}")
                raise
            except aiohttp.ClientError as e:
                print(f"Error fetching balance for {address}: {str(e)}")
                raise

async def get_price():
    url = "https://www.binance.com/api/v3/ticker/price?symbol=BNBUSDT"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return Decimal(data['price'])  # 使用 Decimal 处理价格

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
                async with session.post(RPC_URL, json=payload) as response:
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
async def fetch_wallet_balances(wallets: List[str], bnb_price: Decimal) -> Dict[str, Dict]:
    """批量獲取錢包餘額，使用連接池和錯誤處理"""
    results = {}
    failed_wallets = []
    
    async with RPCClient(RPC_URL) as rpc_client:
        async def process_wallet(wallet: str):
            try:
                bnb_balance = await rpc_client.get_bnb_balance(wallet)
                
                return wallet, {
                    "balance": bnb_balance,
                    "balance_usd": bnb_balance * bnb_price
                }
            except Exception as e:
                print(f"Failed to process wallet {wallet}: {str(e)}")
                failed_wallets.append(wallet)
                return wallet, {
                    "balance": Decimal(0),
                    "balance_usd": Decimal(0)
                }

        # 使用 gather 並行處理所有錢包
        tasks = [process_wallet(wallet) for wallet in wallets]
        results_list = await asyncio.gather(*tasks, return_exceptions=False)
        
        # 整理結果
        results = dict(results_list)
        
        if failed_wallets:
            print(f"\nFailed to fetch balances for {len(failed_wallets)} wallets:")
            for wallet in failed_wallets:
                print(f"- {wallet}")

    return results