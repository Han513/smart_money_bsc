import asyncio
from decimal import Decimal
import aiohttp

# JSON-RPC 配置
BSC_RPC_URL = "https://summer-necessary-diagram.bsc.quiknode.pro/f5a6381fc4834f55d98e9cdef8df8b16b63587bd"

# 查詢 BNB 餘額
async def get_bnb_balance(address):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [address, "latest"],
        "id": 1
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(BSC_RPC_URL, json=payload) as response:
            result = await response.json()
            balance_wei = int(result['result'], 16)  # 转换为整数（单位: wei）
            return Decimal(balance_wei) / Decimal(10 ** 18)  # 转换为 BNB（单位）

# 查詢 BEP20 代幣餘額
async def get_token_balance(address, token_contract):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": token_contract,
            "data": "0x70a08231000000000000000000000000" + address[2:]  # balanceOf(address)
        }, "latest"],
        "id": 1
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(BSC_RPC_URL, json=payload) as response:
            result = await response.json()
            balance_wei = int(result['result'], 16)  # 转换为整数
            return Decimal(balance_wei) / Decimal(10 ** 18)  # 假设代币精度为 18

# 查詢多個錢包餘額
async def fetch_wallet_balances(wallets, token_contracts):
    balances = {}
    for wallet in wallets:
        bnb_balance = await get_bnb_balance(wallet)
        token_balances = {}
        for token_name, token_address in token_contracts.items():
            token_balances[token_name] = await get_token_balance(wallet, token_address)
        balances[wallet] = {
            "bnb": bnb_balance,
            "tokens": token_balances
        }
    return balances

# 測試函數
async def test_fetch_wallet_balances():
    wallets = [
        "0xC3a792AfD57728eDF1453603a67DBFAd45E68e1f"
    ]
    token_contracts = {
        "BUSD": "0x55d398326f99059fF775485246999027B3197955",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"
    }
    balances = await fetch_wallet_balances(wallets, token_contracts)
    print(balances)

# 運行測試
if __name__ == "__main__":
    asyncio.run(test_fetch_wallet_balances())
