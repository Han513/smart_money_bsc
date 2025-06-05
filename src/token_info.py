from concurrent.futures import ThreadPoolExecutor
import requests
import aiohttp
from decimal import Decimal
import time
import os
import json
from dotenv import load_dotenv
from config import DATABASE_URI
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from models import get_token_info_from_db
import redis.asyncio as aioredis

load_dotenv()

BIG_DATA_API = os.getenv('BIG_DATA', 'http://172.25.183.205:8580')

# 新增全域 redis client（與 main.py 保持一致）
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

class TokenInfoCache:
    def __init__(self, cache_duration=300):  # 5分鐘
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_duration = cache_duration

    def get(self, token_address: str):
        token_address = token_address.lower()
        if token_address in self._cache:
            if time.time() - self._cache_timestamps[token_address] < self._cache_duration:
                return self._cache[token_address]
            else:
                del self._cache[token_address]
                del self._cache_timestamps[token_address]
        return None

    def set(self, token_address: str, data: dict):
        token_address = token_address.lower()
        self._cache[token_address] = data
        self._cache_timestamps[token_address] = time.time()

token_info_cache = TokenInfoCache()

# class TokenInfoFetcher:
#     @staticmethod
#     async def get_token_info(token_addresses: List[str], chain_id: int = 9006) -> Dict[str, Dict[str, Any]]:
#         """
#         获取代币信息，支持单个和批量查询
#         :param token_addresses: 代币地址列表，单个代币时传入包含一个地址的列表
#         :param chain_id: 链ID，默认为9006 (BSC)
#         :return: 代币信息字典，key为代币地址，value为代币信息
#         """
#         # 首先从缓存获取数据
#         cached_data, uncached_addresses = token_info_cache.get_batch(token_addresses)
        
#         # 如果所有数据都在缓存中，直接返回
#         if not uncached_addresses:
#             return cached_data

#         try:
#             # 准备请求数据
#             payload = {
#                 "chainId": chain_id,
#                 "tokenAddresses": uncached_addresses
#             }
            
#             print(f"Sending request to API with payload: {payload}")  # 添加這行來查看請求數據

#             # 发送请求到API
#             async with aiohttp.ClientSession() as session:
#                 async with session.post(f"{BIG_DATA_API}/data/ods_token_list", json=payload) as response:
#                     if response.status == 200:
#                         response_data = await response.json()
                        
#                         # 檢查 response_data 是否為字典類型
#                         if not isinstance(response_data, dict):
#                             print(f"Invalid API response format: {response_data}")
#                             return cached_data
                            
#                         if response_data.get('success'):
#                             token_list = response_data.get('data', [])
#                             if not isinstance(token_list, list):
#                                 print(f"Invalid token list format: {token_list}")
#                                 return cached_data
                                
#                             # 处理API返回的数据
#                             result = {}
#                             for token_info in token_list:
#                                 if not isinstance(token_info, dict):
#                                     print(f"Invalid token info format: {token_info}")
#                                     continue
                                    
#                                 addr = token_info.get('address', '').lower()
#                                 if not addr:
#                                     continue

#                                 # 处理风险信息
#                                 risk_items = []
#                                 try:
#                                     if token_info.get("riskItem"):
#                                         risk_items = json.loads(token_info["riskItem"])
#                                 except:
#                                     pass

#                                 # 处理社交信息
#                                 social_info = {}
#                                 try:
#                                     if token_info.get("socialInfo"):
#                                         social_info = json.loads(token_info["socialInfo"])
#                                 except:
#                                     pass

#                                 # 处理交易对信息
#                                 pairs = {}
#                                 try:
#                                     if token_info.get("pairs"):
#                                         pairs = json.loads(token_info["pairs"])
#                                 except:
#                                     pass

#                                 # 确保所有数值都是字符串
#                                 processed_info = {
#                                     "id": str(token_info.get("id", "")),
#                                     "network": str(token_info.get("network", "")),
#                                     "address": str(token_info.get("address", "")),
#                                     "symbol": str(token_info.get("symbol", "Unknown")),
#                                     "name": str(token_info.get("name", "")),
#                                     "logo": str(token_info.get("logo", "")),
#                                     "totalSupply": str(token_info.get("totalSupply", "0")),
#                                     "priceUsd": str(token_info.get("priceUsd", "0")),
#                                     "fdvUsd": str(token_info.get("fdvUsd", "0")),
#                                     "totalReserveInUsd": str(token_info.get("totalReserveInUsd", "0")),
#                                     "volumeUsdH24": str(token_info.get("volumeUsdH24", "0")),
#                                     "topPools": str(token_info.get("topPools", "")),
#                                     "decimals": str(token_info.get("decimals", "18")),
#                                     "description": str(token_info.get("description", "")),
#                                     "gtScore": str(token_info.get("gtScore", "0")),
#                                     "socialInfo": social_info,
#                                     "riskItem": risk_items,
#                                     "riskType": str(token_info.get("riskType", "0")),
#                                     "buyTax": str(token_info.get("buyTax", "0")),
#                                     "sellTax": str(token_info.get("sellTax", "0")),
#                                     "marketShow": bool(token_info.get("marketShow", True)),
#                                     "enable": bool(token_info.get("enable", True)),
#                                     "supportCount": str(token_info.get("supportCount", "0")),
#                                     "unsupportCount": str(token_info.get("unsupportCount", "0")),
#                                     "tokenType": str(token_info.get("tokenType", "")),
#                                     "updateTime": str(token_info.get("updateTime", "")),
#                                     "createTime": str(token_info.get("createTime", "")),
#                                     "brand": str(token_info.get("brand", "")),
#                                     "header": str(token_info.get("header", "")),
#                                     "goldLabel": bool(token_info.get("goldLabel", False)),
#                                     "logoAudit": str(token_info.get("logoAudit", "0")),
#                                     "baseTop10Percent": str(token_info.get("baseTop10Percent", "0")),
#                                     "baseHolders": str(token_info.get("baseHolders", "0")),
#                                     "pairs": pairs,
#                                     "chainId": str(token_info.get("chainId", "9006"))
#                                 }
#                                 result[addr] = processed_info
                                
#                                 # 存入缓存
#                                 token_info_cache.set(addr, processed_info)
                            
#                             # 合并缓存数据和API返回的数据
#                             result.update(cached_data)
#                             return result
#                         else:
#                             print(f"API request failed: {response_data}")
#                             return cached_data  # 如果API请求失败，返回缓存的数据
#                     else:
#                         print(f"Error fetching token info: {response.status}")
#                         return cached_data  # 如果API请求失败，至少返回缓存的数据
                        
#         except Exception as e:
#             print(f"Error fetching token info: {e}")
#             return cached_data  # 发生错误时返回缓存的数据

class TokenInfoFetcher:
    @staticmethod
    async def get_token_info(token_address: str, chain_id: int = 9006) -> dict:
        token_address = token_address.lower()
        redis_key = f"token_info:{token_address}"
        # 先查 redis
        try:
            cached_json = await redis_client.get(redis_key)
            if cached_json:
                return json.loads(cached_json)
        except Exception as e:
            # redis 掛掉 fallback 本地 cache
            pass
        # 查本地 cache
        cached = token_info_cache.get(token_address)
        if cached:
            return cached
        # 查資料庫
        engine = create_async_engine(DATABASE_URI, echo=False)
        async with AsyncSession(engine) as session:
            info = await get_token_info_from_db(token_address, chain_id, session)
            if info:
                token_info_cache.set(token_address, info)
                try:
                    await redis_client.set(redis_key, json.dumps(info), ex=3600)
                except Exception as e:
                    pass
            return info or {}