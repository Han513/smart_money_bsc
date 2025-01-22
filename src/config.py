import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置s
# DATABASE_URI_SWAP_SOL = os.getenv('DATABASE_URI_SWAP_SOL')
# DATABASE_URI_SWAP_ETH = os.getenv('DATABASE_URI_SWAP_ETH')
# DATABASE_URI_SWAP_BASE = os.getenv('DATABASE_URI_SWAP_BASE')
DATABASE_URI_SWAP_BSC = os.getenv('DATABASE_URI_SWAP_BSC')
DATABASE_URI = os.getenv('DATABASE_URI')
# DATABASE_URI_SWAP_TRON = os.getenv('DATABASE_URI_SWAP_TRON')

RPC_URL = os.getenv('RPC_URL', "https://solana-mainnet.rpc.url")
HELIUS_API_KEY = "16e9dd4d-4cf7-4c69-8c2d-fafa13b03423"