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

# BSC RPC URL
RPC_URL = os.getenv('RPC_URL', "https://frosty-wild-knowledge.bsc.quiknode.pro/9a7d28ef93c08d9afdfd5fbb1cc51d6998deed04")
HELIUS_API_KEY = "16e9dd4d-4cf7-4c69-8c2d-fafa13b03423"

# Backend API 配置
BACKEND_HOST = os.getenv('BACKEND_HOST', 'localhost')
BACKEND_PORT = os.getenv('BACKEND_PORT', '4200')

# Redis 配置
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_POOL_SIZE = int(os.getenv('REDIS_POOL_SIZE', 10))

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'trade_events')