from concurrent.futures import ThreadPoolExecutor
import requests

class TokenInfoFetcher:
    @staticmethod
    def get_token_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    return {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
            return {"priceUsd": 0}
        except Exception as e:
            return {"priceUsd": 0}