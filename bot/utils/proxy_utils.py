import os
import aiohttp
from aiohttp_proxy import ProxyConnector
from python_socks import ProxyType
from shutil import copyfile
from better_proxy import Proxy
from bot.config import settings
from bot.utils import logger


PROXY_TYPES = {
    'socks5': ProxyType.SOCKS5,
    'socks4': ProxyType.SOCKS4,
    'http': ProxyType.HTTP,
    'https': ProxyType.HTTP
}


def get_proxy_type(proxy_type: str):
    return PROXY_TYPES.get(proxy_type.lower())


def to_telethon_proxy(proxy: Proxy):
    return {
        'proxy_type': get_proxy_type(proxy.protocol),
        'addr': proxy.host,
        'port': proxy.port,
        'username': proxy.login,
        'password': proxy.password
    }


def get_proxies(proxy_path: str = "bot/config/proxies.txt") -> list[str]:
    """Reads proxoies from the proxy file and returns array of proxies.
    If file doens't exist, creates the file

     Args:
       proxy_path: Path to the proxies.txt file.

     Returns:
       The contents of the file, or an empty list if the file was empty or created.
     """
    proxy_template_path = "bot/config/proxies-template.txt"

    if not os.path.isfile(proxy_path):
        copyfile(proxy_template_path, proxy_path)
        return []

    if settings.USE_PROXY_FROM_FILE:
        with open(file=proxy_path, encoding="utf-8-sig") as file:
            return [Proxy.from_str(proxy=row.strip()).as_url
                    for row in file
                    if row.strip() and not row.strip().startswith('type')]
    else:
        return []


def get_unused_proxies(accounts_config):
    used_proxies = list({v['proxy'] for v in accounts_config.values()})
    all_proxies = get_proxies()
    return [proxy for proxy in all_proxies if proxy not in used_proxies]


async def check_proxy(proxy):
    url = 'https://ifconfig.me/ip'
    proxy_conn = ProxyConnector().from_url(proxy)
    try:
        async with aiohttp.ClientSession(connector=proxy_conn) as session:
            response = await session.get(url)
            if response.status == 200:
                return True
    except Exception as e:
        logger.warning(f"Proxy {proxy} didn't respond")
        return False
