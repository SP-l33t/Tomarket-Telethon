import glob
import asyncio
import argparse
import os

from telethon import TelegramClient

from bot.config import settings
from bot.utils import logger, config_utils, proxy_utils
from bot.core.tapper import run_tapper
from bot.core.registrator import register_sessions


start_text = """
Select an action:

    1. Run clicker
    2. Create session
"""


def get_session_names() -> list[str]:
    session_names = sorted(glob.glob("sessions/*.session"))
    return [os.path.splitext(os.path.basename(file))[0] for file in session_names]


async def get_tg_clients() -> list[TelegramClient]:
    API_ID = settings.API_ID
    API_HASH = settings.API_HASH

    accounts_config = config_utils.read_config_file()
    session_names = get_session_names()

    if not session_names:
        raise FileNotFoundError("Not found session files")

    if not API_ID or not API_HASH:
        raise ValueError("API_ID and API_HASH not found in the .env file.")

    tg_clients = []
    for session_name in session_names:
        config: dict = accounts_config.get(session_name, {})
        if config.get('api_id') and config.get('api_hash'):
            client_params = {
                "session": f"sessions/{session_name}",
                "lang_code": "en",
                "system_lang_code": "en-US"
            }
            for key in ("api_id", "api_hash", "device_model", "system_version", "app_version"):
                if key in config and config[key]:
                    client_params[key] = config[key]

            tg_clients.append(TelegramClient(**client_params))
        else:
            unused_proxies = proxy_utils.get_unused_proxies(accounts_config)
            if not unused_proxies and settings.USE_PROXY_FROM_FILE:
                print(f'No proxy found for session: {session_name}. Skipping')
                continue
            else:
                proxy = unused_proxies[0] if unused_proxies else None

            tg_clients.append(TelegramClient(
                session=f"sessions/{session_name}",
                api_id=API_ID,
                api_hash=API_HASH,
            ))
            accounts_config.update({
                session_name:
                    {
                        'api_id': API_ID,
                        'api_hash': API_HASH,
                        'proxy': proxy
                    }
            })
            config_utils.write_config_file(accounts_config)
    return tg_clients


async def process() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--action", type=int, help="Action to perform")

    logger.info(f"Detected {len(get_session_names())} sessions | {len(proxy_utils.get_proxies())} proxies")

    action = parser.parse_args().action

    if not action:
        logger.info(start_text)

        while True:
            action = input("> ").strip()

            if not action.isdigit():
                logger.warning("Action must be number")
            elif action not in ["1", "2"]:
                logger.warning("Action must be 1 or 2")
            else:
                action = int(action)
                break

    if action == 1:
        await run_tasks()

    elif action == 2:
        await register_sessions()


async def run_tasks():
    tg_clients = await get_tg_clients()

    tasks = [
        asyncio.create_task(
            run_tapper(
                tg_client=tg_client
            )
        )
        for tg_client in tg_clients
    ]

    await asyncio.gather(*tasks)
