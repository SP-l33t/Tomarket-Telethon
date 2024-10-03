import aiohttp
import asyncio
import functools
import os
import random
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from datetime import datetime
from time import time
from urllib.parse import unquote, quote

from telethon import TelegramClient
from telethon.errors import *
from telethon.types import InputBotAppShortName, InputUser
from telethon.functions import messages
from tzlocal import get_localzone

from .agents import generate_random_user_agent
from bot.config import settings
from typing import Callable
from bot.utils import logger, log_error, proxy_utils, config_utils, AsyncInterProcessLock, CONFIG_PATH
from bot.exceptions import InvalidSession
from .headers import headers, get_sec_ch_ua


def error_handler(func: Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            await asyncio.sleep(1)

    return wrapper


def convert_to_local_and_unix(iso_time):
    dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
    local_dt = dt.astimezone(get_localzone())
    unix_time = int(local_dt.timestamp())
    return unix_time


class Tapper:
    def __init__(self, tg_client: TelegramClient):
        self.tg_client = tg_client
        self.session_name, _ = os.path.splitext(os.path.basename(tg_client.session.filename))
        self.config = config_utils.get_session_config(self.session_name, CONFIG_PATH)
        self.proxy = self.config.get('proxy', None)
        self.lock = AsyncInterProcessLock(
            os.path.join(os.path.dirname(CONFIG_PATH), 'lock_files', f"{self.session_name}.lock"))
        self.headers = headers

        session_config = config_utils.get_session_config(self.session_name, CONFIG_PATH)

        if not all(key in session_config for key in ('api_id', 'api_hash', 'user_agent')):
            logger.critical(self.log_message('CHECK accounts_config.json as it might be corrupted'))
            exit(-1)

        user_agent = session_config.get('user_agent')
        self.headers['user-agent'] = user_agent
        self.headers.update(**get_sec_ch_ua(user_agent))

        self.proxy = session_config.get('proxy')
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = proxy_utils.to_telethon_proxy(proxy)
            self.tg_client.set_proxy(proxy_dict)

        self._webview_data = None

    def log_message(self, message) -> str:
        return f"<light-yellow>{self.session_name}</light-yellow> | {message}"

    async def initialize_webview_data(self):
        if not self._webview_data:
            while True:
                try:
                    peer = await self.tg_client.get_input_entity('Tomarket_ai_bot')
                    bot_id = InputUser(user_id=peer.user_id, access_hash=peer.access_hash)
                    input_bot_app = InputBotAppShortName(bot_id=bot_id, short_name="app")
                    self._webview_data = {'peer': peer, 'app': input_bot_app}
                    break
                except FloodWaitError as fl:
                    logger.warning(self.log_message(f"FloodWait {fl}. Waiting {fl.seconds}s"))
                    await asyncio.sleep(fl.seconds + 3)
                except (UnauthorizedError, AuthKeyUnregisteredError):
                    raise InvalidSession(f"{self.session_name}: User is unauthorized")
                except (UserDeactivatedError, UserDeactivatedBanError, PhoneNumberBannedError):
                    raise InvalidSession(f"{self.session_name}: User is banned")

    async def get_tg_web_data(self) -> [str | None, str | None]:
        if self.proxy and not self.tg_client._proxy:
            logger.critical(self.log_message('Proxy found, but not passed to TelegramClient'))
            exit(-1)

        data = None, None
        async with self.lock:
            try:
                if not self.tg_client.is_connected():
                    await self.tg_client.connect()
                await self.initialize_webview_data()
                await asyncio.sleep(random.uniform(1, 2))

                ref_id = settings.REF_ID if random.randint(0, 100) <= 85 else "0000GbQY"

                web_view = await self.tg_client(messages.RequestAppWebViewRequest(
                    **self._webview_data,
                    platform='android',
                    write_allowed=True,
                    start_param=ref_id
                ))

                auth_url = web_view.url
                tg_web_data = unquote(
                    string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))

                user_data = quote(re.findall(r'user=([^&]+)', tg_web_data)[0])
                chat_instance = re.findall(r'chat_instance=([^&]+)', tg_web_data)[0]
                chat_type = re.findall(r'chat_type=([^&]+)', tg_web_data)[0]
                start_param = re.findall(r'start_param=([^&]+)', tg_web_data)[0]
                auth_date = re.findall(r'auth_date=([^&]+)', tg_web_data)[0]
                hash_value = re.findall(r'hash=([^&]+)', tg_web_data)[0]

                init_data = (
                    f"user={user_data}&chat_instance={chat_instance}&chat_type={chat_type}&start_param={ref_id}&auth_date={auth_date}&hash={hash_value}")
                data = ref_id, init_data

            except InvalidSession:
                raise

            except Exception as error:
                log_error(self.log_message(f"Unknown error during Authorization: {type(error).__name__}"))
                await asyncio.sleep(delay=3)

            finally:
                if self.tg_client.is_connected():
                    await self.tg_client.disconnect()
                    await asyncio.sleep(15)

        return data

    @error_handler
    async def make_request(self, http_client: CloudflareScraper, method, endpoint=None, url=None, **kwargs):
        full_url = url or f"https://api-web.tomarket.ai/tomarket-game/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)

        return await response.json()

    @error_handler
    async def login(self, http_client, tg_web_data: str, ref_id: str) -> tuple[str, str]:
        response = await self.make_request(http_client, "POST", "/user/login",
                                           json={"init_data": tg_web_data, "invite_code": ref_id})
        return response.get('data', {}).get('access_token', None)

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        proxy_conn = http_client._connector
        try:
            response = await http_client.get(url='https://ifconfig.me/ip', timeout=aiohttp.ClientTimeout(15))
            logger.info(self.log_message(f"Proxy IP: {await response.text()}"))
            return True
        except Exception as error:
            proxy_url = f"{proxy_conn._proxy_type}://{proxy_conn._proxy_host}:{proxy_conn._proxy_port}"
            log_error(self.log_message(f"Proxy: {proxy_url} | Error: {type(error).__name__}"))
            return False

    @error_handler
    async def get_balance(self, http_client):
        return await self.make_request(http_client, "POST", "/user/balance")

    @error_handler
    async def claim_daily(self, http_client):
        return await self.make_request(http_client, "POST", "/daily/claim",
                                       json={"game_id": "fa873d13-d831-4d6f-8aee-9cff7a1d0db1"})

    @error_handler
    async def start_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/start",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def claim_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/claim",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def play_game(self, http_client):
        return await self.make_request(http_client, "POST", "/game/play",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d"})

    @error_handler
    async def claim_game(self, http_client, points=None):
        return await self.make_request(http_client, "POST", "/game/claim",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d", "points": points})

    @error_handler
    async def start_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/start", json=data)

    @error_handler
    async def check_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/check", json=data)

    @error_handler
    async def claim_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/claim", json=data)

    @error_handler
    async def get_combo(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/hidden")

    @error_handler
    async def get_stars(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/classmateTask")

    @error_handler
    async def start_stars_claim(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/classmateStars", json=data)

    @error_handler
    async def get_tasks(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/list", json=data)

    @error_handler
    async def get_rank_data(self, http_client, data):
        return await self.make_request(http_client, "POST", "/rank/data", json=data)

    @error_handler
    async def upgrade_rank(self, http_client, stars: int):
        return await self.make_request(http_client, "POST", "/rank/upgrade", json={'stars': stars})

    @error_handler
    async def create_rank(self, http_client):
        evaluate = await self.make_request(http_client, "POST", "/rank/evaluate")
        if evaluate and evaluate.get('status', 200) != 404:
            create_rank_resp = await self.make_request(http_client, "POST", "/rank/create")
            if create_rank_resp.get('data', {}).get('isCreated', False) is True:
                return True
        return False

    async def run(self) -> None:

        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.uniform(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(self.log_message(f"Bot will start in <light-red>{int(random_delay)}s</light-red>"))
            await asyncio.sleep(delay=random_delay)

        access_token_created_time = 0
        init_data = None

        token_live_time = random.randint(3500, 3600)

        end_farming_dt = 0
        tickets = 0
        next_stars_check = 0
        next_combo_check = 0

        proxy_conn = {'connector': ProxyConnector.from_url(self.proxy)} if self.proxy else {}
        async with CloudflareScraper(headers=self.headers, timeout=aiohttp.ClientTimeout(60), **proxy_conn) as http_client:
            while True:
                if not await self.check_proxy(http_client=http_client):
                    logger.warning(self.log_message('Failed to connect to proxy server. Sleep 5 minutes.'))
                    await asyncio.sleep(300)
                    continue

                try:
                    if time() - access_token_created_time >= token_live_time:
                        ref_id, init_data = await self.get_tg_web_data()

                        if not init_data:
                            logger.warning(self.log_message('Failed to get webview URL'))
                            await asyncio.sleep(300)
                            continue

                    access_token = await self.login(http_client=http_client, tg_web_data=init_data, ref_id=ref_id)
                    if not access_token:
                        logger.warning(self.log_message(f"Failed login"))
                        logger.info(self.log_message(f"Sleep <light-red>300s</light-red>"))
                        await asyncio.sleep(delay=300)
                        continue
                    else:
                        logger.info(self.log_message(f"<light-red>🍅 Login successful</light-red>"))
                        http_client.headers["Authorization"] = f"{access_token}"
                    await asyncio.sleep(delay=1)

                    access_token_created_time = time()

                    balance = await self.get_balance(http_client=http_client)
                    available_balance = balance['data']['available_balance']
                    logger.info(self.log_message(f"Current balance: <light-red>{available_balance}</light-red>"))

                    if 'farming' in balance['data']:
                        end_farm_time = balance['data']['farming']['end_at']
                        if end_farm_time > time():
                            end_farming_dt = end_farm_time + 240
                            logger.info(self.log_message(
                                f"Farming in progress, next claim in <light-red>{round((end_farming_dt - time()) / 60)}m.</light-red>"))

                    if time() > end_farming_dt:
                        claim_farming = await self.claim_farming(http_client=http_client)
                        if claim_farming and 'status' in claim_farming:
                            start_farming = None
                            if claim_farming.get('status') in [0, 500]:
                                if claim_farming.get('status') == 0:
                                    farm_points = claim_farming['data']['claim_this_time']
                                    logger.info(self.log_message(
                                        f"Success claim farm. Reward: <light-red>{farm_points}</light-red> 🍅"))
                                start_farming = await self.start_farming(http_client=http_client)
                            if start_farming and 'status' in start_farming and start_farming['status'] in [0, 200]:
                                logger.info(self.log_message(f"Farm started.. 🍅"))
                                end_farming_dt = start_farming['data']['end_at'] + 240
                                logger.info(self.log_message(
                                    f"Next farming claim in <light-red>{round((end_farming_dt - time()) / 60)}m.</light-red>"))
                        await asyncio.sleep(1.5)

                    if settings.AUTO_CLAIM_STARS and next_stars_check < time():
                        get_stars = await self.get_stars(http_client)
                        if get_stars:
                            data_stars = get_stars.get('data', {})
                            if get_stars and get_stars.get('status', -1) == 0 and data_stars:

                                if data_stars.get('status') > 2:
                                    logger.info(self.log_message(f"Stars already claimed | Skipping...."))

                                elif data_stars.get('status') < 3 and datetime.fromisoformat(
                                        data_stars.get('endTime')) > datetime.now():
                                    start_stars_claim = await self.start_stars_claim(http_client=http_client, data={
                                        'task_id': data_stars.get('taskId')})
                                    claim_stars = await self.claim_task(http_client=http_client,
                                                                        data={'task_id': data_stars.get('taskId')})
                                    if claim_stars and claim_stars.get('status') == 0 and start_stars_claim and \
                                            start_stars_claim.get('status') == 0:
                                        logger.info(self.log_message(
                                            f"Claimed stars | Stars: <light-red>+{start_stars_claim['data'].get('stars', 0)}</light-red>"))

                                next_stars_check = int(
                                    datetime.fromisoformat(get_stars['data'].get('endTime')).timestamp())

                    await asyncio.sleep(1.5)

                    if settings.AUTO_CLAIM_COMBO and next_combo_check < time():
                        combo_info = await self.get_combo(http_client)
                        combo_info_data = combo_info.get('data', [])[0] if combo_info.get('data') else []

                        if combo_info and combo_info.get('status') == 0 and combo_info_data:
                            if combo_info_data.get('status') > 0:
                                logger.info(self.log_message(f"Combo already claimed | Skipping...."))
                            elif combo_info_data.get('status') == 0 and datetime.fromisoformat(
                                    combo_info_data.get('end')) > datetime.now():
                                claim_combo = await self.claim_task(http_client,
                                                                    data={'task_id': combo_info_data.get('taskId')})

                                if claim_combo is not None and claim_combo.get('status') == 0:
                                    logger.info(self.log_message(
                                        f"Claimed combo | Points: <light-red>+{combo_info_data.get('score')}</light-red> | Combo code: <light-red>{combo_info_data.get('code')}</light-red>"))

                            next_combo_check = int(datetime.fromisoformat(combo_info_data.get('end')).timestamp())

                    await asyncio.sleep(1.5)

                    if settings.AUTO_DAILY_REWARD:
                        claim_daily = await self.claim_daily(http_client=http_client)
                        if claim_daily and 'status' in claim_daily and claim_daily.get("status", 400) != 400:
                            logger.info(self.log_message(
                                f"Daily: <light-red>{claim_daily['data']['today_game']}</light-red> reward: <light-red>{claim_daily['data']['today_points']}</light-red>"))

                    await asyncio.sleep(1.5)

                    if settings.AUTO_PLAY_GAME:
                        tickets = balance.get('data', {}).get('play_passes', 0)

                        logger.info(self.log_message(f"Tickets: <light-red>{tickets}</light-red>"))

                        await asyncio.sleep(1.5)
                        if tickets > 0:
                            logger.info(self.log_message(f"Start ticket games..."))
                            games_points = 0
                            while tickets > 0:
                                play_game = await self.play_game(http_client=http_client)
                                if play_game and 'status' in play_game:
                                    if play_game.get('status') == 0:
                                        await asyncio.sleep(30)
                                        claim_game = await self.claim_game(http_client=http_client,
                                                                           points=random.randint(
                                                                               settings.POINTS_COUNT[0],
                                                                               settings.POINTS_COUNT[1]))
                                        if claim_game and 'status' in claim_game:
                                            if claim_game['status'] == 500 and claim_game['message'] == 'game not start':
                                                continue

                                            if claim_game.get('status') == 0:
                                                tickets -= 1
                                                games_points += claim_game.get('data').get('points')
                                                await asyncio.sleep(1.5)
                            logger.info(self.log_message(
                                f"Games finish! Claimed points: <light-red>{games_points}</light-red>"))

                    if settings.AUTO_TASK:
                        logger.info(self.log_message(f"Start checking tasks."))
                        tasks = await self.get_tasks(http_client=http_client,
                                                     data={'language_code': 'en', 'init_data': init_data})
                        tasks_list = []
                        if tasks and tasks.get("status", 500) == 0:
                            for category, task_group in tasks["data"].items():
                                for task in task_group:
                                    # TODO Temporary change to complete free TOMATO task
                                    if task.get('enable') and (
                                            not task.get('invisible', False) or task.get('action') == "free_tomato"):
                                        # ________________________________________
                                        if task.get('action') == "free_tomato" and (task.get('status') == 0 or 2):
                                            if task.get('status') == 2:
                                                claim = await self.claim_task(http_client=http_client,
                                                                              data={'task_id': task['taskId']})
                                                reward = task.get('score', 'unknown')
                                                logger.info(self.log_message(
                                                    f"Task <light-red>{task['name']}</light-red> claimed! Reward: {reward} 🍅"))
                                            else:
                                                starttask = await self.start_task(http_client=http_client,
                                                                                  data={'task_id': task['taskId'],
                                                                                        'init_data': init_data})
                                        # _______________________________________
                                        if task.get('type') in ['wallet', 'mysterious', 'classmate', 'classmateInvite',
                                                                'classmateInviteBack', 'charge_stars_season2']:
                                            continue
                                        if task.get('startTime') and task.get('endTime'):
                                            task_start = convert_to_local_and_unix(task['startTime'])
                                            task_end = convert_to_local_and_unix(task['endTime'])
                                            if task_start <= time() <= task_end:
                                                tasks_list.append(task)
                                                continue
                                        tasks_list.append(task)

                        for task in tasks_list:
                            wait_second = task.get('waitSecond', 0)
                            starttask = await self.start_task(http_client=http_client,
                                                              data={'task_id': task['taskId'], 'init_data': init_data})
                            task_data = starttask.get('data', {}) if starttask else None
                            task_code = starttask.get('code', 0) if starttask else None
                            task_status = task_data.get('status') == 1 if not type(
                                task_data) is str and task_data else False
                            task_started = task_code != 400 and task_data == 'ok' or task_status
                            if task_started:
                                logger.info(self.log_message(
                                    f"Start task <light-red>{task['name']}.</light-red> Wait {wait_second}s 🍅"))
                                await asyncio.sleep(wait_second + 3)
                                await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                                await asyncio.sleep(3)
                                claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                                if claim:
                                    if claim['status'] == 0:
                                        reward = task.get('score', 'unknown')
                                        logger.info(self.log_message(
                                            f"Task <light-red>{task['name']}</light-red> claimed! Reward: {reward} 🍅"))
                                    else:
                                        logger.info(self.log_message(
                                            f"Task <light-red>{task['name']}</light-red> not claimed. Reason: {claim.get('message', 'Unknown error')} 🍅"))
                                await asyncio.sleep(2)

                    await asyncio.sleep(1.5)

                    if await self.create_rank(http_client=http_client):
                        logger.info(self.log_message(f"Rank created! 🍅"))

                    if settings.AUTO_RANK_UPGRADE:
                        rank_data = await self.get_rank_data(http_client,
                                                             {'task_id': task['taskId'], 'init_data': init_data})
                        unused_stars = rank_data.get('data', {}).get('unusedStars', 0)
                        logger.info(self.log_message(f"Unused stars {unused_stars}"))
                        if unused_stars > 0:
                            await asyncio.sleep(random.randint(30, 63))
                            upgrade_rank = await self.upgrade_rank(http_client=http_client, stars=unused_stars)
                            if upgrade_rank.get('status', 500) == 0:
                                logger.info(self.log_message(f"Rank upgraded! 🍅"))
                            else:
                                logger.info(self.log_message(
                                    f"Rank not upgraded. Reason: {upgrade_rank.get('message', 'Unknown error')} 🍅"))

                    sleep_time = end_farming_dt - time()
                    logger.info(self.log_message(f'Sleep <light-red>{round(sleep_time / 60, 2)}m.</light-red>'))
                    await asyncio.sleep(sleep_time)

                except InvalidSession as error:
                    raise error

                except Exception as error:
                    log_error(self.log_message(f"Unknown error: {error}"))
                    await asyncio.sleep(delay=3)
                    logger.info(self.log_message(f'Sleep <light-red>10m.</light-red>'))
                    await asyncio.sleep(600)


async def run_tapper(tg_client: TelegramClient):
    runner = Tapper(tg_client=tg_client)
    try:
        await runner.run()
    except InvalidSession as e:
        logger.error(runner.log_message(f"Invalid Session: {e}"))
