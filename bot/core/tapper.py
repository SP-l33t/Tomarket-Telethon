import aiohttp
import asyncio
import functools
import fasteners
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
from telethon.types import InputUser, InputBotAppShortName, InputPeerUser
from telethon.functions import messages, contacts
from tzlocal import get_localzone

from .agents import generate_random_user_agent
from bot.config import settings
from typing import Callable
from bot.utils import logger, log_error, proxy_utils, config_utils, CONFIG_PATH, SESSIONS_PATH
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
        self.lock = fasteners.InterProcessLock(os.path.join(SESSIONS_PATH, f"{self.session_name}.lock"))
        self.headers = headers
        self.headers['User-Agent'] = self.check_user_agent()
        self.headers.update(**get_sec_ch_ua(self.headers.get('User-Agent', '')))

    def log_message(self, message) -> str:
        return f"<light-yellow>{self.session_name}</light-yellow> | {message}"

    def check_user_agent(self):
        user_agent = self.config.get('user_agent')
        if not user_agent:
            user_agent = generate_random_user_agent()
            self.config['user_agent'] = user_agent
            config_utils.update_session_config_in_file(self.session_name, self.config, CONFIG_PATH)

        return user_agent

    async def get_tg_web_data(self) -> [str | None, str | None]:
        
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = proxy_utils.to_telethon_proxy(proxy)
        else:
            proxy_dict = None

        self.tg_client.set_proxy(proxy_dict)

        try:
            if not self.tg_client.is_connected():
                try:
                    self.lock.acquire()
                    await self.tg_client.start()
                except (UnauthorizedError, AuthKeyUnregisteredError):
                    raise InvalidSession(self.session_name)
                except (UserDeactivatedError, UserDeactivatedBanError, PhoneNumberBannedError):
                    raise InvalidSession(f"{self.session_name}: User is banned")

            while True:
                try:
                    resolve_result = await self.tg_client(contacts.ResolveUsernameRequest(username='Tomarket_ai_bot'))
                    peer = InputPeerUser(user_id=resolve_result.peer.user_id,
                                         access_hash=resolve_result.users[0].access_hash)
                    break
                except FloodWaitError as fl:
                    fls = fl.seconds

                    logger.warning(self.log_message(f"FloodWait {fl}"))
                    logger.info(self.log_message(f"Sleep {fls}s"))
                    await asyncio.sleep(fls + 3)
            
            ref_id = settings.REF_ID if random.randint(0, 100) <= 85 else "0000GbQY"

            input_user = InputUser(user_id=resolve_result.peer.user_id, access_hash=resolve_result.users[0].access_hash)
            input_bot_app = InputBotAppShortName(bot_id=input_user, short_name="app")

            web_view = await self.tg_client(messages.RequestAppWebViewRequest(
                peer=peer,
                app=input_bot_app,
                platform='android',
                write_allowed=True,
                start_param=ref_id
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))
            tg_web_data_parts = tg_web_data.split('&')

            user_data = quote(tg_web_data_parts[0].split('=')[1])
            chat_instance = tg_web_data_parts[1].split('=')[1]
            chat_type = tg_web_data_parts[2].split('=')[1]
            auth_date = tg_web_data_parts[4].split('=')[1]
            hash_value = tg_web_data_parts[5].split('=')[1]

            init_data = (f"user={user_data}&chat_instance={chat_instance}&chat_type={chat_type}&start_param={ref_id}&auth_date={auth_date}&hash={hash_value}")
            
            if self.tg_client.is_connected():
                await self.tg_client.disconnect()
                if self.lock.acquired:
                    self.lock.release()

            return ref_id, init_data

        except Exception as error:
            log_error(self.log_message(f"Unknown error: {error}"))
            await asyncio.sleep(delay=3)
            return None, None

    @error_handler
    async def make_request(self, http_client: CloudflareScraper, method, endpoint=None, url=None, **kwargs):
        full_url = url or f"https://api-web.tomarket.ai/tomarket-game/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)

        return await response.json()
        
    @error_handler
    async def login(self, http_client, tg_web_data: str, ref_id: str) -> tuple[str, str]:
        response = await self.make_request(http_client, "POST", "/user/login", json={"init_data": tg_web_data, "invite_code": ref_id})
        return response.get('data', {}).get('access_token', None)

    async def check_proxy(self, http_client: aiohttp.ClientSession, proxy: str) -> bool:
        try:
            response = await http_client.get(url='https://httpbin.org/ip', timeout=aiohttp.ClientTimeout(10))
            ip = (await response.json()).get('origin')
            logger.info(self.log_message(f"Proxy IP: {ip}"))
            return True
        except Exception as error:
            log_error(self.log_message(f"Proxy: {proxy} | Error: {error}"))
            return False

    @error_handler
    async def get_balance(self, http_client):
        return await self.make_request(http_client, "POST", "/user/balance")

    @error_handler
    async def claim_daily(self, http_client):
        return await self.make_request(http_client, "POST", "/daily/claim", json={"game_id": "fa873d13-d831-4d6f-8aee-9cff7a1d0db1"})

    @error_handler
    async def start_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/start", json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def claim_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/claim", json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def play_game(self, http_client):
        return await self.make_request(http_client, "POST", "/game/play", json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d"})

    @error_handler
    async def claim_game(self, http_client, points=None):
        return await self.make_request(http_client, "POST", "/game/claim", json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d", "points": points})

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
        evalute = await self.make_request(http_client, "POST", "/rank/evalute")
        if evalute and evalute.get('status', 200) != 404:
            await self.make_request(http_client, "POST", "/rank/create")
            return True
        return False

    async def run(self) -> None:

        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(self.log_message(f"Bot will start in <light-red>{random_delay}s</light-red>"))
            await asyncio.sleep(delay=random_delay)
        
        proxy_conn = None
        if self.proxy:
            proxy_conn = ProxyConnector().from_url(self.proxy)
            http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
            p_type = proxy_conn._proxy_type
            p_host = proxy_conn._proxy_host
            p_port = proxy_conn._proxy_port
            if not await self.check_proxy(http_client=http_client, proxy=f"{p_type}://{p_host}:{p_port}"):
                return
        else:
            http_client = CloudflareScraper(headers=self.headers)

        # ``
        # Наши переменные
        # ``
        end_farming_dt = 0
        tickets = 0
        next_stars_check = 0
        next_combo_check = 0
        while True:
            ref_id, init_data = await self.get_tg_web_data()

            if not init_data:
                if not http_client.closed:
                    await http_client.close()
                if proxy_conn and not proxy_conn.closed:
                    proxy_conn.close()
                return

            try:
                if http_client.closed:
                    if proxy_conn and not proxy_conn.closed:
                        proxy_conn.close()

                    proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
                    http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
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

                balance = await self.get_balance(http_client=http_client)
                available_balance = balance['data']['available_balance']
                logger.info(self.log_message(f"Current balance: <light-red>{available_balance}</light-red>"))

                if 'farming' in balance['data']:
                    end_farm_time = balance['data']['farming']['end_at']
                    if end_farm_time > time():
                        end_farming_dt = end_farm_time + 240
                        logger.info(self.log_message(f"Farming in progress, next claim in <light-red>{round((end_farming_dt - time()) / 60)}m.</light-red>"))

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

                            elif data_stars.get('status') < 3 and datetime.fromisoformat(data_stars.get('endTime')) > datetime.now():
                                start_stars_claim = await self.start_stars_claim(http_client=http_client, data={'task_id': data_stars.get('taskId')})
                                claim_stars = await self.claim_task(http_client=http_client, data={'task_id': data_stars.get('taskId')})
                                if claim_stars is not None and claim_stars.get('status') == 0 and start_stars_claim is not None and start_stars_claim.get('status') == 0:
                                    logger.info(self.log_message(f"Claimed stars | Stars: <light-red>+{start_stars_claim['data'].get('stars', 0)}</light-red>"))
                            
                            next_stars_check = int(datetime.fromisoformat(get_stars['data'].get('endTime')).timestamp())

                await asyncio.sleep(1.5)

                if settings.AUTO_CLAIM_COMBO and next_combo_check < time():
                    combo_info = await self.get_combo(http_client)
                    combo_info_data = combo_info.get('data', [])[0] if combo_info.get('data') else []

                    if combo_info and combo_info.get('status') == 0 and combo_info_data:
                        if combo_info_data.get('status') > 0:
                            logger.info(self.log_message(f"Combo already claimed | Skipping...."))
                        elif combo_info_data.get('status') == 0 and datetime.fromisoformat(
                                combo_info_data.get('end')) > datetime.now():
                            claim_combo = await self.claim_task(http_client, data = { 'task_id': combo_info_data.get('taskId') })

                            if claim_combo is not None and claim_combo.get('status') == 0:
                                logger.info(self.log_message(
                                    f"Claimed combo | Points: <light-red>+{combo_info_data.get('score')}</light-red> | Combo code: <light-red>{combo_info_data.get('code')}</light-red>"))
                        
                        next_combo_check = int(datetime.fromisoformat(combo_info_data.get('end')).timestamp())

                await asyncio.sleep(1.5)

                if settings.AUTO_DAILY_REWARD:
                    claim_daily = await self.claim_daily(http_client=http_client)
                    if claim_daily and 'status' in claim_daily and claim_daily.get("status", 400) != 400:
                        logger.info(self.log_message(f"Daily: <light-red>{claim_daily['data']['today_game']}</light-red> reward: <light-red>{claim_daily['data']['today_points']}</light-red>"))

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
                                    claim_game = await self.claim_game(http_client=http_client, points=random.randint(settings.POINTS_COUNT[0], settings.POINTS_COUNT[1]))
                                    if claim_game and 'status' in claim_game:
                                        if claim_game['status'] == 500 and claim_game['message'] == 'game not start':
                                            continue
                                        
                                        if claim_game.get('status') == 0:
                                            tickets -= 1
                                            games_points += claim_game.get('data').get('points')
                                            await asyncio.sleep(1.5)
                        logger.info(self.log_message(f"Games finish! Claimed points: <light-red>{games_points}</light-red>"))

                if settings.AUTO_TASK:
                    logger.info(self.log_message(f"Start checking tasks."))
                    tasks = await self.get_tasks(http_client=http_client, data={'language_code': 'en', 'init_data': init_data})
                    tasks_list = []
                    if tasks and tasks.get("status", 500) == 0:
                        for category, task_group in tasks["data"].items():
                            for task in task_group:
                                if task.get('enable') and not task.get('invisible', False):
                                    if task.get('startTime') and task.get('endTime'):
                                        task_start = convert_to_local_and_unix(task['startTime'])
                                        task_end = convert_to_local_and_unix(task['endTime'])
                                        if task_start <= time() <= task_end:
                                            tasks_list.append(task)
                                    elif task.get('type') not in ['wallet', 'mysterious', 'classmate', 'classmateInvite', 'classmateInviteBack']:
                                        tasks_list.append(task)
                    
                    for task in tasks_list:
                        wait_second = task.get('waitSecond', 0)
                        starttask = await self.start_task(http_client=http_client, data={'task_id': task['taskId'], 'init_data': init_data})
                        task_data = starttask.get('data', {}) if starttask else None
                        task_started = task_data == 'ok' or task_data.get('status') == 1 if task_data else False
                        if task_started:
                            logger.info(self.log_message(f"Start task <light-red>{task['name']}.</light-red> Wait {wait_second}s 🍅"))
                            await asyncio.sleep(wait_second + 3)
                            await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                            await asyncio.sleep(3)
                            claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                            if claim:
                                if claim['status'] == 0:
                                    reward = task.get('score', 'unknown')
                                    logger.info(self.log_message(f"Task <light-red>{task['name']}</light-red> claimed! Reward: {reward} 🍅"))
                                else:
                                    logger.info(self.log_message(f"Task <light-red>{task['name']}</light-red> not claimed. Reason: {claim.get('message', 'Unknown error')} 🍅"))
                            await asyncio.sleep(2)

                await asyncio.sleep(1.5)

                if await self.create_rank(http_client=http_client):
                    logger.info(self.log_message(f"Rank created! 🍅"))

                if settings.AUTO_RANK_UPGRADE:
                    rank_data = await self.get_rank_data(http_client, {'task_id': task['taskId'], 'init_data': init_data})
                    unused_stars = rank_data.get('data', {}).get('unusedStars', 0)
                    logger.info(self.log_message(f"Unused stars {unused_stars}"))
                    if unused_stars > 0:
                        upgrade_rank = await self.upgrade_rank(http_client=http_client, stars=unused_stars)
                        if upgrade_rank.get('status', 500) == 0:
                            logger.info(self.log_message(f"Rank upgraded! 🍅"))
                        else:
                            logger.info(self.log_message(
                                f"Rank not upgraded. Reason: {upgrade_rank.get('message', 'Unknown error')} 🍅"))

                sleep_time = end_farming_dt - time()
                logger.info(self.log_message(f'Sleep <light-red>{round(sleep_time / 60, 2)}m.</light-red>'))
                await asyncio.sleep(sleep_time)
                await http_client.close()
                if proxy_conn:
                    if not proxy_conn.closed:
                        proxy_conn.close()
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
    finally:
        if runner.lock.acquired:
            runner.lock.release()
