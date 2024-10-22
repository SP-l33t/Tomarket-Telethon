import aiohttp
import asyncio
import functools
import json
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from datetime import datetime
from time import time
from pytz import UTC
from random import randint, uniform
from urllib.parse import unquote, parse_qs

from bot.utils.universal_telegram_client import UniversalTelegramClient

from bot.config import settings
from typing import Callable
from bot.utils import logger, log_error, config_utils, CONFIG_PATH, first_run
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
    dt = UTC.localize(dt)
    unix_time = int(dt.timestamp())
    return unix_time


class Tapper:
    def __init__(self, tg_client: UniversalTelegramClient):
        self.tg_client = tg_client
        self.session_name = tg_client.session_name

        session_config = config_utils.get_session_config(self.session_name, CONFIG_PATH)

        if not all(key in session_config for key in ('api', 'user_agent')):
            logger.critical(self.log_message('CHECK accounts_config.json as it might be corrupted'))
            exit(-1)

        self.headers = headers
        user_agent = session_config.get('user_agent')
        self.headers['user-agent'] = user_agent
        self.headers.update(**get_sec_ch_ua(user_agent))

        self.proxy = session_config.get('proxy')
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            self.tg_client.set_proxy(proxy)

        self.ref_id = ""
        self.user_data = None
        self.wallet = session_config['ton_address']

        self._webview_data = None

    def log_message(self, message) -> str:
        return f"<ly>{self.session_name}</ly> | {message}"

    async def get_tg_web_data(self) -> str:
        webview_url = await self.tg_client.get_app_webview_url('Tomarket_ai_bot', "app", "0000GbQY")

        tg_web_data = unquote(webview_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0])
        query_params = parse_qs(tg_web_data)
        self.user_data = json.loads(query_params.get('user', [''])[0])
        self.ref_id = query_params.get('start_param', [''])[0]

        return tg_web_data

    async def check_proxy(self, http_client: CloudflareScraper) -> bool:
        proxy_conn = http_client.connector
        if proxy_conn and not hasattr(proxy_conn, '_proxy_host'):
            logger.info(self.log_message(f"Running Proxy-less"))
            return True
        try:
            response = await http_client.get(url='https://ifconfig.me/ip', timeout=aiohttp.ClientTimeout(15))
            logger.info(self.log_message(f"Proxy IP: {await response.text()}"))
            return True
        except Exception as error:
            proxy_url = f"{proxy_conn._proxy_type}://{proxy_conn._proxy_host}:{proxy_conn._proxy_port}"
            log_error(self.log_message(f"Proxy: {proxy_url} | Error: {type(error).__name__}"))
            return False

    @error_handler
    async def make_request(self, http_client: CloudflareScraper, method, endpoint=None, url=None, **kwargs):
        full_url = url or f"https://api-web.tomarket.ai/tomarket-game/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)

        return await response.json()

    @error_handler
    async def login(self, http_client, tg_web_data: str) -> tuple[str, str]:
        response = await self.make_request(http_client, "POST", "/user/login",
                                           json={"init_data": tg_web_data, "invite_code": self.ref_id})
        return response.get('data', {}).get('access_token', None)

    @error_handler
    async def get_balance(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/user/balance")

    @error_handler
    async def claim_daily(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/daily/claim",
                                       json={"game_id": "fa873d13-d831-4d6f-8aee-9cff7a1d0db1"})

    @error_handler
    async def start_farming(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/farm/start",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def claim_farming(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/farm/claim",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    @error_handler
    async def play_game(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/game/play",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d"})

    @error_handler
    async def claim_game(self, http_client: CloudflareScraper, points=None):
        return await self.make_request(http_client, "POST", "/game/claim",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d", "points": points})

    @error_handler
    async def start_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/start", json=data)

    @error_handler
    async def check_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/check", json=data)

    @error_handler
    async def claim_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/claim", json=data)

    @error_handler
    async def get_stars(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/tasks/classmateTask")

    @error_handler
    async def start_stars_claim(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/classmateStars", json=data)

    @error_handler
    async def get_tasks(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/list", json=data)

    @error_handler
    async def get_spin_tickets(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/user/tickets", json=data)

    @error_handler
    async def get_spin_assets(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/spin/assets", json=data)

    @error_handler
    async def get_spin_free(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/spin/free", json=data)

    @error_handler
    async def spin_once(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", "/spin/once")

    @error_handler
    async def spin_raffle(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/spin/raffle", json=data)

    @error_handler
    async def get_rank_data(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/rank/data", json=data)

    @error_handler
    async def upgrade_rank(self, http_client: CloudflareScraper, stars: int):
        return await self.make_request(http_client, "POST", "/rank/upgrade", json={'stars': stars})

    @error_handler
    async def create_rank(self, http_client: CloudflareScraper):
        evaluate = await self.make_request(http_client, "POST", "/rank/evaluate")
        if evaluate and evaluate.get('status', 200) != 404:
            create_rank_resp = await self.make_request(http_client, "POST", "/rank/create")
            if create_rank_resp.get('data', {}).get('isCreated', False) is True:
                return True
        return False

    async def wallet_task(self, http_client: CloudflareScraper):
        resp = await self.make_request(http_client, "POST", "/tasks/walletTask")
        return resp.get("data", {}).get("walletAddress", "")

    async def link_wallet(self, http_client: CloudflareScraper):
        payload = {"wallet_address": self.wallet}
        return await self.make_request(http_client, "POST", "/tasks/address", json=payload)

    async def get_puzzle_status(self, http_client: CloudflareScraper, data):
        resp = await self.make_request(http_client, "POST", "/tasks/puzzle", json=data)
        resp_data = resp.get('data', [{}])[0]
        return resp_data.get("taskId") if resp_data.get("status") != 3 else None

    async def puzzle_claim(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", "/tasks/puzzleClaim", json=data)

    @staticmethod
    async def get_combo():
        async with aiohttp.request(method="GET", url="https://raw.githubusercontent.com/zuydd/database/refs/heads/main/tomarket.json") as resp:
            if resp.status in range(200, 300):
                return json.loads(await resp.text()).get('puzzle', {})

    async def add_tomato_to_first_name(self):
        if '🍅' not in self.user_data.get('first_name'):
            await self.tg_client.update_profile(first_name=f"{self.user_data.get('first_name')} 🍅")

    async def run(self) -> None:

        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = uniform(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(self.log_message(f"Bot will start in <light-red>{int(random_delay)}s</light-red>"))
            await asyncio.sleep(delay=random_delay)

        access_token_created_time = 0
        init_data = None

        token_live_time = randint(3500, 3600)

        end_farming_dt = 0
        tickets = 0
        next_stars_check = 0

        proxy_conn = {'connector': ProxyConnector.from_url(self.proxy)} if self.proxy else {}
        async with CloudflareScraper(headers=self.headers, timeout=aiohttp.ClientTimeout(60), **proxy_conn) as http_client:
            while True:
                if not await self.check_proxy(http_client=http_client):
                    logger.warning(self.log_message('Failed to connect to proxy server. Sleep 5 minutes.'))
                    await asyncio.sleep(300)
                    continue

                try:
                    if time() - access_token_created_time >= token_live_time:
                        init_data = await self.get_tg_web_data()

                        if not init_data:
                            logger.warning(self.log_message('Failed to get webview URL'))
                            await asyncio.sleep(300)
                            continue

                    access_token = await self.login(http_client=http_client, tg_web_data=init_data)
                    if not access_token:
                        logger.warning(self.log_message(f"Failed login"))
                        logger.info(self.log_message(f"Sleep <light-red>300s</light-red>"))
                        await asyncio.sleep(delay=300)
                        continue
                    else:
                        if self.tg_client.is_fist_run:
                            await first_run.append_recurring_session(self.session_name)
                        logger.info(self.log_message(f"<light-red>🍅 Login successful</light-red>"))
                        http_client.headers["Authorization"] = f"{access_token}"
                    await asyncio.sleep(delay=1)

                    access_token_created_time = time()

                    balance = await self.get_balance(http_client=http_client)
                    rank_data = await self.get_rank_data(http_client,
                                                         {'language_code': 'en', 'init_data': init_data})
                    available_balance = balance['data']['available_balance']
                    current_rank = rank_data.get('data', {}).get('currentRank', {}).get('name')
                    logger.info(self.log_message(f"Current balance: <light-red>{available_balance}</light-red> | "
                                                 f"Current Rank: <light-red>{current_rank}</light-red>"))

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

                    await asyncio.sleep(uniform(2, 4))

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
                                                                           points=randint(
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
                        current_time = time()
                        tasks_list = []
                        excluded_types = ['mysterious', 'classmate', 'classmateInvite', 'classmateInviteBack',
                                          'charge_stars_season2', 'invite_star_group', 'chain_donate_free', 'new_package']
                        excluded_names = ['Buy Tomatos']
                        if tasks and tasks.get("status", 500) == 0:
                            for category, task_group in tasks.get("data", {}).items():
                                task_list = task_group if isinstance(task_group, list) else task_group.get("default", [])
                                logger.info(self.log_message(
                                    f"Checking tasks: <r>{category}</r> ({len(task_list)} tasks)"))
                                for task in task_list:
                                    if (task.get('enable') and not task.get('invisible', False) and
                                            task.get('type', '').lower() not in excluded_types and
                                            task.get('name') not in excluded_names):
                                        if task.get('type') == 'free_tomato' and task.get('status') != 3:
                                            task_start = convert_to_local_and_unix(task['startTime']) - 86400
                                            task_end = convert_to_local_and_unix(task['endTime']) - 86400
                                            if task_start <= current_time <= task_end:
                                                tasks_list.append(task)
                                            continue
                                        if task.get('startTime') and task.get('endTime') and task.get('status') != 3:
                                            task_start = convert_to_local_and_unix(task['startTime'])
                                            task_end = convert_to_local_and_unix(task['endTime'])
                                            if task_start <= current_time <= task_end:
                                                tasks_list.append(task)
                                        elif task.get('status') != 3:
                                            tasks_list.append(task)

                        for task in tasks_list:
                            wait_second = task.get('waitSecond', 0)
                            starttask = await self.start_task(http_client=http_client, data={'task_id': task['taskId'],
                                                                                             'init_data': init_data})
                            task_data = starttask.get('data', {}) if starttask else None
                            if task_data == 'ok' or task_data.get('status') in [1, 2]:
                                logger.info(self.log_message(
                                    f"Start task <light-red>{task['name']}.</light-red> Wait {wait_second}s 🍅"))
                                if task.get('type') == 'wallet':
                                    if not settings.PERFORM_WALLET_TASK:
                                        continue
                                    get_wallet = await self.wallet_task(http_client)
                                    if not get_wallet:
                                        await asyncio.sleep(uniform(10, 20))
                                        wallet = await self.link_wallet(http_client)
                                        if wallet.get("data") != "ok":
                                            continue
                                        if task_data.get('status') == 2:
                                            await asyncio.sleep(wait_second + uniform(3, 5))
                                if task.get('type') == 'emoji':
                                    await self.add_tomato_to_first_name()
                                if task.get('needVerify', False) and (task_data == 'ok' or task_data.get('status') != 2):
                                    await asyncio.sleep(wait_second + uniform(3, 5))
                                    resp = await self.check_task(http_client=http_client,
                                                                 data={'task_id': task['taskId'],
                                                                       'init_data': init_data})
                                    if resp.get('data', {}).get('status') != 2:
                                        continue
                                claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                                if claim:
                                    if claim['status'] == 0:
                                        reward = task.get('score', 'unknown')
                                        logger.success(self.log_message(
                                            f"Task <light-red>{task['name']}</light-red> claimed! Reward: {reward} 🍅"))
                                    else:
                                        logger.warning(self.log_message(
                                            f"Task <light-red>{task['name']}</light-red> not claimed. Reason: "
                                            f"{claim.get('message', 'Unknown error')} 🍅"))
                                await asyncio.sleep(2)

                    await asyncio.sleep(1.5)

                    if settings.AUTO_CLAIM_COMBO:
                        combo_id = await self.get_puzzle_status(http_client,
                                                                data={'language_code': 'en', 'init_data': init_data})
                        combo = await self.get_combo()
                        if combo and combo.get("task_id") == combo_id:
                            await asyncio.sleep(uniform(3, 10))
                            combo['code'] = combo['code'].replace(' ', '')
                            claim_combo = await self.puzzle_claim(http_client, combo)
                            if claim_combo.get("status") == 0 and 'incorrect' not in claim_combo.get('data', {}).get('message', ""):
                                logger.success(self.log_message("Successfully claimed daily puzzle"))

                    await asyncio.sleep(1.5)

                    spin_tickets = await self.get_spin_tickets(http_client=http_client,
                                                               data={'language_code': 'en', 'init_data': init_data})
                    free_spin = await self.get_spin_free(http_client=http_client,
                                                         data={'language_code': 'en', 'init_data': init_data})
                    if free_spin.get('data', {}).get('is_free', False):
                        await self.get_spin_assets(http_client=http_client,
                                                   data={'language_code': 'en', 'init_data': init_data})
                        await asyncio.sleep((uniform(1, 2)))
                        result = await self.spin_once(http_client)
                        await asyncio.sleep((uniform(1, 2)))
                        result_data = result.get('data', {}).get('results', [{}])[0]
                        if result and result_data:
                            logger.success(self.log_message(f'Used free spin. Got <lr>{result_data.get("amount")} '
                                                            f'{result_data.get("type")}</lr>'))
                    if spin_tickets.get('data', {}).get("ticket_spin_1", 0) > 0:
                        logger.info(self.log_message(
                            f"Amount of free spins: <lr>{spin_tickets.get('data', {}).get('ticket_spin_1', 0)}</lr>"))
                    while spin_tickets.get('data', {}).get("ticket_spin_1", 0) > 0:
                        await self.get_spin_assets(http_client=http_client,
                                                   data={'language_code': 'en', 'init_data': init_data})
                        await asyncio.sleep((uniform(1, 2)))
                        result = await self.spin_raffle(http_client, data={"category": "ticket_spin_1"})
                        await asyncio.sleep((uniform(1, 2)))
                        result_data = result.get('data', {}).get('results', [{}])[0]
                        if result and result_data:
                            logger.success(self.log_message(f'Used free spin. Got <lr>{result_data.get("amount")} '
                                                            f'{result_data.get("type")}</lr>'))
                        spin_tickets = await self.get_spin_tickets(http_client=http_client,
                                                                   data={'language_code': 'en', 'init_data': init_data})
                        await asyncio.sleep((uniform(1, 2)))

                    if await self.create_rank(http_client=http_client):
                        logger.info(self.log_message(f"Rank created! 🍅"))

                    if settings.AUTO_RANK_UPGRADE:
                        rank_data = await self.get_rank_data(http_client,
                                                             {'language_code': 'en', 'init_data': init_data})
                        unused_stars = rank_data.get('data', {}).get('unusedStars', 0)
                        logger.info(self.log_message(f"Unused stars {unused_stars}"))
                        if unused_stars > 0:
                            await asyncio.sleep(randint(30, 63))
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


async def run_tapper(tg_client: UniversalTelegramClient):
    runner = Tapper(tg_client=tg_client)
    try:
        await runner.run()
    except InvalidSession as e:
        logger.error(runner.log_message(f"Invalid Session: {e}"))
