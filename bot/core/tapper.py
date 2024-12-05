import aiofiles
import aiohttp
import asyncio
import json
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector, SocksError
from better_proxy import Proxy
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_incrementing, retry_if_exception_type
from time import time
from pytz import UTC
from random import randint, uniform
from urllib.parse import unquote, parse_qs

from bot.utils.universal_telegram_client import UniversalTelegramClient

from bot.config import settings
from bot.utils import logger, log_error, config_utils, CONFIG_PATH, first_run
from bot.exceptions import InvalidSession
from .headers import headers, get_sec_ch_ua

API_ENDPOINT = "https://api-web.tomarket.ai/tomarket-game/v1"
TASK_TYPES_BL = ['mysterious', 'classmate', 'classmateInvite', 'classmateInviteBack', 'charge_stars_season2',
                 'invite_star_group', 'chain_donate_free', 'chain_free', 'new_package', 'medal_donate']
TASK_NAMES_BL = ['Buy Tomatos', "Join Duck's Mini App to Get $DUCK"]


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
        self.counter = 0

    def log_message(self, message) -> str:
        return f"<ly>{self.session_name}</ly> | {message}"

    async def get_tg_web_data(self) -> str:
        webview_url = await self.tg_client.get_app_webview_url('Tomarket_ai_bot', "app", "0000GbQY")

        tg_web_data = unquote(webview_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0])
        query_params = parse_qs(tg_web_data)
        self.user_data = json.loads(query_params.get('user', [''])[0])
        self.ref_id = (query_params.get('start_param', [''])[0]).removeprefix('r-').removesuffix("--p-_treasure")

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

    @retry(stop=stop_after_attempt(4),
           wait=wait_incrementing(1, 4),
           retry=retry_if_exception_type((
                   asyncio.exceptions.TimeoutError,
                   aiohttp.ServerDisconnectedError,
                   aiohttp.ClientProxyConnectionError,
                   SocksError
           )))
    async def make_request(self, http_client: CloudflareScraper, method, url=None, **kwargs):
        response = await http_client.request(method, url, **kwargs)
        if response.status in range(200, 300):
            resp = await response.json() if 'json' in response.content_type else await response.text()
            return resp
        else:
            error_json = await response.json() if 'json' in response.content_type else {}
            error_text = f"Error: {error_json}" if error_json else ""
            if settings.DEBUG_LOGGING:
                logger.warning(self.log_message(
                    f"{method} Request to {url} failed with {response.status} code. {error_text}"))
            return error_json

    async def login(self, http_client: CloudflareScraper, tg_web_data: str) -> tuple[str, str]:
        response = await self.make_request(http_client, "POST", f"{API_ENDPOINT}/user/login",
                                           json={"init_data": tg_web_data,
                                                 "invite_code": self.ref_id,
                                                 "from": "",
                                                 "is_bot": False})
        return response.get('data', {}).get('access_token', None)

    async def get_balance(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/user/balance")

    async def claim_daily(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/daily/claim",
                                       json={"game_id": "fa873d13-d831-4d6f-8aee-9cff7a1d0db1"})

    async def start_farming(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/farm/start",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    async def claim_farming(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/farm/claim",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})

    async def play_game(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/game/play",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d"})

    async def claim_game(self, http_client: CloudflareScraper, points=0, stars=0):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/game/claim",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d",
                                             "points": points,
                                             "stars": stars})

    async def start_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/start", json=data)

    async def check_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/check", json=data)

    async def claim_task(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/claim", json=data)

    async def get_stars(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/classmateTask")

    async def start_stars_claim(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/classmateStars", json=data)

    async def get_tasks(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/list", json=data)

    async def get_spin_tickets(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/user/tickets", json=data)

    async def get_spin_assets(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/spin/assets", json=data)

    async def get_spin_free(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/spin/free", json=data)

    async def spin_once(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/spin/once")

    async def spin_raffle(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/spin/raffle", json=data)

    async def get_rank_data(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/data", json=data)

    async def upgrade_rank(self, http_client: CloudflareScraper, stars: int):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/upgrade", json={'stars': stars})

    async def share_tg(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/sharetg")

    async def create_rank(self, http_client: CloudflareScraper):
        evaluate = await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/evaluate")
        if evaluate and evaluate.get('status', 200) != 404:
            create_rank_resp = await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/create")
            if create_rank_resp.get('data', {}).get('isCreated', False) is True:
                return True
        return False

    async def get_wallet(self, http_client: CloudflareScraper):
        resp = await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/walletTask")
        return resp.get("data", {}).get("walletAddress", "")

    async def link_wallet(self, http_client: CloudflareScraper):
        payload = {"wallet_address": self.wallet}
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/address", json=payload)

    async def delete_wallet(self, http_client: CloudflareScraper):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/deleteAddress")

    async def get_puzzle_status(self, http_client: CloudflareScraper, data):
        resp = await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/puzzle", json=data)
        resp_data = resp.get('data', [{}])[0]
        return resp_data.get("taskId") if resp_data.get("status") != 3 else None

    async def puzzle_claim(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/puzzleClaim", json=data)

    @staticmethod
    async def get_combo():
        async with aiohttp.request(method="GET", url="https://raw.githubusercontent.com/zuydd/database/refs/heads/main/tomarket.json") as resp:
            if resp.status in range(200, 300):
                return json.loads(await resp.text()).get('puzzle', {})

    async def check_airdrop(self, http_client: CloudflareScraper, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/check", json=data)

    async def add_tomato_to_first_name(self):
        if '🍅' not in self.user_data.get('first_name'):
            await self.tg_client.update_profile(first_name=f"{self.user_data.get('first_name')} 🍅")

    async def check_blacklist(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/rank/blacklist", json=data)

    async def token_weeks(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/weeks", json=data)

    async def claim_airdrop(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/claim", json=data)

    async def airdrop_task(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/airdropTasks", json=data)

    async def airdrop_start_task(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/startTask", json=data)

    async def airdrop_check_task(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/checkTask", json=data)

    async def airdrop_claim_task(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/tasks/claimTask", json=data)

    async def check_toma(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/tomatoes", json=data)

    async def convert_toma(self, http_client):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/tomatoToStar")

    async def check_season_reward(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/season", json=data)

    async def launchpad_list(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/list", json=data)

    async def launchpad_get_auto_farms(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/getAutoFarms", json=data)

    async def launchpad_toma_balance(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/tomaBalance", json=data)

    async def launchpad_tasks(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/tasks", json=data)

    async def launchpad_task_claim(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/taskClaim", json=data)

    async def launchpad_invest_toma(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/investToma", json=data)

    async def launchpad_claim_auto_farm(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/claimAutoFarm", json=data)

    async def launchpad_start_auto_farm(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/launchpad/startAutoFarm", json=data)

    async def query_treasure_pool_balance(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/queryTreasureBoxTomaPoolBalance", json=data)

    async def get_token_balance(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/token/balance", json=data)

    async def query_treasure_box_balance(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/queryTreasureBoxBalance", json=data)

    async def get_treasure_box_history(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/treasureBoxHistory", json=data)

    async def is_treasure_box_open(self, http_client, data):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/isTreasureBoxOpen", json=data)

    async def query_inviter_info(self, http_client):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/queryInviterInfo")

    async def open_treasure_box(self, http_client):
        return await self.make_request(http_client, "POST", f"{API_ENDPOINT}/invite/openTreasureBox")

    async def run(self):
        random_delay = uniform(1, settings.SESSION_START_DELAY)
        logger.info(self.log_message(f"Bot will start in <lr>{int(random_delay)}s</lr>"))
        await asyncio.sleep(delay=random_delay)

        access_token_created_time = 0
        init_data = None

        token_live_time = uniform(3500, 3600)

        end_farming_dt = 0
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
                        logger.warning(self.log_message(f"Login failed"))
                        logger.info(self.log_message(f"Sleep <lr>300s</lr>"))
                        await asyncio.sleep(delay=300)
                        continue
                    else:
                        if self.tg_client.is_fist_run:
                            await first_run.append_recurring_session(self.session_name)
                        logger.info(self.log_message(f"<lr>🍅 Logged in successfully</lr>"))
                        http_client.headers["Authorization"] = f"{access_token}"
                    await asyncio.sleep(delay=1)

                    access_token_created_time = time()

                    balance = await self.get_balance(http_client=http_client)
                    rank_data = await self.get_rank_data(http_client,
                                                         {'language_code': 'en', 'init_data': init_data})
                    available_balance = balance.get('data', {}).get('available_balance')
                    current_rank = rank_data.get('data', {}).get('currentRank', {}).get('name')
                    token_balance = await self.get_token_balance(http_client, {'language_code': 'en', 'init_data': init_data})
                    logger.info(self.log_message(f"Current balance: <lr>{available_balance}</lr> | "
                                                 f"Current Rank: <lr>{current_rank}</lr> | "
                                                 f"Token Balance: <ly>{int(token_balance.get('data', {}).get('total', 0))}</ly>"))

                    await self.query_treasure_box_balance(http_client, {'language_code': 'en', 'init_data': init_data})
                    await self.get_treasure_box_history(http_client, {'language_code': 'en', 'init_data': init_data})
                    tb_open = (await self.is_treasure_box_open(http_client, {'language_code': 'en', 'init_data': init_data})).get('data', {}).get('open_status', 1)
                    if not tb_open:
                        inviter = await self.query_inviter_info(http_client)
                        if inviter.get('data', {}).get('tel_firstname'):
                            box_loot = (await self.open_treasure_box(http_client)).get('data', {}).get('toma_reward', 0)
                            if box_loot:
                                logger.success(self.log_message(
                                    f"Successfully opened invite box and got <ly>{box_loot}</ly> Toma"))

                    if settings.ONLY_CHECK_AIRDROP:
                        airdrop_data = (await self.check_airdrop(http_client,
                                                                 {'language_code': 'en', 'init_data': init_data,
                                                                  'round': 'OG'})).get('data', {})
                        toma_drop = airdrop_data.get('tomaAirDrop', {}).get('amount', 0)
                        if toma_drop:
                            logger.success(self.log_message(
                                f"Rank: <lr>{airdrop_data.get('rank')}</lr> | Airdrop amount <lr>{toma_drop}</lr> | "
                                f"Is claimed: <lr>{airdrop_data.get('claimed')}</lr> | "
                                f"Status: <lr>{airdrop_data.get('status')}</lr>"))
                        else:
                            logger.info(self.log_message(f"Account is eligible for airdrop: "
                                                         f"Rank: <lr>{airdrop_data.get('rank')}</lr> | "
                                                         f"Status: <lr>{airdrop_data.get('status')}</lr> "))
                        return f"{self.session_name.lower().strip()};{airdrop_data.get('rank')};{toma_drop};{airdrop_data.get('status')}\n"

                    if 'farming' in balance['data']:
                        end_farm_time = balance.get('data', {}).get('farming', {}).get('end_at')
                        if end_farm_time > time():
                            end_farming_dt = end_farm_time + 240
                            logger.info(self.log_message(
                                f"Farming in progress, next claim in <lr>{round((end_farming_dt - time()) / 60)}m.</lr>"))

                    if time() > end_farming_dt:
                        claim_farming = await self.claim_farming(http_client=http_client)
                        if claim_farming and 'status' in claim_farming:
                            start_farming = None
                            if claim_farming.get('status') in [0, 500]:
                                if claim_farming.get('status') == 0:
                                    farm_points = claim_farming.get('data', {}).get('claim_this_time')
                                    logger.info(self.log_message(
                                        f"Success claim farm. Reward: <lr>{farm_points}</lr> 🍅"))
                                start_farming = await self.start_farming(http_client=http_client)
                            if start_farming and 'status' in start_farming and start_farming['status'] in [0, 200]:
                                logger.info(self.log_message(f"Farm started.. 🍅"))
                                end_farming_dt = start_farming.get('data', {}).get('end_at')
                                logger.info(self.log_message(
                                    f"Next farming claim in <lr>{round((end_farming_dt - time()) / 60)}m.</lr>"))
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
                                            f"Claimed stars | Stars: <lr>+{start_stars_claim['data'].get('stars', 0)}</lr>"))

                                next_stars_check = int(
                                    datetime.fromisoformat(get_stars['data'].get('endTime')).timestamp())

                    await asyncio.sleep(uniform(2, 4))

                    if settings.AUTO_DAILY_REWARD:
                        claim_daily = await self.claim_daily(http_client=http_client)
                        if isinstance(claim_daily, dict) and claim_daily.get("status", 400) != 400:
                            logger.info(self.log_message(
                                f"Daily: <lr>{claim_daily['data']['today_game']}</lr> reward: <lr>{claim_daily['data']['today_points']}</lr>"))

                    await asyncio.sleep(uniform(1, 2))

                    if settings.AUTO_PLAY_GAME:
                        tickets = balance.get('data', {}).get('play_passes', 0)
                        games = sorted(settings.GAMES_PER_CYCLE)
                        games_to_play = randint(min(tickets, games[0]), min(tickets, games[1]))

                        logger.info(self.log_message(f"Available tickets 🎟️: <lr>{tickets}</lr> | Playing <lr>{games_to_play}</lr> games this time."))

                        while tickets > 0:
                            await asyncio.sleep(uniform(1, 5))
                            play_game = await self.play_game(http_client=http_client)
                            if isinstance(play_game, dict) and play_game.get('status') == 0:
                                stars_amount = play_game.get('data', {}).get('stars', 0)
                                await asyncio.sleep(30 + randint(0, 3) * 3)
                                claim_game = await self.claim_game(http_client=http_client,
                                                                   points=randint(
                                                                       settings.POINTS_COUNT[0],
                                                                       settings.POINTS_COUNT[1]),
                                                                   stars=stars_amount)

                                if isinstance(claim_game, dict) and 'status' in claim_game:
                                    if claim_game['status'] == 500 and claim_game['message'] == 'game not start':
                                        continue

                                    elif claim_game.get('status', 500) == 0:
                                        tickets -= 1
                                        games_points = claim_game.get('data', {}).get('points', 0)

                                        logger.info(self.log_message(
                                            f"Game finish! Claimed points: <lr>{games_points}</lr> 🍅 | Stars: <ly>{claim_game.get('data', {}).get('stars', 0)}</ly> ⭐"))

                    if settings.AUTO_TASK:
                        logger.info(self.log_message(f"Start checking tasks."))
                        tasks = await self.get_tasks(http_client=http_client,
                                                     data={'language_code': 'en', 'init_data': init_data})
                        current_time = time()
                        tasks_list = []

                        if tasks and tasks.get("status", 500) == 0:
                            for category, task_group in tasks.get("data", {}).items():
                                task_list = task_group if isinstance(task_group, list) else task_group.get("default", [])
                                logger.info(self.log_message(
                                    f"Checking tasks: <lr>{category}</lr> ({len(task_list)} tasks)"))
                                for task in task_list:
                                    if (task.get('enable') and not task.get('invisible', False) and
                                            task.get('type', '').lower() not in TASK_TYPES_BL and
                                            task.get('name') not in TASK_NAMES_BL):
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
                            if task.get('type') == 'wallet' and not settings.PERFORM_WALLET_TASK:
                                continue

                            wait_second = task.get('waitSecond', 0)
                            if not task.get('status', 0):
                                start_task = await self.start_task(http_client=http_client,
                                                                   data={'task_id': task['taskId'],
                                                                         'init_data': init_data})
                                task_data = start_task.get('data', {}) if start_task else None
                            else:
                                task_data = task
                            if task_data == 'ok' or task_data.get('status') in [1, 2]:
                                logger.info(self.log_message(
                                    f"Start task <lr>{task['name']}.</lr> Wait {wait_second}s 🍅"))
                                if task.get('type') == 'wallet':
                                    get_wallet = await self.get_wallet(http_client)
                                    if not get_wallet:
                                        await asyncio.sleep(uniform(10, 20))
                                        wallet = await self.link_wallet(http_client)
                                        if wallet.get("data") != "ok":
                                            continue
                                        if task_data.get('status') == 2:
                                            await asyncio.sleep(wait_second + uniform(3, 5))
                                if task.get('type') == 'emoji':
                                    await self.add_tomato_to_first_name()
                                if task_data == 'ok' or task_data.get('status') not in [2, 3]:
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
                                            f"Task <lr>{task['name']}</lr> claimed! Reward: {reward} 🍅"))
                                    else:
                                        logger.warning(self.log_message(
                                            f"Task <lr>{task['name']}</lr> not claimed. Reason: "
                                            f"{claim.get('message', 'Unknown error')} 🍅"))
                                await asyncio.sleep(2)

                    await asyncio.sleep(1.5)

                    if settings.REPLACE_WALLET:
                        current_wallet = await self.get_wallet(http_client)
                        if not current_wallet or current_wallet != self.wallet:
                            delete_wallet = await self.delete_wallet(http_client)
                            if delete_wallet.get('data') == 'ok':
                                logger.info(f'Removed linked wallet: {current_wallet}')
                            await asyncio.sleep(uniform(10, 20))
                            wallet = await self.link_wallet(http_client)
                            if wallet.get('data') == 'ok':
                                logger.info(f"Successfully linked new wallet {self.wallet}")

                    if settings.AUTO_CLAIM_COMBO:
                        combo_id = await self.get_puzzle_status(http_client,
                                                                data={'language_code': 'en', 'init_data': init_data})
                        combo = await self.get_combo()
                        if combo and combo.get("task_id") == combo_id:
                            await asyncio.sleep(uniform(3, 10))
                            combo['code'] = combo['code'].replace(' ', '')
                            claim_combo = await self.puzzle_claim(http_client, combo)
                            if claim_combo.get("status") == 0 and \
                                    'incorrect' not in claim_combo.get('data', {}).get('message', ""):
                                logger.success(self.log_message("Successfully claimed daily puzzle"))

                    await asyncio.sleep(1.5)

                    spin_tickets = await self.get_spin_tickets(http_client=http_client,
                                                               data={'language_code': 'en', 'init_data': init_data})
                    free_spin = await self.get_spin_free(http_client=http_client,
                                                         data={'language_code': 'en', 'init_data': init_data})
                    if free_spin.get('data', {}).get('is_free', False):
                        await self.get_spin_assets(http_client=http_client,
                                                   data={'language_code': 'en', 'init_data': init_data})
                        await asyncio.sleep(uniform(1, 2))
                        result = await self.spin_once(http_client)
                        await asyncio.sleep(uniform(1, 2))
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
                        await asyncio.sleep(uniform(1, 2))
                        result = await self.spin_raffle(http_client, data={"category": "ticket_spin_1"})
                        await asyncio.sleep(uniform(1, 2))
                        result_data = result.get('data', {}).get('results', [{}])[0]
                        if result and result_data:
                            logger.success(self.log_message(f'Used free spin. Got <lr>{result_data.get("amount")} '
                                                            f'{result_data.get("type")}</lr>'))
                        spin_tickets = await self.get_spin_tickets(http_client=http_client,
                                                                   data={'language_code': 'en', 'init_data': init_data})
                        await asyncio.sleep(uniform(1, 2))

                    if await self.create_rank(http_client=http_client):
                        logger.info(self.log_message(f"Rank created! 🍅"))

                    if settings.AUTO_RANK_UPGRADE:
                        rank_data = await self.get_rank_data(http_client,
                                                             {'language_code': 'en', 'init_data': init_data})
                        unused_stars = rank_data.get('data', {}).get('unusedStars', 0)
                        logger.info(self.log_message(f"Unused stars {unused_stars}"))
                        if unused_stars > 0:
                            await asyncio.sleep(uniform(10, 20))
                            upgrade_rank = await self.upgrade_rank(http_client=http_client, stars=unused_stars)
                            if upgrade_rank.get('status', 500) == 0:
                                logger.info(self.log_message(f"Rank upgraded! 🍅"))
                                if upgrade_rank.get('data', {}).get('isUpgrade', False):
                                    share = await self.share_tg(http_client)
                                    if share.get('data') == 'ok':
                                        logger.success(self.log_message(
                                            "Successfully posted a story and got <lr>2000</lr> 🍅."))
                            else:
                                logger.info(self.log_message(
                                    f"Rank not upgraded. Reason: {upgrade_rank.get('message', 'Unknown error')} 🍅"))

                    if settings.AUTO_CLAIM_AIRDROP:
                        airdrop_check = await self.check_airdrop(http_client=http_client,
                                                                 data={"language_code": "en", "init_data": init_data,
                                                                       "round": "One"})
                        if airdrop_check and airdrop_check.get('status', 500) == 0:
                            token_weeks = await self.token_weeks(http_client=http_client,
                                                                 data={"language_code": "en", "init_data": init_data})
                            round_names = [item['round']['name'] for item in token_weeks.get('data', {}) if
                                           not item['claimed'] and item['stars'] > 0]
                            logger.info(self.log_message(f"Effective claim round:{round_names}.")) if round_names else \
                                logger.info(self.log_message("No Weekly airdrop available to claim."))
                            for round_name in round_names:
                                claim_airdrop = await self.claim_airdrop(http_client=http_client,
                                                                         data={"round": f"{round_name}"})
                                if claim_airdrop and claim_airdrop.get('status', 500) == 0:
                                    logger.success(self.log_message(
                                        f"Airdrop claimed! Token: <lr>+{claim_airdrop.get('data', {}).get('amount', 0)}"
                                        f" TOMA</lr> 🍅"))
                                else:
                                    log_error(self.log_message(
                                        "Airdrop not claimed. Reason: {claim_airdrop.get('message', 'Unknown error')}"))
                                await asyncio.sleep(uniform(2, 5))
                    await asyncio.sleep(randint(3, 5))

                    if settings.AUTO_AIRDROP_TASK:
                        airdrop_task = await self.airdrop_task(http_client=http_client,
                                                               data={"language_code": "en", "init_data": init_data,
                                                                     "round": "One"})
                        if airdrop_task and airdrop_task.get('status', 500) == 0:
                            for task in airdrop_task.get('data', []):
                                current_counter = task.get('currentCounter', 0)
                                check_counter = task.get('checkCounter', 0)
                                current_round = task.get('round', 'Unknown')
                                name = task.get('name', 'Unknown')
                                task_id = task.get('taskId', 'Unknown')
                                status = task.get('status', 500)
                                check_task = await self.airdrop_check_task(http_client=http_client,
                                                                           data={"task_id": task_id, "round": "One"})

                                if current_round == 'One':
                                    if status == 0:
                                        start_task = await self.airdrop_start_task(http_client=http_client,
                                                                                   data={"task_id": task_id,
                                                                                         "round": "One"})
                                        if start_task and start_task.get('status', 500) == 0:
                                            logger.success(self.log_message(f"Airdrop task <lr>{name}</lr> started!"))
                                        await asyncio.sleep(uniform(2, 5))
                                    elif status == 1:
                                        check_task = await self.airdrop_check_task(http_client=http_client,
                                                                                   data={"task_id": task_id,
                                                                                         "round": "One"})
                                        if check_task and check_task.get('status', 500) == 0:
                                            logger.success(self.log_message(f"Airdrop task <lr>{name}</lr> checked!"))
                                        await asyncio.sleep(uniform(2, 5))
                                    elif status == 2 and current_counter >= check_counter:
                                        claim_task = await self.airdrop_claim_task(http_client=http_client,
                                                                                   data={"task_id": task_id,
                                                                                         "round": "One"})
                                        if claim_task and claim_task.get('status', 500) == 0:
                                            logger.success(self.log_message(f"Airdrop task <lr>{name}</lr> claimed!"))
                                        await asyncio.sleep(uniform(2, 5))
                                else:
                                    logger.info(self.log_message(f"Airdrop task <lr>{name}</lr> not ready yet. "
                                                                 f"Progress: {current_counter}/{check_counter}"))

                        else:
                            log_error(self.log_message(f"Failed to get airdrop tasks. Reason: "
                                                       f"{airdrop_task.get('message', 'Unknown error')}"))
                    await asyncio.sleep(uniform(2, 5))

                    if settings.AUTO_LAUNCHPAD_AND_CLAIM:
                        logger.info(self.log_message("Getting launchpad info..."))
                        farms = await self.launchpad_get_auto_farms(http_client=http_client, data={})
                        farms_hash = {}
                        if farms and farms.get('status', 500) == 0:
                            farms_hash = {farm['launchpad_id']: farm for farm in farms.get('data', [])}

                        launchpad_list = await self.launchpad_list(http_client=http_client,
                                                                   data={"language_code": "en",
                                                                         "init_data": init_data})

                        if launchpad_list and launchpad_list.get('status', 500) == 0:
                            for farm in launchpad_list.get('data', []):
                                status = farm.get('status', 0)
                                settleStatus = farm.get('settleStatus', 0)

                                if settleStatus == 1 and status == 2:
                                    can_claim = float(farms_hash.get(farm.get('id')).get('can_claim'))
                                    if can_claim > 0:
                                        claim_auto_farm = await self.launchpad_claim_auto_farm(
                                            http_client=http_client,
                                            data={
                                                'launchpad_id': farm.get(
                                                    'id')})
                                        if claim_auto_farm and claim_auto_farm.get('status', 500) == 0:
                                            logger.success(self.log_message("Claim auto farm successfully!"))
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to claim auto farm. Reason: {claim_auto_farm.get('message', 'Unknown error')}"))
                                        await asyncio.sleep(uniform(1, 3))

                                if settleStatus != 1 or status != 1:
                                    continue
                                tasks = await self.launchpad_tasks(http_client=http_client,
                                                                   data={'launchpad_id': farm.get('id')})
                                if tasks and tasks.get('status', 500) != 0:
                                    continue

                                first_farming = False
                                for task in tasks.get('data', []):
                                    if task.get('status') != 3:
                                        task_claim = await self.launchpad_task_claim(http_client=http_client, data={
                                            'launchpad_id': farm.get('id'), 'task_id': task.get('taskId')})
                                        if task_claim and task_claim.get('status', 500) == 0 and task_claim.get(
                                                'data', {}).get('success', False):
                                            first_farming = True
                                            logger.success(self.log_message(f"claimed launchpad task."))
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to claim launchpad task. Reason: {task_claim.get('message', 'Unknown error')}"))
                                        await asyncio.sleep(randint(3, 5))
                                await asyncio.sleep(randint(30, 35))
                                toma_balance = await self.launchpad_toma_balance(http_client=http_client,
                                                                                 data={"language_code": "en",
                                                                                       "init_data": init_data})
                                balance = float(
                                    toma_balance.get('data', {}).get('balance', 0) if toma_balance and toma_balance.get('status', 500) == 0 else 0)
                                invest_toma_amount = balance if balance >= float(farm.get('minInvestToma')) else 0
                                await asyncio.sleep(randint(1, 3))
                                if first_farming:
                                    invest_toma = await self.launchpad_invest_toma(http_client=http_client,
                                                                                   data={'launchpad_id': farm.get('id'),
                                                                                         'amount': invest_toma_amount})
                                    if invest_toma and invest_toma.get('status', 500) == 0:
                                        logger.success(self.log_message(f"Invest toma {invest_toma_amount} completed!"))
                                    else:
                                        log_error(self.log_message(
                                            f"Failed to invest toma. Reason: {invest_toma.get('message', 'Unknown error')}"))
                                    await asyncio.sleep(randint(1, 3))
                                    start_auto_farm = await self.launchpad_start_auto_farm(http_client=http_client,
                                                                                           data={'launchpad_id': farm.get('id')})
                                    if start_auto_farm and start_auto_farm.get('status', 500) == 0:
                                        logger.success(self.log_message("Start auto toma successfully!"))
                                    else:
                                        log_error(self.log_message(
                                            f"Failed to start auto toma. Reason: {start_auto_farm.get('message', 'Unknown error')}"))
                                else:
                                    can_claim = float(farms_hash.get(farm.get('id')).get('can_claim'))
                                    end_at = float(farms_hash.get(farm.get('id')).get('end_at'))
                                    logger.info(
                                        f"{self.session_name} | current_time: {current_time}s, launchpad_end_at: {end_at}s")
                                    if current_time > end_at:
                                        await asyncio.sleep(uniform(1, 3))
                                        claim_auto_farm = await self.launchpad_claim_auto_farm(
                                            http_client=http_client,
                                            data={
                                                'launchpad_id': farm.get(
                                                    'id')})
                                        if claim_auto_farm and claim_auto_farm.get('status', 500) == 0:
                                            logger.success(f"{self.session_name} | Claim auto farm successfully!")
                                            can_claim = 0
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to claim auto farm. Reason: {claim_auto_farm.get('message', 'Unknown error')}"))
                                    if can_claim <= 0:
                                        await asyncio.sleep(uniform(1, 3))
                                        start_auto_farm = await self.launchpad_start_auto_farm(
                                            http_client=http_client,
                                            data={
                                                'launchpad_id': farm.get(
                                                    'id')})
                                        if start_auto_farm and start_auto_farm.get('status', 500) == 0:
                                            logger.success(self.log_message("Start auto toma successfully!"))
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to start auto toma. Reason: {start_auto_farm.get('message', 'Unknown error')}"))
                                    if invest_toma_amount >= 10000:
                                        invest_toma = await self.launchpad_invest_toma(http_client=http_client,
                                                                                       data={
                                                                                           'launchpad_id': farm.get('id'),
                                                                                           'amount': invest_toma_amount})
                                        if invest_toma and invest_toma.get('status', 500) == 0:
                                            logger.success(self.log_message(f"Invest toma {invest_toma_amount} completed!"))
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to invest toma. Reason: {invest_toma.get('message', 'Unknown error')}"))
                                        await asyncio.sleep(uniform(1, 3))

                    await asyncio.sleep(uniform(2, 5))

                    if settings.AUTO_CONVERT_TOMA:
                        check_toma = await self.check_toma(http_client=http_client,
                                                           data={"language_code": "en", "init_data": init_data})
                        check_toma_status = check_toma.get('status', 500) if check_toma else 500
                        check_toma_message = check_toma.get('message', 'Unknown error') if check_toma else 'Unknown error'
                        check_toma_balance = int(float(check_toma.get('data', {}).get('balance', 0) if check_toma and check_toma.get('data', {}) else 0))

                        if check_toma and check_toma_status == 0:
                            if check_toma_balance > 21000 and check_toma_balance >= randint(
                                    settings.MIN_BALANCE_BEFORE_CONVERT[0], settings.MIN_BALANCE_BEFORE_CONVERT[1]):
                                logger.info(self.log_message(
                                    f"Available TOMA balance to convert: <lr>{check_toma_balance} 🍅</lr>"))

                                convert_toma = await self.convert_toma(http_client=http_client)
                                if convert_toma and convert_toma.get('status', 500) == 0 and \
                                        convert_toma.get('data', {}).get('success', False):
                                    logger.success(f"{self.session_name} | Converted <lr>TOMA</lr> 🍅")
                                else:
                                    log_error(self.log_message(
                                        f"Failed to convert TOMA. Reason: {convert_toma.get('message', 'Unknown error')}"))

                            check_season_reward = await self.check_season_reward(http_client=http_client,
                                                                                 data={"language_code": "en",
                                                                                       "init_data": init_data})
                            if check_season_reward and check_season_reward.get('status', 500) == 0:
                                toma_season = check_season_reward.get('data', {}).get('toma', 0)
                                stars_season = check_season_reward.get('data', {}).get('stars', 0)
                                isCurrent = check_season_reward.get('data', {}).get('isCurrent', True)
                                current_round = check_season_reward.get('data', {}).get('round', {}).get('name')
                                logger.info(self.log_message(
                                    f"Current Weekly reward: <lr>+{toma_season}</lr> Toma 🍅 "
                                    f"for <ly>{stars_season}</ly> ⭐"))

                                if 'tomaAirDrop' and 'status' in check_season_reward.get('data', {}):
                                    check_claim_status = check_season_reward.get('data', {}).get('tomaAirDrop', {}).get(
                                        'status', 0)
                                    token_claim_amount = int(float(
                                        check_season_reward.get('data', {}).get('tomaAirDrop', {}).get('amount', 0)))
                                    if check_claim_status == 2 and token_claim_amount > 0 and isCurrent is False:
                                        logger.info(self.log_message(
                                            f"Claiming Weekly airdrop , <lr>{token_claim_amount}</lr> token 🍅..."))
                                        claim_weekly_airdrop = await self.claim_airdrop(http_client=http_client,
                                                                                        data={"round": current_round})
                                        if claim_weekly_airdrop and claim_weekly_airdrop.get('status', 500) == 0:
                                            logger.success(self.log_message(
                                                f"Successfully claimed weekly airdrop allocation,<lr>+{token_claim_amount}</lr> 🍅"))
                                        else:
                                            log_error(self.log_message(
                                                f"Failed to claim weekly airdrop,Reason :{claim_weekly_airdrop.get('message', 'Unkown')}"))
                        else:
                            log_error(self.log_message(f"Failed to check TOMA balance. Reason: {check_toma_message}"))

                    sleep_time = (end_farming_dt - time()) * uniform(1.0, 1.1)
                    logger.info(self.log_message(f'Sleep <lr>{round(sleep_time / 60, 2)}m.</lr>'))
                    await asyncio.sleep(sleep_time)

                except InvalidSession as error:
                    raise error

                except Exception as error:
                    log_error(self.log_message(f"Unknown error: {error}"))
                    logger.info(self.log_message('Sleep <lr>10m.</lr>'))
                    await asyncio.sleep(600)


async def is_recorded(session_name: str):
    async with aiofiles.open('airdrop.csv', mode='a+') as file:
        await file.seek(0)
        lines = await file.readlines()
    return bool([line for line in lines if session_name.strip().lower() in line])


async def append_airdrop_info(data: str):
    async with aiofiles.open('airdrop.csv', mode='a+') as file:
        await file.writelines(data)


async def run_tapper(tg_client: UniversalTelegramClient):
    runner = Tapper(tg_client=tg_client)
    try:
        result = await runner.run()
        if result and not await is_recorded(runner.session_name):
            await append_airdrop_info(result)
    except InvalidSession as e:
        logger.error(runner.log_message(f"Invalid Session: {e}"))
