from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str
    GLOBAL_CONFIG_PATH: str = "TG_FARM"

    FIX_CERT: bool = False

    REF_ID: str = '0000GbQY'
    
    POINTS_COUNT: list[int] = [380, 480]
    AUTO_PLAY_GAME: bool = True
    GAMES_PER_CYCLE: list[int] = [10, 30]
    AUTO_TASK: bool = True
    AUTO_DAILY_REWARD: bool = True
    AUTO_CLAIM_STARS: bool = True
    AUTO_CLAIM_COMBO: bool = True
    AUTO_RANK_UPGRADE: bool = True
    AUTO_AIRDROP_TASK: bool = True
    AUTO_CLAIM_AIRDROP: bool = True
    AUTO_LAUNCHPAD_AND_CLAIM: bool = True
    AUTO_CONVERT_TOMA: bool = True
    MIN_BALANCE_BEFORE_CONVERT: list[int] = [30000, 100000]
    PERFORM_WALLET_TASK: bool = False
    REPLACE_WALLET: bool = False

    ONLY_CHECK_AIRDROP: bool = False

    SESSION_START_DELAY: int = 360

    SESSIONS_PER_PROXY: int = 1
    USE_PROXY_FROM_FILE: bool = True
    DISABLE_PROXY_REPLACE: bool = False
    USE_PROXY_CHAIN: bool = False

    DEVICE_PARAMS: bool = False

    DEBUG_LOGGING: bool = False


settings = Settings()

