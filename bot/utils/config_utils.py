import json
from bot.utils import logger, error


def read_config_file(relative_path: str = 'bot/config/accounts_config.json') -> dict:
    """Reads the contents of a config file. If the file does not exist, creates it.

     Args:
       relative_path: Path to the .json file.

     Returns:
       The contents of the file, or an empty dict if the file was empty or created.
     """
    try:
        with open(relative_path, 'r') as f:
            content = f.read()
            config = json.loads(content) if content else {}
    except FileNotFoundError:
        config = {}
        with open(relative_path, 'w'):
            print(f"Accounts config file `{relative_path}` not found. Creating a new one.")
    return config


def write_config_file(content: dict, relative_path: str = 'bot/config/accounts_config.json'):
    """Writes the contents of a config file. If the file does not exist, creates it.

     Args:
       relative_path: Path to the .json file.
       content (dict): Content we want to write

     Returns:
       The contents of the file, or an empty dict if the file was empty or created.
     """
    try:
        with open(relative_path, 'w+') as f:
            json.dump(content, f, indent=2)
    except IOError as e:
        logger.error(f"An error occurred while writing to {relative_path}: {e}")


def get_session_config(session_name: str, relative_path: str = 'bot/config/accounts_config.json') -> dict:
    """Gets the session config for specified session name.

     Args:
       session_name (dict): The name of the session
       relative_path: Path to the .json file.

     Returns:
       The config object for specified session_name, or an empty dict if the file was empty or created.
     """
    return read_config_file(relative_path).get(session_name, {})


def update_config_file(session_name: str, updated_session_config: dict,
                       relative_path: str = 'bot/config/accounts_config.json'):
    """Updates the content of a session in config file. If the file does not exist, creates it.

     Args:
       session_name (dict): The name of the session
       updated_session_config (dict): The config to override
       relative_path: Path to the .json file.

     Returns:
       The contents of the file, or an empty dict if the file was empty or created.
     """
    try:
        config = read_config_file(relative_path)
        config[session_name] = updated_session_config
        write_config_file(config)
    except Exception as e:
        error(e)
