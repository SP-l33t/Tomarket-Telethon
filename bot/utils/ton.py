import json
from os import path
from tonsdk.contract.wallet import Wallets, WalletVersionEnum


def generate_wallet(config_path: str, wallets_path=None):
    mnemonics, public_key, private_key, wallet = Wallets.create(WalletVersionEnum.v4r2, workchain=0)
    wallet_address = wallet.address.to_string(True, True, False)
    wall = {
        "mnemonic_phrase": " ".join(mnemonics),
        "public_key": public_key.hex(),
        "private_key": private_key.hex(),
        "wallet_address": wallet_address
    }
    if not wallets_path:
        wallets_path = path.join(path.dirname(config_path), 'wallets.txt')

    with open(wallets_path, "a+") as f:
        json.dump(wall, f, indent=4)

    return wallet_address
