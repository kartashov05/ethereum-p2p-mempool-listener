import os
import toml
import redis
import rlp
from pprint import pprint
from hexbytes import HexBytes
from eth_utils import keccak
from eth_account._utils.legacy_transactions import Transaction
from eth_account._utils.typed_transactions import TypedTransaction


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, "config.toml")
    config = toml.load(config_path)
    redis_url = config["redis_url"]
    redis_client = redis.Redis.from_url(redis_url)

    print("Waiting for txs...\n")
    while True:
        _, raw_tx = redis_client.blpop("txs")

        tx_hash = "0x" + keccak(raw_tx).hex()

        try:
            if raw_tx[0] <= 0x7f:
                tx = TypedTransaction.from_bytes(HexBytes(raw_tx))
            else:
                tx = rlp.decode(raw_tx, Transaction)

            tx_dict = tx.as_dict()

            print(f"{tx_hash=}")
            pprint(tx_dict)
            print("-"*80)

        except Exception as e:
            print("decode error:", e)


if __name__ == "__main__":
    main()