import asyncio
import aiohttp
import json
import itertools
import time
from tqdm import tqdm
import os
from dotenv import load_dotenv
import sys
import multiprocessing as mp
from spl.token.instructions import get_associated_token_address
from solders.pubkey import Pubkey
import psycopg2
import random
from db_operations import (
    get_connection,
    setup_database,
    clear_database,
    insert_tokens,
    insert_transactions,
    insert_users,
)
from datetime import datetime

load_dotenv()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


ALCHEMY_API_KEYS = os.getenv("ALCHEMY_KEYS", "").split(",")
ALCHEMY_BASE_URL = "https://solana-mainnet.g.alchemy.com/v2/"
COIN_GECKO_API_KEY = os.getenv("COIN_GECKO_API_KEY", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
MINT_TO_USD_PRICE_CACHE = {}

# Maximum number of concurrent requests overall.
MAX_BLOCK_CONCURRENT_REQUESTS = len(ALCHEMY_API_KEYS) * 10
PRICE_FETCH_SEMAPHORE = asyncio.Semaphore(1)


#! Needs fixing for non ATAs
def check_destinations_signer_owned(
    token_accounts: list[str], token_balances: list[dict], signer: str
) -> bool:

    # make a hashset of mint addresses
    mint_addresses = set()
    for balance in token_balances:
        mint_addresses.add(balance.get("mint", ""))

    atas = set()

    # for all mint addresses, get_associated_token_address()
    for mint_address in mint_addresses:
        ata = get_associated_token_address(
            Pubkey.from_string(signer), Pubkey.from_string(mint_address)
        )
        atas.add(ata)

    # if signer == "GqvpRMaYKYRYon1BBEGSDFfqsuwX1zcP5zMVGHD78P2K":
    #     print(f"{mint_addresses=}")
    #     print(f"{atas=}")

    # check if all the token accounts are Signer's ATAs
    for token_account in token_accounts:
        if Pubkey.from_string(token_account) not in atas:
            return False

    return True


async def fetch_token_price(mint_addresses: list[str], max_retries: int = 3):
    # Acquire the semaphore before starting any network call
    async with PRICE_FETCH_SEMAPHORE:
        url = f"https://api.g.alchemy.com/prices/v1/{random.choice(ALCHEMY_API_KEYS)}/tokens/by-address"
        headers = {"accept": "application/json", "content-type": "application/json"}

        # Initialize result dictionary with cached values
        mint_to_usd_price = {}
        uncached_mints = []

        # Separate cached and uncached mints
        for mint in mint_addresses:
            if mint in MINT_TO_USD_PRICE_CACHE:
                mint_to_usd_price[mint] = MINT_TO_USD_PRICE_CACHE[mint]
            else:
                uncached_mints.append(mint)

        # Only make API call if there are uncached mints
        if uncached_mints:
            payload = {
                "addresses": [
                    {"network": "solana-mainnet", "address": mint}
                    for mint in uncached_mints
                ]
            }

            # Wait random amount of time between 0 and 2 seconds
            await asyncio.sleep(1 / random.uniform(1, 15))

            backoff = 1  # Initial backoff time in seconds
            for attempt in range(max_retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            url, headers=headers, json=payload
                        ) as response:
                            if response.status == 429:  # Rate limited
                                print(
                                    f"Rate limited when fetching price of {uncached_mints}. Retrying in {backoff}s..."
                                )
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                continue

                            response.raise_for_status()
                            data = await response.json()

                            if "error" in data:
                                print(
                                    f"API error when fetching prices: {data['error']}"
                                )
                                return mint_to_usd_price

                            token_prices_data = data.get("data", {})

                            for token_price_data in token_prices_data:
                                address = token_price_data.get("address")
                                prices = token_price_data.get("prices", [])
                                if prices:
                                    token_price = float(prices[0].get("value", 0))
                                    mint_to_usd_price[address] = token_price
                                    MINT_TO_USD_PRICE_CACHE[address] = token_price

                            # If we get here, the request was successful
                            break

                except aiohttp.ClientError as e:
                    print(f"Network error when fetching prices: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    continue

                except (ValueError, KeyError) as e:
                    print(f"Error parsing price data: {e}")
                    break

                except Exception as e:
                    print(f"Unexpected error when fetching prices: {e}")
                    break

        return mint_to_usd_price


async def get_tokens_and_profits(tx: dict, signer: str) -> dict:
    # 1) Build pre balances
    pre_balances: dict[str, int] = {}
    for token in tx.get("meta", {}).get("preTokenBalances", []):
        if token.get("owner") == signer:
            mint = token.get("mint")
            amount = token.get("uiTokenAmount", {}).get("amount", 0)
            pre_balances[mint] = int(amount)

    # 2) Build a set of *all* relevant mints (pre + post)
    pre_mints = {
        token.get("mint", "")
        for token in tx.get("meta", {}).get("preTokenBalances", [])
        if token.get("owner") == signer
    }
    post_mints = {
        token.get("mint", "")
        for token in tx.get("meta", {}).get("postTokenBalances", [])
        if token.get("owner") == signer
    }
    all_mints = list(pre_mints.union(post_mints))

    # 3) Fetch prices for all
    mint_to_usd_price = await fetch_token_price(all_mints)

    # 4) Compute net changes
    net_balances: dict[str, dict] = {}
    for token in tx.get("meta", {}).get("postTokenBalances", []):
        if token.get("owner") == signer:
            mint = token.get("mint", "")
            pre_amount = pre_balances.get(mint, 0)
            post_amount = int(token.get("uiTokenAmount", {}).get("amount", 0))
            decimals = int(token.get("uiTokenAmount", {}).get("decimals", 0))

            ui_amount = (post_amount - pre_amount) / (10**decimals)
            raw_amount = post_amount - pre_amount

            # Use .get(...) to avoid KeyError if somehow missing
            price = mint_to_usd_price.get(mint, 0)
            net_balances[mint] = {
                "uiAmount": ui_amount,
                "rawAmount": raw_amount,
                "decimals": decimals,
                "usdValue": ui_amount * price,
                "mintToUsd": price,
            }

    return net_balances


def is_multi_swap_arb(tx: dict) -> bool:
    """
    Check if the transaction is a simple arb swap.
    """
    # TODO: Add error handling
    inner_ixs = tx.get("meta", {}).get("innerInstructions", [])

    # Check if inner instructions exist
    if not inner_ixs:
        return False

    swaps = []

    # Check each index in the inner instructions
    for index in inner_ixs:
        ixs = index.get("instructions", [])

        # Check inner instructions for at least 3 instructions (swap, transfer, transfer)
        if len(ixs) < 3:
            continue

        # Check for a swap 3 pair like (swap, transferChecked, transferChecked)
        for i in range(len(ixs) - 2):
            # # Ensure these three consecutive instructions are all dicts (not strings)
            # if not all(isinstance(ixs[j], dict) for j in [i, i + 1, i + 2]):
            #     continue

            # Maybe a swap instruction?
            if ixs[i].get("accounts", []):
                parsed0 = ixs[i]
                parsed1 = ixs[i + 1].get("parsed")
                parsed2 = ixs[i + 2].get("parsed")

                if not all(isinstance(p, dict) for p in (parsed0, parsed1, parsed2)):
                    continue

                # Maybe next two are transfer instructions?
                if (
                    "transfer" in ixs[i + 1].get("parsed", {}).get("type", "").lower()
                    and "transfer"
                    in ixs[i + 2].get("parsed", {}).get("type", "").lower()
                ):
                    swaps.append([ixs[i], ixs[i + 1], ixs[i + 2]])

    # Should contain at least two swap 3 pairs
    if len(swaps) < 2:
        return False

    signer = (
        tx.get("transaction", {})
        .get("message", {})
        .get("accountKeys", [])[0]
        .get("pubkey", "")
    )
    prev_destination = ""
    receivers = []
    swap_destinations = []

    for swap in swaps:
        # ix_type = swap[1].get("parsed", {}).get("type", "")
        # if ix_type == "transfer":

        # Check if the sender is signer in 2nd instruction
        if swap[1].get("parsed", {}).get("info", {}).get("authority", "") != signer:
            return False

        # Check if the receiver is not the signer in 2nd instruction
        receiver = swap[1].get("parsed", {}).get("info", {}).get("destination", "")
        if receiver != signer:
            # Check if receiver (DEX+tokenPool) is not unique (not expecting repeat swaps)
            if receiver in receivers:
                return False
            receivers.append(receiver)
        else:
            return False

        # Check if the source of the 2nd instruction is the destination of the previous 3rd instruction
        # Chained swaps
        if prev_destination:
            if (
                swap[1].get("parsed", {}).get("info", {}).get("source", "")
                != prev_destination
            ):
                return False

        # Check if the sender is not the signer in 3rd instruction
        if swap[2].get("parsed", {}).get("info", {}).get("authority", "") == signer:
            return False

        # Check if the destination is an account owned by signer in 3rd instruction
        destination = swap[2].get("parsed", {}).get("info", {}).get("destination", "")
        if destination:
            swap_destinations.append(destination)

        # Set previous destination for next swap iteration
        prev_destination = destination

    # # Check if all final swap destinations are owned by signer
    # if swap_destinations:
    #     if not check_destinations_signer_owned(
    #         swap_destinations,
    #         tx.get("meta", {}).get("postTokenBalances", []),
    #         signer,
    #     ):
    #         return False

    # Check if the starting and ending token are the same
    start_token_account = (
        swaps[0][1].get("parsed", {}).get("info", {}).get("source", "")
    )
    end_token_account = (
        swaps[-1][2].get("parsed", {}).get("info", {}).get("destination", "")
    )

    if start_token_account != end_token_account:
        return False

    return True


# --- Consumer Function ---
def transaction_consumer(queue: mp.Queue):
    """
    Consumer process: reads from the queue, filters transactions that involve
    arb swaps, and writes them to the database.
    """
    db_conn = get_connection()

    try:
        while True:
            item = queue.get()
            if item is None:
                break

            tx_rows = []
            token_rows = []
            user_rows = []
            slot = item[1]

            for tx in item[0]:
                if is_multi_swap_arb(tx):
                    # Extract transaction data
                    signature = tx.get("transaction", {}).get("signatures", [""])[0]

                    if slot == 0 and tx.get("slot"):
                        slot = tx.get("slot")

                    user_address = (
                        tx.get("transaction", {})
                        .get("message", {})
                        .get("accountKeys", [])[0]
                        .get("pubkey", "")
                    )
                    block_time = datetime.fromtimestamp(int(tx.get("blockTime", 0)))
                    success = not tx.get("meta", {}).get("err")

                    # Get token balances and profits
                    net_balances = asyncio.run(get_tokens_and_profits(tx, user_address))

                    if not net_balances:
                        continue
                        # print(f"No net balances for {signature}")

                    # Get involved tokens
                    tokens_involved = list(net_balances.keys())

                    # Token arbitraged is the token with the highest profit
                    token_arbitraged = max(
                        net_balances, key=lambda x: net_balances.get(x).get("usdValue")
                    )

                    # Profit is the max of all the net balances
                    profit = max(
                        float(balance.get("usdValue"))
                        for balance in net_balances.values()
                    )

                    # Prepare token data for database
                    for mint, balance in net_balances.items():
                        token_rows.append(
                            (
                                mint,  # mint_address
                                int(balance.get("decimals")),  # decimals
                                float(balance.get("mintToUsd")),  # mint_to_usd
                            )
                        )

                    # Prepare transaction data
                    tx_rows.append(
                        (
                            signature,
                            slot,
                            user_address,
                            block_time,
                            success,
                            tokens_involved,
                            token_arbitraged,
                            profit,
                            None,  # created_at will use default
                        )
                    )

                    # Prepare user data
                    user_rows.append((user_address, signature, profit))

            unique_token_map = {}
            for mint, decimals, price in token_rows:
                unique_token_map[mint] = (mint, decimals, price)

            deduped_token_rows = list(unique_token_map.values())

            # Batch insert to database if we have data
            if tx_rows:
                try:
                    insert_tokens(db_conn, deduped_token_rows)
                    insert_transactions(db_conn, tx_rows)
                    insert_users(db_conn, user_rows)
                except Exception as e:
                    print(f"Error inserting into database: {e}")
                    db_conn.rollback()
                else:
                    db_conn.commit()

    finally:
        db_conn.close()

    # print("Consumer finished processing transactions.")


# --- Async Producer Functions ---
async def fetch_block(
    slot: int, session: aiohttp.ClientSession, api_key: str, max_retries: int = 5
):
    """
    Fetch block data for a given slot using the provided API key.
    Retries with exponential backoff if rate-limited or in case of network errors.
    """
    url = f"{ALCHEMY_BASE_URL}{api_key}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full",
            },
        ],
    }

    # Randomly wait between 0 and 2 seconds
    await asyncio.sleep(1 / random.uniform(1, 15))

    backoff = 1
    for attempt in range(max_retries):
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 429:
                    print(
                        f"Rate limited on slot {slot} with API key {api_key}. Retrying in {backoff} sec (attempt {attempt+1}/{max_retries})..."
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                if response.status != 200:
                    print(
                        f"HTTP {response.status} on slot {slot} with API key {api_key}."
                    )
                    return None
                data = await response.json()
                if "error" in data:
                    print(f"API error on slot {slot}: {data['error']}")
                    return None
                return data.get("result")
        except Exception as e:
            print(f"Exception on slot {slot}: {e}. Retrying in {backoff} sec...")
            await asyncio.sleep(backoff)
            backoff *= 2

    print(f"Failed to fetch slot {slot} after {max_retries} attempts.")
    return None


async def fetch_all_blocks(start_slot: int, end_slot: int, tx_queue: mp.Queue):
    """
    Fetch blocks in the given range concurrently. For each block that contains transactions,
    put the transactions list on the multiprocessing queue.
    """
    results = {}
    semaphore = asyncio.Semaphore(MAX_BLOCK_CONCURRENT_REQUESTS)
    # Create one aiohttp session per API key.
    sessions = {key: aiohttp.ClientSession() for key in ALCHEMY_API_KEYS}
    keys_cycle = itertools.cycle(ALCHEMY_API_KEYS)

    async def sem_fetch(slot: int):
        api_key = next(keys_cycle)
        session = sessions[api_key]
        # print(
        #     f"Trying to start at {time.time()}, currently active: {MAX_CONCURRENT_REQUESTS - semaphore._value}"
        # )
        async with semaphore:
            block = await fetch_block(slot, session, api_key)
            return slot, block

    tasks = [
        asyncio.create_task(sem_fetch(slot)) for slot in range(start_slot, end_slot)
    ]

    # Process tasks as they complete (with progress bar).
    for future in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), desc="Fetching blocks"
    ):
        slot, block = await future
        results[slot] = block
        if block and "transactions" in block:
            # Put the transactions list into the queue.
            tx_queue.put((block["transactions"], slot))
            # await asyncio.to_thread(tx_queue.put, block["transactions"])

    # Close sessions.
    for session in sessions.values():
        await session.close()

    return results


async def main_async(tx_queue: mp.Queue):
    start_slot = int(input("Enter start slot: "))
    end_slot = int(input("Enter end slot: "))
    print(f"Fetching blocks for slots {start_slot} to {end_slot}...")
    results = await fetch_all_blocks(start_slot, end_slot, tx_queue)

    # # Optionally, write all raw block data to a file.
    # with open("blocks.json", "w") as f:
    #     json.dump(results, f, indent=4)
    print("Finished fetching blocks. Processing transactions...")


# --- Main Function ---
def main():
    start_time = time.time()

    # Create a multiprocessing queue.
    tx_queue = mp.Queue()

    # Start consumer processes.
    num_consumers = 1
    # num_consumers = mp.cpu_count()
    print(f"Starting {num_consumers} consumer processes...")
    consumers = []
    for i in range(num_consumers):
        p = mp.Process(target=transaction_consumer, args=(tx_queue,))
        p.start()
        consumers.append(p)

    # Run the async producer to fetch blocks and push transactions to the queue.
    asyncio.run(main_async(tx_queue))

    # After finishing, send a sentinel (None) to each consumer to signal shutdown.
    for _ in range(num_consumers):
        tx_queue.put(None)

    # Wait for all consumer processes to finish.
    for p in consumers:
        p.join()
        print(f"Consumer {p.name} finished")

    elapsed_time = time.time() - start_time
    print(f"All processing complete. Total time taken: {elapsed_time:.2f} seconds")


def connect_to_db():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password=POSTGRES_PASSWORD,
            host="localhost",
            port="5432",
        )
        print("Connected to PostgreSQL successfully!")

        # Create a cursor
        cur = conn.cursor()

        # Execute a simple query
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print("PostgreSQL version:", db_version)

        # Close the connection
        cur.close()
        conn.close()

    except Exception as e:
        print("Error connecting to PostgreSQL:", e)


if __name__ == "__main__":
    conn = get_connection()
    clear_database(conn)
    setup_database(conn)

    # Call queries to get info about the database
    # print(get_total_profit(conn))
    # print(get_average_profit(conn))
    # print(get_top_10_transactions(conn))
    # print(get_most_common_tokens_arbitraged(conn))
    # print(get_top_10_users_by_profit(conn))

    # results = asyncio.run(
    #     get_tokens_and_profits(tx, "BK5XfFrKGTBfMZEk1V3wjpoRsFFmzS42PxkpQJNahx4B")
    # )
    # print(results)
    conn.close()

    main()
