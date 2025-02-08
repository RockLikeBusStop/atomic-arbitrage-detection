import asyncio
import aiohttp
import itertools
import time
import os
import sys
import multiprocessing as mp
import random
import uuid
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm

# DB imports, etc.
from db_operations import (
    get_connection,
    setup_database,
    clear_database,
    insert_tokens,
    insert_transactions,
    insert_users,
)
import filters

from solders.pubkey import Pubkey
from spl.token.instructions import get_associated_token_address

load_dotenv()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ALCHEMY_API_KEYS = os.getenv("ALCHEMY_KEYS", "").split(",")
ALCHEMY_BASE_URL = "https://solana-mainnet.g.alchemy.com/v2/"
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
MAX_BLOCK_CONCURRENT_REQUESTS = len(ALCHEMY_API_KEYS) * 10
RESET_DB_ON_START = os.getenv("RESET_DB_ON_START", "false").lower() == "true"

###############################################################################
#                          PRICE FETCHER PROCESS
###############################################################################


async def do_external_fetch(mint_addresses, session, max_retries=3):
    """
    Actually fetch prices from Alchemy (or whichever external API you prefer).
    This is the only place that does real network calls.
    """
    # We'll choose any random key each time
    api_key = random.choice(ALCHEMY_API_KEYS)
    url = f"https://api.g.alchemy.com/prices/v1/{api_key}/tokens/by-address"
    headers = {"accept": "application/json", "content-type": "application/json"}

    payload = {
        "addresses": [
            {"network": "solana-mainnet", "address": mint} for mint in mint_addresses
        ]
    }

    backoff = 1
    mint_to_usd_price = {}
    for attempt in range(max_retries):
        try:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 429:  # Rate limited
                    print(f"Rate limited on {mint_addresses}, backoff {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

                response.raise_for_status()
                data = await response.json()

                if "error" in data:
                    print(f"API error fetching prices: {data['error']}")
                    return mint_to_usd_price  # Return whatever partial or empty

                token_prices_data = data.get("data", {})
                for token_price_data in token_prices_data:
                    address = token_price_data.get("address")
                    prices = token_price_data.get("prices", [])
                    if prices:
                        token_price = float(prices[0].get("value", 0))
                        mint_to_usd_price[address] = token_price

                break  # success, break out of retry loop

        except aiohttp.ClientError as e:
            print(f"Network error: {e}, attempt {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff)
                backoff *= 2
            else:
                print(f"Failed to fetch after {max_retries} attempts.")
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

    return mint_to_usd_price


async def run_price_fetcher(request_queue, response_dict, shared_price_cache):
    """
    Async portion of the price fetcher, reading from request_queue and responding in response_dict.
    """
    # Create a single aiohttp ClientSession for repeated use
    async with aiohttp.ClientSession() as session:
        while True:
            # request_queue.get() is a blocking call in normal multiprocessing.
            # We want it non-blocking or polled in an async context. We'll do a small trick:
            # We'll poll in a small loop. Alternatively, we can run a separate thread reading from queue.
            # For simplicity, let's just do a short sleep if queue is empty.
            if request_queue.empty():
                await asyncio.sleep(0.05)
                continue

            # Get an item
            item = request_queue.get()
            if item is None:
                # Sentinel to stop
                print("Price fetcher received sentinel, shutting down.")
                break

            request_id, mint_addresses = item
            if request_id is None and mint_addresses is None:
                # Another sentinel style
                print("Price fetcher received sentinel, shutting down.")
                break

            # Check what's already in shared_price_cache
            mint_addresses = list(set(mint_addresses))  # deduplicate
            missing = [m for m in mint_addresses if m not in shared_price_cache]

            # If anything is missing, fetch it externally
            if missing:
                fetched = await do_external_fetch(missing, session)
                # store in shared_price_cache
                for m, p in fetched.items():
                    shared_price_cache[m] = p

            # Build the final result from cache
            result = {}
            for mint in mint_addresses:
                # It's possible some wasn't returned by the external call
                # We'll default to 0 or None
                result[mint] = shared_price_cache.get(mint, 0)

            # Store it in response_dict so the requesting consumer can read it
            response_dict[request_id] = result


def price_fetch_process(request_queue, response_dict, shared_price_cache):
    """
    Entry point of the dedicated Price Fetcher process.
    It starts an asyncio event loop running `run_price_fetcher`.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        run_price_fetcher(request_queue, response_dict, shared_price_cache)
    )
    loop.close()


###############################################################################
#                        SYNCHRONOUS HELPER FOR CONSUMERS
###############################################################################


def fetch_token_price_sync(mint_addresses, request_queue, response_dict):
    """
    Synchronous function for consumers to request prices through the dedicated
    price-fetch process.
    1. Generate a unique request_id.
    2. Put (request_id, mint_addresses) on the queue.
    3. Block until the response shows up in `response_dict`.
    4. Return the price map.
    """
    if not mint_addresses:
        return {}

    request_id = str(uuid.uuid4())
    request_queue.put((request_id, mint_addresses))

    # Busy-wait until the response is available
    while request_id not in response_dict:
        time.sleep(0.01)  # yield to avoid hammering CPU

    # Retrieve the result
    result = response_dict[request_id]
    # Optionally remove it from the dict so it doesn't grow unbounded
    del response_dict[request_id]

    return result


###############################################################################
#                          HELPERS / CONSUMER LOGIC
###############################################################################


def check_destinations_signer_owned(token_accounts, token_balances, signer):
    """
    Checks if all token_accounts are associated token accounts for `signer`.
    """
    mint_addresses = set()
    for balance in token_balances:
        mint_addresses.add(balance.get("mint", ""))

    atas = set()
    for mint_address in mint_addresses:
        ata = get_associated_token_address(
            Pubkey.from_string(signer), Pubkey.from_string(mint_address)
        )
        atas.add(ata)

    for token_account in token_accounts:
        if Pubkey.from_string(token_account) not in atas:
            return False

    return True


def get_tokens_and_profits_sync(tx, signer, request_queue, response_dict):
    """
    Synchronous version of your logic to parse token balances & fetch prices.

    1. Build pre_balances
    2. Gather all relevant mint addresses
    3. Do a single call to `fetch_token_price_sync` to get all missing prices
    4. Calculate net changes in user balances
    """
    # 1) Build pre balances
    pre_balances = {}
    for token in tx.get("meta", {}).get("preTokenBalances", []):
        if token.get("owner") == signer:
            mint = token.get("mint")
            amount = token.get("uiTokenAmount", {}).get("amount", 0)
            pre_balances[mint] = int(amount)

    # 2) All relevant mints
    pre_mints = {
        t.get("mint", "")
        for t in tx.get("meta", {}).get("preTokenBalances", [])
        if t.get("owner") == signer
    }
    post_mints = {
        t.get("mint", "")
        for t in tx.get("meta", {}).get("postTokenBalances", [])
        if t.get("owner") == signer
    }
    all_mints = list(pre_mints.union(post_mints))

    # 3) Fetch prices (single synchronous call)
    mint_to_usd_price = fetch_token_price_sync(all_mints, request_queue, response_dict)

    # 4) Compute net changes
    net_balances = {}
    for token in tx.get("meta", {}).get("postTokenBalances", []):
        if token.get("owner") == signer:
            mint = token.get("mint", "")
            pre_amount = pre_balances.get(mint, 0)
            post_amount = int(token.get("uiTokenAmount", {}).get("amount", 0))
            decimals = int(token.get("uiTokenAmount", {}).get("decimals", 0))

            ui_amount = (post_amount - pre_amount) / (10**decimals)
            raw_amount = post_amount - pre_amount
            price = mint_to_usd_price.get(mint, 0)
            net_balances[mint] = {
                "uiAmount": ui_amount,
                "rawAmount": raw_amount,
                "decimals": decimals,
                "usdValue": ui_amount * price,
                "mintToUsd": price,
            }

    return net_balances


###############################################################################
#                          CONSUMER PROCESS
###############################################################################


def transaction_consumer(
    tx_queue, request_queue, response_dict, shared_price_cache, time_between_prints=180
):
    """
    Consumer process that:
      - Reads transactions from tx_queue
      - Filters those that look like multi-swap arb
      - Fetches token prices via the single price-fetch process
      - Inserts data into DB
    """
    db_conn = get_connection()
    last_print_time = time.time()
    try:
        while True:
            item = tx_queue.get()
            if item is None:
                # sentinel => we're done
                break

            tx_list, slot = item  # ( [transactions], slot_number )

            current_time = time.time()
            if current_time - last_print_time >= time_between_prints:
                print(f"Processing slot: {slot}")
                last_print_time = current_time

            if not tx_list:
                continue

            tx_rows = []
            token_rows = []
            user_rows = []

            for tx in tx_list:
                if filters.is_multi_swap_arb(tx):
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

                    # Synchronously get token balances & profits
                    net_balances = get_tokens_and_profits_sync(
                        tx, user_address, request_queue, response_dict
                    )

                    if not net_balances:
                        continue

                    tokens_involved = list(net_balances.keys())
                    token_arbitraged = max(
                        net_balances, key=lambda x: net_balances[x]["usdValue"]
                    )
                    profit = max(
                        float(balance["usdValue"]) for balance in net_balances.values()
                    )

                    # Prepare token data
                    for mint, balance in net_balances.items():
                        token_rows.append(
                            (
                                mint,
                                int(balance["decimals"]),
                                float(balance["mintToUsd"]),
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
                            None,  # default for created_at
                        )
                    )

                    # Prepare user data
                    user_rows.append((user_address, signature, profit))

            # Deduplicate tokens
            unique_token_map = {}
            for mint, decimals, price in token_rows:
                unique_token_map[mint] = (mint, decimals, price)
            deduped_token_rows = list(unique_token_map.values())

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


###############################################################################
#                   ASYNC PRODUCER FOR FETCHING BLOCKS
###############################################################################


async def fetch_block(slot, session, api_key, max_retries=5):
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

    backoff = 1
    for attempt in range(max_retries):
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 429:
                    print(f"Rate limited on slot {slot}. Retry in {backoff} sec.")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                if response.status != 200:
                    print(f"HTTP {response.status} on slot {slot}")
                    return None
                data = await response.json()
                if "error" in data:
                    print(f"API error on slot {slot}: {data['error']}")
                    return None
                return data.get("result")
        except Exception as e:
            print(f"Exception on slot {slot}: {e}, waiting {backoff} sec...")
            await asyncio.sleep(backoff)
            backoff *= 2

    return None


async def fetch_all_blocks(start_slot, end_slot, tx_queue):
    """
    Fetch blocks concurrently for slots [start_slot, end_slot).
    Put (transactions, slot) onto tx_queue for each block that has transactions.
    """
    results = {}
    semaphore = asyncio.Semaphore(MAX_BLOCK_CONCURRENT_REQUESTS)

    # We'll reuse multiple sessions keyed by API keys in a round-robin
    sessions = {k: aiohttp.ClientSession() for k in ALCHEMY_API_KEYS}
    keys_cycle = itertools.cycle(ALCHEMY_API_KEYS)

    async def sem_fetch(slot_):
        api_key = next(keys_cycle)
        session_ = sessions[api_key]
        async with semaphore:
            block = await fetch_block(slot_, session_, api_key)
            return slot_, block

    tasks = [asyncio.create_task(sem_fetch(s)) for s in range(start_slot, end_slot)]

    for fut in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), desc="Fetching blocks"
    ):
        slot, block = await fut
        results[slot] = block
        if block and "transactions" in block:
            tx_queue.put((block["transactions"], slot))

    for s in sessions.values():
        await s.close()

    return results


async def main_async(tx_queue):
    start_slot = int(input("Enter start slot: "))
    end_slot = int(input("Enter end slot: "))
    print(f"Fetching blocks for slots {start_slot} to {end_slot}...")
    await fetch_all_blocks(start_slot, end_slot, tx_queue)
    print("Finished fetching blocks. Processing transactions...")


###############################################################################
#                             MAIN ENTRY POINT
###############################################################################


def main():
    # Setup DB
    conn = get_connection()
    if RESET_DB_ON_START:
        clear_database(conn)
    setup_database(conn)
    conn.close()

    # Create a multiprocessing Manager for shared structures
    manager = mp.Manager()
    tx_queue = mp.Queue()  # for passing transaction-lists to consumers
    request_queue = manager.Queue()  # for price requests to the single fetcher
    response_dict = manager.dict()  # for responses {request_id -> {mint->price}}
    shared_price_cache = manager.dict()  # global mint->price

    # Start the single price-fetcher process
    price_fetcher_proc = mp.Process(
        target=price_fetch_process,
        args=(request_queue, response_dict, shared_price_cache),
        daemon=True,
    )
    price_fetcher_proc.start()

    # Start consumer processes
    # num_consumers = min(8, mp.cpu_count() - 1)
    num_consumers = mp.cpu_count()
    print(f"Starting {num_consumers} consumer processes...")
    consumers = []
    for i in range(num_consumers):
        p = mp.Process(
            target=transaction_consumer,
            args=(tx_queue, request_queue, response_dict, shared_price_cache),
            daemon=True,
        )
        p.start()
        consumers.append(p)

    # Run the async producer for fetching blocks
    asyncio.run(main_async(tx_queue))

    # Signal consumers to finish
    for _ in range(num_consumers):
        tx_queue.put(None)

    # Wait for consumers to finish
    for p in consumers:
        p.join()
        print(f"Consumer {p.name} finished.")

    # Signal the price fetcher to stop
    request_queue.put(None)
    price_fetcher_proc.join()
    print("Price fetcher stopped.")

    print("All processing complete.")


if __name__ == "__main__":
    main()
