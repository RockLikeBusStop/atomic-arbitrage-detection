def is_multi_swap_arb(tx):
    """
    Heuristic check if a transaction looks like a multi-swap arbitrage.
    """
    inner_ixs = tx.get("meta", {}).get("innerInstructions", [])
    if not inner_ixs:
        return False

    swaps = []
    for index in inner_ixs:
        ixs = index.get("instructions", [])
        if len(ixs) < 3:
            continue
        for i in range(len(ixs) - 2):
            parsed0 = ixs[i]
            parsed1 = ixs[i + 1].get("parsed")
            parsed2 = ixs[i + 2].get("parsed")
            if not all(isinstance(x, dict) for x in [parsed0, parsed1, parsed2]):
                continue

            if (
                "transfer" in ixs[i + 1].get("parsed", {}).get("type", "").lower()
                and "transfer" in ixs[i + 2].get("parsed", {}).get("type", "").lower()
            ):
                swaps.append([ixs[i], ixs[i + 1], ixs[i + 2]])

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
        if swap[1].get("parsed", {}).get("info", {}).get("authority", "") != signer:
            return False
        receiver = swap[1].get("parsed", {}).get("info", {}).get("destination", "")
        if receiver != signer:
            if receiver in receivers:
                return False
            receivers.append(receiver)
        else:
            return False

        if prev_destination:
            if (
                swap[1].get("parsed", {}).get("info", {}).get("source", "")
                != prev_destination
            ):
                return False

        if swap[2].get("parsed", {}).get("info", {}).get("authority", "") == signer:
            return False

        destination = swap[2].get("parsed", {}).get("info", {}).get("destination", "")
        if destination:
            swap_destinations.append(destination)
        prev_destination = destination

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


def is_two_swap_arb(tx):
    """
    Heuristic check if a transaction looks like a two-swap arbitrage.
    """
    inner_ixs = tx.get("meta", {}).get("innerInstructions", [])
    if not inner_ixs:
        return False

    swaps = []
    for index in inner_ixs:
        ixs = index.get("instructions", [])
        if len(ixs) < 3:
            continue
        for i in range(len(ixs) - 2):
            parsed0 = ixs[i]
            parsed1 = ixs[i + 1].get("parsed")
            parsed2 = ixs[i + 2].get("parsed")
            if not all(isinstance(x, dict) for x in [parsed0, parsed1, parsed2]):
                continue

            if (
                "transfer" in ixs[i + 1].get("parsed", {}).get("type", "").lower()
                and "transfer" in ixs[i + 2].get("parsed", {}).get("type", "").lower()
            ):
                swaps.append([ixs[i], ixs[i + 1], ixs[i + 2]])

    if len(swaps) < 2 or len(swaps) > 3:
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
        if swap[1].get("parsed", {}).get("info", {}).get("authority", "") != signer:
            return False
        receiver = swap[1].get("parsed", {}).get("info", {}).get("destination", "")
        if receiver != signer:
            if receiver in receivers:
                return False
            receivers.append(receiver)
        else:
            return False

        if prev_destination:
            if (
                swap[1].get("parsed", {}).get("info", {}).get("source", "")
                != prev_destination
            ):
                return False

        if swap[2].get("parsed", {}).get("info", {}).get("authority", "") == signer:
            return False

        destination = swap[2].get("parsed", {}).get("info", {}).get("destination", "")
        if destination:
            swap_destinations.append(destination)
        prev_destination = destination

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
