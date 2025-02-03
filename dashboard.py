import streamlit as st
import pandas as pd
from db_operations import (
    get_connection,
    get_total_profit,
    get_average_profit,
    get_top_10_transactions,
    get_most_common_tokens_arbitraged,
    get_top_10_users_by_profit,
    get_success_rate,
)


def main():
    st.title("Solana Atomic Arbitrage Dashboard")

    # Connect to the database
    conn = get_connection()
    try:
        # Fetch all query data
        total_profit = get_total_profit(conn)
        avg_profit = get_average_profit(conn)
        top_transactions = get_top_10_transactions(conn)
        common_tokens = get_most_common_tokens_arbitraged(conn, limit=10)
        top_users = get_top_10_users_by_profit(conn)
        success_rate = get_success_rate(conn)
    finally:
        conn.close()

    # Display profit and success summary
    st.header("Profit & Success Summary")
    st.markdown(f"**Total Profit:** ${total_profit:,.2f}")
    st.markdown(f"**Average Profit:** ${avg_profit:,.2f}")
    st.markdown(f"**Success Rate:** {success_rate * 100:.2f}%")

    # Display top 10 transactions
    st.header("Top 10 Largest Profit Transactions")
    if top_transactions:
        df_transactions = pd.DataFrame(
            top_transactions, columns=["Signature", "Profit (USD)"]
        )
        df_transactions["Profit (USD)"] = (
            df_transactions["Profit (USD)"]
            .astype(float)
            .round(2)
            .apply(lambda x: f"{x:,.2f}")
        )
        st.table(df_transactions)
    else:
        st.write("No transaction data available.")

    # Display most common tokens arbitraged
    st.header("Most Common Tokens Arbitraged")
    if common_tokens:
        df_tokens = pd.DataFrame(common_tokens, columns=["Token", "Count"])
        st.table(df_tokens)
    else:
        st.write("No token data available.")

    # Display top 10 users by total profit
    st.header("Top 10 Users by Total Profit")
    if top_users:
        df_users = pd.DataFrame(
            top_users, columns=["User Address", "Total Profit (USD)"]
        )
        df_users["Total Profit (USD)"] = (
            df_users["Total Profit (USD)"]
            .astype(float)
            .round(2)
            .apply(lambda x: f"{x:,.2f}")
        )
        st.table(df_users)
    else:
        st.write("No user data available.")


if __name__ == "__main__":
    main()
