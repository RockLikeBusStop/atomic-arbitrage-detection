# Solana Atomic Arbitrage Detection

This project analyzes Solana blockchain transactions to detect and track atomic arbitrage trades. It processes blockchain data in real-time, identifies arbitrage patterns, and stores the results in a PostgreSQL database for analysis.

## Overview

The system identifies atomic arbitrage transactions by looking for specific patterns in transaction instructions where:

- Multiple token swaps occur in sequence (number of hops is irrelevant)
- The starting and ending token are the same
- The transactions are atomic (executed in a single transaction)
- The trades result in a profit

## Technical Architecture

### Components

1. **Block Fetcher (Producer)**

   - Asynchronously fetches block data from Solana using Alchemy API
   - Implements rate limiting and retry logic
   - Uses multiple API keys for increased throughput
   - Pushes relevant transactions to a processing queue

2. **Transaction Processor (Consumer)**

   - Multiple consumer processes analyze transactions in parallel
   - Selects different arbitrage filters to use
   - Identifies arbitrage patterns in transaction instructions
   - Calculates profits and token movements
   - Stores results in PostgreSQL Database

3. **Price Fetcher Process**

   - Dedicated process for handling token price requests
   - Maintains shared price cache across consumers
   - Handles rate limiting and retries for price API calls

4. **Dashboard**

   - Displays key metrics and transaction data on Streamlit dashboard

5. **Database Layer**
   - Stores transaction data, token information, and user statistics
   - Tables:
     - `tokens`: Token metadata and prices
     - `transactions`: Detailed transaction information
     - `users`: User-specific transaction data and profits

## Setup

1. **Environment Variables**
   Create a `.env` file with:

   ```
   ALCHEMY_KEYS=your_key1,your_key2,...
   POSTGRES_PASSWORD=your_password
   RESET_DB_ON_START=false
   ```

2. **Database Setup**

   ```bash
   docker-compose up -d
   ```

## Usage

1. **Run the System**

   ```bash
   python fetch_transaction.py
   ```

   You'll be prompted to enter:

   - Start slot number (inclusive)
   - End slot number (exclusive)

2. **Launch Dashboard**

   Once the fetch process is complete, you can launch the dashboard:

   ```bash
   streamlit run dashboard.py
   ```

3. **Query Results**
   The system provides several analysis functions through both the dashboard and direct database queries:
   - Get total profit across all transactions
   - Calculate average profit per transaction
   - View top 10 most profitable transactions
   - Analyze most commonly arbitraged tokens
   - List top 10 users by profit
   - Monitor transaction success rates

## Database Schema

### tokens

- `mint_address` (TEXT, PK): Token's mint address
- `decimals` (INT): Token's decimal places
- `mint_to_usd` (NUMERIC): Token's USD price

### transactions

- `signature` (TEXT, PK): Transaction signature
- `slot_number` (BIGINT): Block slot number
- `user_address` (TEXT): User's wallet address
- `block_time` (TIMESTAMP): Transaction timestamp
- `success` (BOOLEAN): Transaction success status
- `tokens_involved` (TEXT[]): Array of tokens involved
- `token_arbitraged` (TEXT): Main token being arbitraged
- `profit` (NUMERIC): Profit in USD
- `created_at` (TIMESTAMP): Record creation time

### users

- `user_address` (TEXT): User's wallet address
- `transaction_signature` (TEXT): Transaction signature
- `profit` (NUMERIC): Profit for this transaction

## Areas for Improvement

### Transaction Analysis

- Limited support for complex patterns (token account creation/deletion, non-ATA accounts)
- Needs better nested instruction parsing and asset flow tracking

### Data Collection

- Current implementation handles only small slot ranges (<10k) and needs optimization for larger ranges
- Multi-API approach with Alchemy is costly and has rate limit bottlenecks
- Consider alternatives: BigQuery, dedicated RPC nodes, or specialized data providers

### Analysis and Visualization

- Basic profit calculations with limited trend analysis
- Opportunity for advanced analytics (time-series, correlation, risk metrics)
- Enhance visualization with interactive dashboards and network graphs

### Database and Architecture

- Schema optimization for better query performance and transaction metadata
- Consider implementing: message queues, caching layers, and improved failover mechanisms
