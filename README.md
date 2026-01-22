# Blockdag ASIC Mining Pool

An open-source mining pool for BlockDAG supporting Stratum v1.

## Features
- **Stratum Server**: High-performance TCP server for ASIC miners (X30, X100).
- **PPLNS Rewards**: Pay Per Last N Shares system with "Strict Limit" FIFO.
- **VarDiff**: Variable difficulty adjustment for optimal share submission rates.
- **Reorg Protection**: Two-stage maturity (Pending -> Mature -> Unlocked).
- **Dual-Ledger Rewards**: Automatically sums DAG Subsidy + EVM Fees.
- **Pool Fees**: Configurable percentage fee (default 1%).
- **Persistence**: PostgreSQL integration for reliable data storage.

## Prerequisites
- **Go**: 1.22+
- **BlockDAG Node**: Fully synced node with RPC enabled.
- **PostgreSQL**: Database for persisting shares, blocks, and credits.

## Quick Start

### 1. Database Setup
The easiest way to set up PostgreSQL and Adminer is using the provided helper script:

```bash
./setup_db
```

This will:
- Start a PostgreSQL container (`pool-db`)
- Start an Adminer container (`db-gui`)
- Apply the database schema

**Adminer UI**
Access at [http://localhost:8080](http://localhost:8080).
- **System**: PostgreSQL
- **Server**: `pool-db`
- **Username**: `postgres` (or `test` if configured in .env)
- **Password**: `mysecretpassword` (or as configured)
- **Database**: `pool`

### 2. Configuration
Create a `.env` file in the root directory:

```ini
# Server Port
POOL_PORT=:3334

# BlockDAG Node RPC
NODE_RPC_URL=http://127.0.0.1:2011
NODE_RPC_USER=test
NODE_RPC_PASS=test

# PPLNS Settings
PPLNS_N_WORK=1000          # Window size (Target Shares)

# Maturity Settings
POOL_BLOCK_MATURITY=10     # Blocks before balance shows as "Confirmed"
POOL_PAYOUT_MATURITY=4096  # Blocks before coins unlock for payout

# Economics
POOL_FEE_PERCENTAGE=1.0    # 1% Pool Fee

# Database Connection
PG_URL=postgres://postgres:mysecretpassword@localhost:5432/pool

# IPFS Settings
IPFS_NODE_URL=http://localhost:5001
LOG_ROTATION_INTERVAL=24h
```
### 3. Run the ipfs daemon

```bash
ipfs daemon
```

### 3. Run the Pool

```bash
# Build
go build -o bin/pool ./cmd/pool

# Run
./bin/pool
```

## Connecting Miners
Point your ASICs to:
- **URL**: `stratum+tcp://<YOUR_POOL_IP>:3334`
- **User**: `<EVM_WALLET_ADDRESS>` (Must be a valid 0x address)
- **Pass**: `x` (ignored)

## Development
- **Run Tests**: `go test ./...`
- **Lint**: `golangci-lint run`
