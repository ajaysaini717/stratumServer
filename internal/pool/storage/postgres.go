package storage

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage defines the interface for persistence
type Storage interface {
	Connect(ctx context.Context) error
	Close()
	AddBlock(ctx context.Context, hash string, height uint64, reward, fees uint64) error
	AddCredits(ctx context.Context, blockHash string, credits map[string]uint64) error
	AddMiner(ctx context.Context, address string) error
	// GetMatureBlocks fetches blocks that are not yet PAID
	GetMatureBlocks(ctx context.Context) ([]Block, error)
	// GetPendingBlocks fetches blocks that are not yet MATURE
	GetPendingBlocks(ctx context.Context) ([]Block, error)
	UpdateBlockStatus(ctx context.Context, hash string, status string) error

	GetUnpaidCredits(ctx context.Context, blockHash string) ([]Credit, error)
	MarkCreditPaid(ctx context.Context, creditID int, txHash string, amountWei *big.Int, minerAddr string) error
}

type Block struct {
	Hash   string
	Height uint64
	Status string
}

type Credit struct {
	ID           int
	MinerAddress string
	Amount       uint64
}

// PostgresStorage implements Storage using pgx
type PostgresStorage struct {
	dbURL string
	pool  *pgxpool.Pool
}

func NewPostgresStorage(url string) *PostgresStorage {
	return &PostgresStorage{
		dbURL: url,
	}
}

func (s *PostgresStorage) Connect(ctx context.Context) error {
	var err error
	s.pool, err = pgxpool.New(ctx, s.dbURL)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	log.Println("✅ Connected to PostgreSQL")
	return nil
}

func (s *PostgresStorage) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStorage) AddBlock(ctx context.Context, hash string, height uint64, reward, fees uint64) error {
	query := `
		INSERT INTO blocks (hash, height, reward, fees, status) 
		VALUES ($1, $2, $3, $4, 'PENDING')
		ON CONFLICT (hash) DO NOTHING
	`
	_, err := s.pool.Exec(ctx, query, hash, height, reward, fees)
	if err != nil {
		return fmt.Errorf("AddBlock error: %w", err)
	}
	return nil
}

func (s *PostgresStorage) GetMatureBlocks(ctx context.Context) ([]Block, error) {
	query := `SELECT hash, height, status FROM blocks WHERE status = 'MATURE'`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocks []Block
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.Hash, &b.Height, &b.Status); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func (s *PostgresStorage) GetPendingBlocks(ctx context.Context) ([]Block, error) {
	query := `SELECT hash, height, status FROM blocks WHERE status = 'PENDING'`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocks []Block
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.Hash, &b.Height, &b.Status); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func (s *PostgresStorage) UpdateBlockStatus(ctx context.Context, hash string, status string) error {
	query := `UPDATE blocks SET status = $1 WHERE hash = $2`
	_, err := s.pool.Exec(ctx, query, status, hash)
	return err
}

func (s *PostgresStorage) AddCredits(ctx context.Context, blockHash string, credits map[string]uint64) error {
	// Use a transaction for bulk insert
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO credits (block_hash, miner_address, amount) VALUES ($1, $2, $3)`

	for miner, amount := range credits {
		_, err := tx.Exec(ctx, query, blockHash, miner, amount)
		if err != nil {
			return fmt.Errorf("failed to insert credit for %s: %w", miner, err)
		}
	}

	return tx.Commit(ctx)
}

func (s *PostgresStorage) AddMiner(ctx context.Context, address string) error {
	query := `
		INSERT INTO miners (address, last_active) 
		VALUES ($1, NOW())
		ON CONFLICT (address) 
		DO UPDATE SET last_active = NOW()
	`
	_, err := s.pool.Exec(ctx, query, address)
	if err != nil {
		return fmt.Errorf("AddMiner error: %w", err)
	}
	return nil
}

func (s *PostgresStorage) GetUnpaidCredits(ctx context.Context, blockHash string) ([]Credit, error) {
	query := `SELECT id, miner_address, amount FROM credits WHERE block_hash = $1 AND is_paid = FALSE`
	rows, err := s.pool.Query(ctx, query, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var credits []Credit
	for rows.Next() {
		var c Credit
		if err := rows.Scan(&c.ID, &c.MinerAddress, &c.Amount); err != nil {
			return nil, err
		}
		credits = append(credits, c)
	}
	return credits, nil
}

func (s *PostgresStorage) MarkCreditPaid(ctx context.Context, creditID int, txHash string, amountWei *big.Int, minerAddr string) error {
	// 1. Mark Credit as Paid
	query := `UPDATE credits SET is_paid = TRUE WHERE id = $1`
	if _, err := s.pool.Exec(ctx, query, creditID); err != nil {
		return fmt.Errorf("failed to update credit status: %w", err)
	}

	// 2. Insert into Payouts Table
	// We store the exact Wei amount sent.

	insertSimple := `
		INSERT INTO payouts (tx_hash, amount, created_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (tx_hash) DO NOTHING
	`
	// Convert big.Int to string for NUMERIC
	if _, err := s.pool.Exec(ctx, insertSimple, txHash, amountWei.String()); err != nil {
		// Log error but don't fail the whole operation since credit is already marked paid
		log.Printf("⚠️ Failed to record payout in DB: %v", err)
	}

	return nil
}
