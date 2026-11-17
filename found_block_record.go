package main

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type foundBlockDetails struct {
	ShareDiff     float64 `json:"share_diff"`
	JobID         string  `json:"job_id"`
	CoinbaseValue int64   `json:"coinbase_value_sats"`
	PoolFee       int64   `json:"pool_fee_sats"`
	WorkerPayout  int64   `json:"worker_payout_sats"`
	DualFallback  bool    `json:"dual_payout_fallback"`
}

// Store the same optional metadata in SQLite and the emergency JSON spool.
func (d foundBlockDetails) Value() (driver.Value, error) {
	data, err := json.Marshal(d)
	return string(data), err
}

// Commit history and terminal state together. The first insert for a hash owns
// the Discord notification, including when another submitter receives "duplicate".
func completeAcceptedSubmission(key string, rec pendingSubmissionRecord, notifier *discordNotifier) error {
	db := getSharedStateDB()
	if db == nil {
		return fmt.Errorf("shared state db not initialized")
	}
	hash := rec.Hash
	if hash == "" {
		// Older pending rows may use the whole block as their submission key.
		if len(rec.BlockHex) < 160 {
			return fmt.Errorf("missing accepted block header")
		}
		header, err := hex.DecodeString(rec.BlockHex[:160])
		if err != nil {
			return err
		}
		hash = hex.EncodeToString(reverseBytes(doubleSHA256(header)))
	}
	timestamp := rec.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	record := struct {
		Timestamp     time.Time `json:"timestamp"`
		Height        int64     `json:"height"`
		Hash          string    `json:"hash"`
		Worker        string    `json:"worker"`
		PayoutAddress string    `json:"payout_address"`
		*foundBlockDetails
	}{timestamp, rec.Height, hash, rec.Worker, rec.PayoutAddr, rec.Details}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The single conditional INSERT also recognizes history from older releases.
	// SQLite serializes writers, so competing attempts cannot both insert.
	result, err := tx.Exec(`INSERT INTO found_blocks_log (created_at_unix, json)
		SELECT ?, ? WHERE NOT EXISTS (
			SELECT 1 FROM found_blocks_log WHERE
			CASE WHEN json_valid(json) THEN json_extract(json, '$.hash') END = ? COLLATE NOCASE
		)`, timestamp.Unix(), string(data), hash)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	result, err = tx.Exec(`UPDATE pending_submissions SET status='submitted', rpc_error='' WHERE submission_key=?`, key)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 0 {
		// SQLite may have been unavailable before the initial RPC. Import the
		// exact solved block before the caller can discard its emergency spool.
		rec.Status = pendingSubmissionStatusSubmitted
		rec.RPCError = ""
		if err := upsertPendingSubmission(context.Background(), tx, rec); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if inserted == 1 {
		notifier.NotifyFoundBlock(rec.Worker, rec.Height, hash, timestamp)
	}
	return nil
}
