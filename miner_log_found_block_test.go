package main

import (
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

// setupTestStateDB initializes a test-specific shared state DB.
func setupTestStateDB(t *testing.T, dataDir string) {
	t.Helper()
	dbPath := filepath.Join(dataDir, "state", "workers.db")
	db, err := openStateDB(dbPath)
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	cleanup := setSharedStateDBForTest(db)
	t.Cleanup(cleanup)
}

// Record the accepted submission and inspect the JSON consumed by the dashboard.
func foundBlockLogForTest(t *testing.T, mc *MinerConn, job *Job, worker string, shareDiff float64) map[string]any {
	t.Helper()
	rec := mc.pendingSubmissionRecord(job, worker, "deadbeef", "abcd", shareDiff)
	if err := completeAcceptedSubmission(rec.Hash, rec, nil); err != nil {
		t.Fatal(err)
	}
	return readLastFoundBlockRecord(t)
}

func readLastFoundBlockRecord(t *testing.T) map[string]any {
	t.Helper()
	var line string
	if err := getSharedStateDB().QueryRow("SELECT json FROM found_blocks_log ORDER BY id DESC LIMIT 1").Scan(&line); err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// TestLogFoundBlockFallback_NoWorkerScript verifies that when there is no
// cached worker payout script, the history record treats the full reward as a
// pool-only payout and sets dual_payout_fallback=true.
func TestLogFoundBlockFallback_NoWorkerScript(t *testing.T) {
	dir := t.TempDir()
	setupTestStateDB(t, dir)
	job := &Job{
		JobID: "log-test",
		Template: GetBlockTemplateResult{
			Height:        100,
			CurTime:       1700000000,
			Bits:          "1d00ffff",
			Previous:      "00",
			CoinbaseValue: 50 * 1e8,
		},
	}
	cfg := Config{
		DataDir:        dir,
		PayoutAddress:  "1PoolAddrExample111111111111111111",
		PoolFeePercent: 2.0,
	}
	mc := &MinerConn{cfg: cfg}

	rec := foundBlockLogForTest(t, mc, job, "worker1", 1.0)

	total := int64(rec["coinbase_value_sats"].(float64))
	poolFee := int64(rec["pool_fee_sats"].(float64))
	workerAmt := int64(rec["worker_payout_sats"].(float64))
	fallback, ok := rec["dual_payout_fallback"].(bool)
	if !ok {
		t.Fatalf("dual_payout_fallback missing or not bool")
	}

	if total != job.Template.CoinbaseValue {
		t.Fatalf("coinbase_value_sats mismatch: got %d, want %d", total, job.Template.CoinbaseValue)
	}
	if poolFee != total || workerAmt != 0 {
		t.Fatalf("expected full reward to pool in fallback: pool_fee=%d worker=%d total=%d", poolFee, workerAmt, total)
	}
	if !fallback {
		t.Fatalf("expected dual_payout_fallback=true when worker script is missing")
	}
}

// TestLogFoundBlockFallback_SameWorkerAddress verifies that when the worker
// address matches the pool payout address, the history record treats the reward as
// pool-only and sets dual_payout_fallback=true even when a worker payout
// script is cached.
func TestLogFoundBlockFallback_SameWorkerAddress(t *testing.T) {
	dir := t.TempDir()
	setupTestStateDB(t, dir)
	addr := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa" // valid mainnet address

	job := &Job{
		JobID: "log-test-same-addr",
		Template: GetBlockTemplateResult{
			Height:        101,
			CurTime:       1700000100,
			Bits:          "1d00ffff",
			Previous:      "00",
			CoinbaseValue: 25 * 1e8,
		},
	}
	cfg := Config{
		DataDir:        dir,
		PayoutAddress:  addr,
		PoolFeePercent: 1.5,
	}
	mc := &MinerConn{cfg: cfg}

	// Cache a valid worker payout script for the worker so the fallback
	// path is triggered by address equality rather than missing script.
	script, err := scriptForAddress(addr, ChainParams())
	if err != nil {
		t.Fatalf("scriptForAddress error: %v", err)
	}
	mc.setWorkerWallet(addr, addr, script)
	job.PayoutScript = script

	rec := foundBlockLogForTest(t, mc, job, addr, 1.0)

	total := int64(rec["coinbase_value_sats"].(float64))
	poolFee := int64(rec["pool_fee_sats"].(float64))
	workerAmt := int64(rec["worker_payout_sats"].(float64))
	fallback, ok := rec["dual_payout_fallback"].(bool)
	if !ok {
		t.Fatalf("dual_payout_fallback missing or not bool")
	}

	if total != job.Template.CoinbaseValue {
		t.Fatalf("coinbase_value_sats mismatch: got %d, want %d", total, job.Template.CoinbaseValue)
	}
	if poolFee != total || workerAmt != 0 {
		t.Fatalf("expected full reward to pool when worker address equals payout: pool_fee=%d worker=%d total=%d", poolFee, workerAmt, total)
	}
	if !fallback {
		t.Fatalf("expected dual_payout_fallback=true when worker address equals pool payout address")
	}
}

func TestLogFoundBlockCaseFoldedLabelsUseDistinctScripts(t *testing.T) {
	dir := t.TempDir()
	setupTestStateDB(t, dir)
	poolScript := []byte{0x51}
	workerScript := []byte{0x52}
	poolLabel := "1CaseSensitiveBeneficiary"
	workerLabel := "1casesensitivebeneficiary"
	if !strings.EqualFold(poolLabel, workerLabel) || poolLabel == workerLabel {
		t.Fatal("test labels must differ only by case")
	}
	job := &Job{
		JobID: "log-test-case-sensitive-beneficiary",
		Template: GetBlockTemplateResult{
			Height:        103,
			CoinbaseValue: 1000,
		},
		PayoutScript: poolScript,
	}
	mc := &MinerConn{cfg: Config{
		DataDir:        dir,
		PayoutAddress:  poolLabel,
		PoolFeePercent: 2,
	}}
	mc.setWorkerWallet(workerLabel, workerLabel, workerScript)

	rec := foundBlockLogForTest(t, mc, job, workerLabel, 1)
	fallback, ok := rec["dual_payout_fallback"].(bool)
	if !ok {
		t.Fatal("dual_payout_fallback missing or not bool")
	}
	if fallback {
		t.Fatal("case-fold-equal labels with distinct scripts were logged as one beneficiary")
	}
	if got := int64(rec["pool_fee_sats"].(float64)); got != 20 {
		t.Fatalf("pool fee = %d, want 20", got)
	}
	if got := int64(rec["worker_payout_sats"].(float64)); got != 980 {
		t.Fatalf("worker payout = %d, want 980", got)
	}
}

// TestLogFoundBlock_NoFallbackDifferentAddress verifies that when the worker
// address differs from the pool payout address and a worker payout script is
// cached, the history record shows a pool/worker split based on PoolFeePercent
// and dual_payout_fallback=false.
func TestLogFoundBlock_NoFallbackDifferentAddress(t *testing.T) {
	dir := t.TempDir()
	setupTestStateDB(t, dir)
	poolAddr := "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"   // valid mainnet
	workerAddr := "1BoatSLRHtKNngkdXEeobR76b53LETtpyT" // another valid mainnet

	job := &Job{
		JobID: "log-test-no-fallback",
		Template: GetBlockTemplateResult{
			Height:        102,
			CurTime:       1700000200,
			Bits:          "1d00ffff",
			Previous:      "00",
			CoinbaseValue: 10 * 1e8,
		},
	}
	cfg := Config{
		DataDir:        dir,
		PayoutAddress:  poolAddr,
		PoolFeePercent: 2.5,
	}
	mc := &MinerConn{cfg: cfg}

	script, err := scriptForAddress(workerAddr, ChainParams())
	if err != nil {
		t.Fatalf("scriptForAddress error: %v", err)
	}
	mc.setWorkerWallet(workerAddr, workerAddr, script)

	rec := foundBlockLogForTest(t, mc, job, workerAddr, 1.0)

	total := int64(rec["coinbase_value_sats"].(float64))
	poolFee := int64(rec["pool_fee_sats"].(float64))
	workerAmt := int64(rec["worker_payout_sats"].(float64))
	fallback, ok := rec["dual_payout_fallback"].(bool)
	if !ok {
		t.Fatalf("dual_payout_fallback missing or not bool")
	}

	if total != job.Template.CoinbaseValue {
		t.Fatalf("coinbase_value_sats mismatch: got %d, want %d", total, job.Template.CoinbaseValue)
	}

	// Mirror the pool's split logic.
	feePct := cfg.PoolFeePercent
	if feePct < 0 {
		feePct = 0
	}
	if feePct > 99.99 {
		feePct = 99.99
	}
	expectedPoolFee := max(int64(math.Round(float64(total)*feePct/100.0)), 0)
	if expectedPoolFee > total {
		expectedPoolFee = total
	}
	expectedWorker := total - expectedPoolFee

	if poolFee != expectedPoolFee || workerAmt != expectedWorker {
		t.Fatalf("unexpected split: pool_fee=%d worker=%d total=%d expected_pool=%d expected_worker=%d",
			poolFee, workerAmt, total, expectedPoolFee, expectedWorker)
	}
	if fallback {
		t.Fatalf("expected dual_payout_fallback=false when worker address differs from pool payout")
	}
}
