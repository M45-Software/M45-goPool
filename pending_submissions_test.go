package main

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Test that replayPendingSubmissions successfully resubmits a pending block
// and marks it as "submitted" in SQLite.
func TestReplayPendingSubmissionsMarksSubmitted(t *testing.T) {
	t.Helper()

	tmpDir := t.TempDir()

	// Set up the shared DB for this test
	dbPath := filepath.Join(tmpDir, "state", "workers.db")
	db, err := openStateDB(dbPath)
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	defer db.Close()
	cleanup := setSharedStateDBForTest(db)
	defer cleanup()

	rec := pendingSubmissionRecord{
		Timestamp:  time.Now().UTC(),
		Height:     100,
		Hash:       "test-hash",
		Worker:     "worker1",
		BlockHex:   "deadbeef",
		RPCError:   "initial error",
		RPCURL:     "http://127.0.0.1:8332",
		PayoutAddr: "bc1qexample",
		Status:     "pending",
	}
	appendPendingSubmissionRecord(rec)

	var submitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		_ = body
		submitCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":null,"error":null,"id":1}`))
	}))
	defer server.Close()

	rpc := &RPCClient{
		url:     server.URL,
		user:    "",
		pass:    "",
		metrics: nil,
		client:  server.Client(),
		lp:      server.Client(),
		nextID:  1,
	}

	ctx := context.Background()
	replayPendingSubmissions(ctx, rpc, nil)

	if submitCalls == 0 {
		t.Fatalf("expected submitblock to be called at least once")
	}

	// Use the shared DB that was set up earlier
	var status string
	if err := db.QueryRow("SELECT status FROM pending_submissions WHERE submission_key = ?", "test-hash").Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "submitted" {
		t.Fatalf("expected status=submitted, got %q", status)
	}
}

// Test that failed submitblock calls remain pending and are backed off.
func TestReplayPendingSubmissionsFailureBackoff(t *testing.T) {
	t.Helper()

	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "state", "workers.db")
	db, err := openStateDB(dbPath)
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	defer db.Close()
	cleanup := setSharedStateDBForTest(db)
	defer cleanup()

	prevBase := pendingReplayBaseDelay
	prevMax := pendingReplayMaxDelay
	prevJitter := pendingReplayJitterFrac
	prevBackoff := pendingReplayBackoff
	pendingReplayBaseDelay = 10 * time.Millisecond
	pendingReplayMaxDelay = 50 * time.Millisecond
	pendingReplayJitterFrac = 0
	pendingReplayBackoff = newPendingReplayBackoff()
	defer func() {
		pendingReplayBaseDelay = prevBase
		pendingReplayMaxDelay = prevMax
		pendingReplayJitterFrac = prevJitter
		pendingReplayBackoff = prevBackoff
	}()

	rec := pendingSubmissionRecord{
		Timestamp:  time.Now().UTC(),
		Height:     101,
		Hash:       "fail-hash",
		Worker:     "worker2",
		BlockHex:   "deadbeef",
		RPCError:   "initial error",
		RPCURL:     "http://127.0.0.1:8332",
		PayoutAddr: "bc1qexample",
		Status:     "pending",
	}
	appendPendingSubmissionRecord(rec)

	var submitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		submitCalls++
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	rpc := &RPCClient{
		url:     server.URL,
		user:    "",
		pass:    "",
		metrics: nil,
		client:  server.Client(),
		lp:      server.Client(),
		nextID:  1,
	}

	ctx := context.Background()
	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 1 {
		t.Fatalf("expected 1 submit attempt, got %d", submitCalls)
	}

	// Immediate retry should be backed off.
	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 1 {
		t.Fatalf("expected backoff to skip retry, got %d calls", submitCalls)
	}

	time.Sleep(20 * time.Millisecond)
	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 2 {
		t.Fatalf("expected retry after backoff, got %d calls", submitCalls)
	}

	var status string
	if err := db.QueryRow("SELECT status FROM pending_submissions WHERE submission_key = ?", "fail-hash").Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected status=pending, got %q", status)
	}
}

// A non-null BIP22 result means submitblock rejected the block even though the
// JSON-RPC request itself succeeded. Keep the record pending and retry it after
// backoff so transient results such as "inconclusive" do not end recovery.
func TestReplayPendingSubmissionsInconclusiveResultRetries(t *testing.T) {
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "state", "workers.db")
	db, err := openStateDB(dbPath)
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	defer db.Close()
	cleanup := setSharedStateDBForTest(db)
	defer cleanup()

	prevBase := pendingReplayBaseDelay
	prevMax := pendingReplayMaxDelay
	prevJitter := pendingReplayJitterFrac
	prevBackoff := pendingReplayBackoff
	pendingReplayBaseDelay = 10 * time.Millisecond
	pendingReplayMaxDelay = 50 * time.Millisecond
	pendingReplayJitterFrac = 0
	pendingReplayBackoff = newPendingReplayBackoff()
	defer func() {
		pendingReplayBaseDelay = prevBase
		pendingReplayMaxDelay = prevMax
		pendingReplayJitterFrac = prevJitter
		pendingReplayBackoff = prevBackoff
	}()

	appendPendingSubmissionRecord(pendingSubmissionRecord{
		Timestamp: time.Now().UTC(),
		Height:    102,
		Hash:      "inconclusive-hash",
		Worker:    "worker3",
		BlockHex:  "deadbeef",
		RPCError:  "initial error",
		Status:    "pending",
	})

	var submitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		submitCalls++
		w.Header().Set("Content-Type", "application/json")
		if submitCalls == 1 {
			_, _ = w.Write([]byte(`{"result":"inconclusive","error":null,"id":1}`))
			return
		}
		_, _ = w.Write([]byte(`{"result":null,"error":null,"id":1}`))
	}))
	defer server.Close()

	rpc := &RPCClient{
		url:    server.URL,
		client: server.Client(),
		lp:     server.Client(),
		nextID: 1,
	}
	ctx := context.Background()

	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 1 {
		t.Fatalf("expected 1 submit attempt, got %d", submitCalls)
	}
	var status string
	if err := db.QueryRow("SELECT status FROM pending_submissions WHERE submission_key = ?", "inconclusive-hash").Scan(&status); err != nil {
		t.Fatalf("query status after inconclusive result: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected status=pending after inconclusive result, got %q", status)
	}

	// The result rejection participates in the same backoff as RPC failures.
	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 1 {
		t.Fatalf("expected immediate retry to be backed off, got %d calls", submitCalls)
	}

	time.Sleep(20 * time.Millisecond)
	replayPendingSubmissions(ctx, rpc, nil)
	if submitCalls != 2 {
		t.Fatalf("expected retry after backoff, got %d calls", submitCalls)
	}
	if err := db.QueryRow("SELECT status FROM pending_submissions WHERE submission_key = ?", "inconclusive-hash").Scan(&status); err != nil {
		t.Fatalf("query status after successful retry: %v", err)
	}
	if status != "submitted" {
		t.Fatalf("expected status=submitted after successful retry, got %q", status)
	}
}

func TestReplayPendingSubmissionsDuplicateMarksSubmitted(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := openStateDB(filepath.Join(tmpDir, "state", "workers.db"))
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	defer db.Close()
	cleanup := setSharedStateDBForTest(db)
	defer cleanup()

	rec := pendingSubmissionRecord{
		Timestamp: time.Now().UTC(),
		Height:    103,
		Hash:      "duplicate-side-chain-hash",
		Worker:    "worker4",
		BlockHex:  "deadbeef",
		Status:    pendingSubmissionStatusPending,
	}
	if err := appendPendingSubmissionRecord(rec); err != nil {
		t.Fatalf("append pending submission: %v", err)
	}

	var submitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		submitCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"duplicate","error":null,"id":1}`))
	}))
	defer server.Close()
	rpc := &RPCClient{url: server.URL, client: server.Client(), lp: server.Client(), nextID: 1}

	replayPendingSubmissions(context.Background(), rpc, nil)
	if submitCalls != 1 {
		t.Fatalf("submitblock calls = %d, want 1", submitCalls)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM pending_submissions WHERE submission_key = ?`, rec.Hash).Scan(&status); err != nil {
		t.Fatalf("query duplicate status: %v", err)
	}
	if status != pendingSubmissionStatusSubmitted {
		t.Fatalf("duplicate status = %q, want %q", status, pendingSubmissionStatusSubmitted)
	}
}

func TestStartPendingSubmissionReplayerReplaysImmediately(t *testing.T) {
	db := openPendingSubmissionTestDB(t)
	rec := pendingSubmissionRecord{
		Timestamp: time.Now().UTC(),
		Height:    104,
		Hash:      "immediate-replay-hash",
		BlockHex:  "deadbeef",
		Status:    pendingSubmissionStatusPending,
	}
	if err := appendPendingSubmissionRecord(rec); err != nil {
		t.Fatalf("append pending submission: %v", err)
	}

	called := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":null,"error":null,"id":1}`))
	}))
	defer server.Close()
	rpc := &RPCClient{url: server.URL, client: server.Client(), lp: server.Client(), nextID: 1}
	previousInterval := pendingReplayInterval
	pendingReplayInterval = time.Hour
	defer func() { pendingReplayInterval = previousInterval }()
	ctx, cancel := context.WithCancel(context.Background())

	done, err := startPendingSubmissionReplayer(ctx, rpc, nil)
	if err != nil {
		t.Fatalf("start pending replayer: %v", err)
	}
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("pending block waited for the periodic replay ticker")
	}
	deadline := time.Now().Add(time.Second)
	for {
		var status string
		if err := db.QueryRow(`SELECT status FROM pending_submissions WHERE submission_key = ?`, rec.Hash).Scan(&status); err == nil && status == pendingSubmissionStatusSubmitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("immediate replay did not mark block submitted")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPendingSubmissionPeriodicReplayDoesNotReclaimLiveSubmittingRow(t *testing.T) {
	db := openPendingSubmissionTestDB(t)
	var submitCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		submitCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":null,"error":null,"id":1}`))
	}))
	defer server.Close()
	rpc := &RPCClient{url: server.URL, client: server.Client(), lp: server.Client(), nextID: 1}
	previousInterval := pendingReplayInterval
	pendingReplayInterval = 5 * time.Millisecond
	defer func() { pendingReplayInterval = previousInterval }()
	ctx, cancel := context.WithCancel(context.Background())
	done, err := startPendingSubmissionReplayer(ctx, rpc, nil)
	if err != nil {
		t.Fatalf("start pending replayer: %v", err)
	}
	defer func() {
		cancel()
		<-done
	}()

	rec := pendingSubmissionRecord{
		Timestamp: time.Now().UTC(),
		Height:    105,
		Hash:      "live-submitting-hash",
		BlockHex:  "deadbeef",
		Status:    pendingSubmissionStatusSubmitting,
	}
	if err := appendPendingSubmissionRecord(rec); err != nil {
		t.Fatalf("append live submitting row: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if calls := submitCalls.Load(); calls != 0 {
		t.Fatalf("live submitting row replay calls = %d, want 0", calls)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM pending_submissions WHERE submission_key = ?`, rec.Hash).Scan(&status); err != nil {
		t.Fatalf("query live submitting row: %v", err)
	}
	if status != pendingSubmissionStatusSubmitting {
		t.Fatalf("live row status = %q, want submitting", status)
	}
}

// Exercise the gap between Core's acceptance and the SQLite commit, then force
// duplicate delivery from both the periodic scanner and a live miner.
func TestRecoveredAcceptancePublishesHistoryAndDiscordOnce(t *testing.T) {
	db := openPendingSubmissionTestDB(t)
	store, err := newWorkerListStore(filepath.Join(t.TempDir(), "subscriptions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	const worker = "wallet-from-advertised-job.rig-1"
	if err := store.Add("subscriber", worker); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDiscordLink("subscriber", "discord-user", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	status := &StatusServer{workerLists: store}
	status.UpdateConfig(Config{DiscordServerID: "server", DiscordBotToken: "test", DiscordNotifyChannelID: "channel"})
	notifier := &discordNotifier{s: status}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			io.WriteString(w, `{"result":null,"error":null,"id":1}`)
			return
		}
		io.WriteString(w, `{"result":"duplicate","error":null,"id":1}`)
	}))
	defer server.Close()
	rpc := &RPCClient{url: server.URL, client: server.Client(), lp: server.Client(), nextID: 1}
	mc := &MinerConn{id: "test", rpc: rpc, conn: nopConn{}, discordNotifier: notifier}
	job := solvedBlockPersistenceTestJob()
	hash := strings.Repeat("3", 64)
	header := make([]byte, 80)
	coinbase := solvedBlockPersistenceTestCoinbase(t, job)
	if _, err := db.Exec(`CREATE TRIGGER fail_terminal_update BEFORE UPDATE ON pending_submissions
        WHEN NEW.status='submitted' BEGIN SELECT RAISE(FAIL, 'forced commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	runSolvedBlockPersistenceTest(mc, job, header, coinbase, hash)
	if len(loadFoundBlocks("", 10)) != 0 || len(notifier.pingQueue) != 0 {
		t.Fatal("uncommitted acceptance was published")
	}
	if _, err := db.Exec("DROP TRIGGER fail_terminal_update"); err != nil {
		t.Fatal(err)
	}
	// Await the startup scan via its normal shutdown barrier.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, err := startPendingSubmissionReplayer(ctx, rpc, notifier)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(loadFoundBlocks("", 10)) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if len(notifier.pingQueue) != 2 {
		t.Fatalf("first replay queued %d Discord messages, want announcement and subscriber ping", len(notifier.pingQueue))
	}
	// Even if an old spool or interrupted process presents the same block again,
	// including a later live duplicate, the durable history owns the alert.
	if _, err := db.Exec("UPDATE pending_submissions SET status='pending' WHERE submission_key=?", hash); err != nil {
		t.Fatal(err)
	}
	replayPendingSubmissions(context.Background(), rpc, notifier)
	runSolvedBlockPersistenceTest(mc, job, header, coinbase, hash)
	blocks := loadFoundBlocks("", 10)
	if len(blocks) != 1 || blocks[0].Hash != hash || blocks[0].Worker != worker || blocks[0].Height != job.Template.Height ||
		blocks[0].ShareDiff != 1 || blocks[0].PoolFeeSats != job.Template.CoinbaseValue {
		t.Fatalf("recovered history: %+v", blocks)
	}
	record := readLastFoundBlockRecord(t)
	if record["job_id"] != job.JobID || record["payout_address"] != job.PayoutAddress {
		t.Fatalf("job metadata: %+v", record)
	}
	var terminal string
	if err := db.QueryRow("SELECT status FROM pending_submissions WHERE submission_key=?", hash).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if terminal != "submitted" || calls.Load() != 4 {
		t.Fatalf("terminal state %s, RPC calls %d", terminal, calls.Load())
	}
	if len(notifier.pingQueue) != 2 {
		t.Fatalf("duplicate Discord messages: %+v", notifier.pingQueue)
	}
	announcement, ping := notifier.pingQueue[0], notifier.pingQueue[1]
	if !announcement.MentionEveryone || len(announcement.Notices) != 1 ||
		len(ping.LinesByUser["discord-user"]) != 1 || ping.LinesByUser["discord-user"][0] != announcement.Notices[0] ||
		!strings.Contains(announcement.Notices[0], "Block found: height 840001") {
		t.Fatalf("unexpected Discord messages: %+v", notifier.pingQueue)
	}
}

func TestAcceptedSubmissionRecognizesLegacyHistory(t *testing.T) {
	db := openPendingSubmissionTestDB(t)
	header := make([]byte, 80)
	hash := hex.EncodeToString(reverseBytes(doubleSHA256(header)))
	rec := pendingSubmissionRecord{Height: 1, Worker: "worker", BlockHex: hex.EncodeToString(header), Status: "pending"}
	if err := appendPendingSubmissionRecord(rec); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO found_blocks_log(created_at_unix,json) VALUES (1,json_object('hash',?,'height',1))`, strings.ToUpper(hash)); err != nil {
		t.Fatal(err)
	}
	if err := completeAcceptedSubmission(rec.BlockHex, rec, nil); err != nil {
		t.Fatal(err)
	}
	if blocks := loadFoundBlocks("", 10); len(blocks) != 1 || blocks[0].Hash != strings.ToUpper(hash) {
		t.Fatalf("legacy history: %+v", blocks)
	}
}
