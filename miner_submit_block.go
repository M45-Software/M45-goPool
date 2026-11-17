package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"time"
)

// A winning block must not wait behind unrelated users of the singleton state
// database connection. On expiry, the exact block is fsynced to the emergency
// spool before submitblock is attempted.
const solvedBlockPersistenceTimeout = 50 * time.Millisecond

// handleBlockShare processes a share that satisfies the network target. It
// builds the full block (reusing any dual-payout header/coinbase when
// available), submits it via RPC, logs the reward split and found-block
// record, and sends the final Stratum response.
func (mc *MinerConn) handleBlockShare(reqID any, job *Job, stratumJobID string, workerName string, en2 []byte, ntime string, nonce string, useVersion uint32, scriptTime int64, solvedHeader, solvedCoinbase []byte, hashHex string, shareDiff float64, now time.Time) {
	var (
		blockHex  string
		submitRes any
		err       error
	)
	if scriptTime == 0 {
		scriptTime = mc.scriptTimeForJob(stratumJobID, job.ScriptTime)
	}
	if len(solvedHeader) > 0 || len(solvedCoinbase) > 0 {
		blockHex, err = assembleSolvedBlock(job, solvedHeader, solvedCoinbase)
		if err != nil {
			if mc.metrics != nil {
				mc.metrics.RecordBlockSubmission("error")
				mc.metrics.RecordErrorEvent("submitblock", err.Error(), now)
			}
			logger.Error("assemble solved block", "remote", mc.id, "error", err)
			mc.writeResponse(StratumResponse{ID: reqID, Result: false, Error: newStratumError(stratumErrCodeInvalidRequest, err.Error())})
			return
		}
	}

	// Only construct the full block (including all non-coinbase transactions)
	// when the share actually satisfies the network target.
	if blockHex == "" {
		if poolScript, workerScript, totalValue, feePercent, ok := mc.dualPayoutParams(job, workerName); ok {
			var cbTx, cbTxid []byte
			var err error
			if job.OperatorDonationPercent > 0 && len(job.DonationScript) > 0 {
				cbTx, cbTxid, err = serializeTripleCoinbaseTxPredecoded(
					job.Template.Height,
					mc.extranonce1,
					en2,
					job.TemplateExtraNonce2Size,
					poolScript,
					job.DonationScript,
					workerScript,
					totalValue,
					feePercent,
					job.OperatorDonationPercent,
					job.witnessCommitScript,
					job.coinbaseFlagsBytes,
					job.CoinbaseMsg,
					scriptTime,
				)
			} else {
				cbTx, cbTxid, err = serializeDualCoinbaseTxPredecoded(
					job.Template.Height,
					mc.extranonce1,
					en2,
					job.TemplateExtraNonce2Size,
					poolScript,
					workerScript,
					totalValue,
					feePercent,
					job.witnessCommitScript,
					job.coinbaseFlagsBytes,
					job.CoinbaseMsg,
					scriptTime,
				)
			}
			if err == nil && len(cbTxid) == 32 {
				var merkleRoot [32]byte
				var merkleOK bool
				if job.merkleBranchesBytes != nil {
					merkleRoot, merkleOK = computeMerkleRootFromBranchesBytes32(cbTxid, job.merkleBranchesBytes)
				} else {
					merkleRoot, merkleOK = computeMerkleRootFromBranches32(cbTxid, job.MerkleBranches)
				}
				if merkleOK {
					header, err := job.buildBlockHeader(merkleRoot[:], ntime, nonce, int32(useVersion))
					if err == nil {
						blockHex, err = assembleSolvedBlock(job, header, cbTx)
					}
				}
			}
		}
	}
	if blockHex == "" {
		// Fallback to single-output block build if dual-payout params are
		// unavailable or any step fails. This reuses the existing helper that
		// constructs a canonical block for submission.
		blockHex, _, _, _, err = buildBlockWithScriptTime(job, mc.extranonce1, en2, ntime, nonce, int32(useVersion), mc.singlePayoutScript(job, workerName), scriptTime)
		if err != nil {
			if mc.metrics != nil {
				mc.metrics.RecordBlockSubmission("error")
				mc.metrics.RecordErrorEvent("submitblock", err.Error(), now)
			}
			logger.Error("submitblock build error", "remote", mc.id, "error", err)
			mc.writeResponse(StratumResponse{ID: reqID, Result: false, Error: newStratumError(stratumErrCodeInvalidRequest, err.Error())})
			return
		}
	}

	// Persist the exact bytes reconstructed from the advertised job before the
	// first submitblock attempt. The submitting state is deliberately excluded
	// from periodic replay; if this process exits mid-attempt, startup changes it
	// back to pending. The SQLite attempt is bounded; on timeout or failure the
	// exact block is fsynced to the emergency spool instead. Persistence is a
	// recovery aid and must never prevent submission when storage is unavailable.
	pendingRec := mc.pendingSubmissionRecord(job, workerName, hashHex, blockHex, shareDiff)
	pendingKey := pendingSubmissionKey(pendingRec)
	persistCtx, cancelPersist := context.WithTimeout(context.Background(), solvedBlockPersistenceTimeout)
	persistErr := appendPendingSubmissionRecordCtx(persistCtx, pendingRec)
	cancelPersist()
	pendingPersisted := persistErr == nil
	if !pendingPersisted {
		spoolRec := pendingRec
		spoolRec.Status = pendingSubmissionStatusPending
		spoolRec.RPCError = persistErr.Error()
		if spoolErr := writePendingSubmissionSpool(mc.cfg.DataDir, spoolRec); spoolErr != nil {
			logger.Error("solved block emergency spool failed",
				"height", job.Template.Height,
				"hash", hashHex,
				"sqlite_error", persistErr,
				"spool_error", spoolErr,
			)
		}
		logger.Warn("solved block persistence failed; submitting immediately",
			"height", job.Template.Height,
			"hash", hashHex,
			"error", persistErr,
		)
	}
	submissionFinished := false
	if pendingPersisted {
		defer func() {
			if submissionFinished {
				return
			}
			// This also runs if an unexpected panic or future early return exits
			// the path after persistence but before an RPC outcome is recorded.
			if changed, updateErr := transitionPendingSubmissionStatus(pendingKey, pendingSubmissionStatusSubmitting, pendingSubmissionStatusPending, "initial submitblock path interrupted"); updateErr != nil {
				logger.Warn("pending submission interruption state update failed", "key", pendingKey, "height", job.Template.Height, "hash", hashHex, "error", updateErr)
			} else if !changed {
				logger.Warn("pending submission interruption state changed unexpectedly", "key", pendingKey, "height", job.Template.Height, "hash", hashHex)
			}
		}()
	}

	// Submit the block via RPC using an aggressive, no-backoff retry loop
	// so we race the rest of the network as hard as possible. This path is
	// intentionally not tied to the miner or process context so shutdown
	// signals do not cancel in-flight submissions.
	err = mc.submitBlockWithFastRetry(job, workerName, hashHex, blockHex, &submitRes)
	if err != nil {
		if pendingPersisted {
			changed, updateErr := transitionPendingSubmissionStatus(pendingKey, pendingSubmissionStatusSubmitting, pendingSubmissionStatusPending, err.Error())
			submissionFinished = updateErr == nil && changed
			if updateErr != nil || !changed {
				logger.Warn("pending submission failure state update failed", "key", pendingKey, "height", job.Template.Height, "hash", hashHex, "changed", changed, "error", updateErr)
				// Retry the durable write with the complete record in case the
				// compare-and-transition failed for a transient database reason.
				fallbackRec := pendingRec
				fallbackRec.Status = pendingSubmissionStatusPending
				fallbackRec.RPCError = err.Error()
				if fallbackErr := appendPendingSubmissionRecord(fallbackRec); fallbackErr != nil {
					logger.Warn("pending submission failure fallback write failed", "key", pendingKey, "height", job.Template.Height, "hash", hashHex, "error", fallbackErr)
				} else {
					submissionFinished = true
				}
			}
		}
		if mc.metrics != nil {
			mc.metrics.RecordBlockSubmission("error")
			mc.metrics.RecordErrorEvent("submitblock", err.Error(), time.Now())
		}
		logger.Error("submitblock error", "error", err)
		// Best-effort: record this block for manual or future retry when the
		// node RPC is unavailable or submitblock fails. This does not imply
		// that the block was accepted; it only preserves the data needed for
		// a later submitblock attempt.
		if !pendingPersisted {
			spoolRec := pendingRec
			spoolRec.Status = pendingSubmissionStatusPending
			spoolRec.RPCError = err.Error()
			if spoolErr := writePendingSubmissionSpool(mc.cfg.DataDir, spoolRec); spoolErr != nil {
				logger.Error("failed block emergency spool update failed", "height", job.Template.Height, "hash", hashHex, "error", spoolErr)
			}
			if fallbackErr := appendPendingSubmissionRecord(spoolRec); fallbackErr == nil {
				if spoolErr := removePendingSubmissionSpool(mc.cfg.DataDir, blockHex); spoolErr != nil {
					logger.Warn("failed block emergency spool cleanup failed", "height", job.Template.Height, "hash", hashHex, "error", spoolErr)
				}
			}
		}
		mc.writeResponse(StratumResponse{ID: reqID, Result: false, Error: newStratumError(stratumErrCodeInvalidRequest, err.Error())})
		return
	}
	if err := completeAcceptedSubmission(pendingKey, pendingRec, mc.discordNotifier); err != nil {
		// The durable row (or emergency spool) remains replayable if recording
		// the acceptance failed. Bitcoin Core already accepted the block.
		logger.Warn("record accepted block", "height", job.Template.Height, "hash", hashHex, "error", err)
	} else {
		submissionFinished = true
		if !pendingPersisted {
			if err := removePendingSubmissionSpool(mc.cfg.DataDir, blockHex); err != nil {
				logger.Warn("accepted block emergency spool cleanup failed", "height", job.Template.Height, "hash", hashHex, "error", err)
			}
		}
	}
	if mc.metrics != nil {
		mc.metrics.RecordBlockSubmission("accepted")
	}

	// For solo mining, treat the worker that submitted the block as the
	// beneficiary of the block reward. We always split the reward between
	// the pool fee and worker payout for logging purposes.
	if logger.Enabled(logLevelInfo) && workerName != "" && job != nil && job.CoinbaseValue > 0 {
		total := job.CoinbaseValue
		feePct, _ := mc.jobPayoutPolicy(job)
		if feePct < 0 {
			feePct = 0
		}
		if feePct > 99.99 {
			feePct = 99.99
		}
		poolFee := max(int64(math.Round(float64(total)*feePct/100.0)), 0)
		if poolFee > total {
			poolFee = total
		}
		minerAmt := total - poolFee
		if minerAmt > 0 {
			logger.Info("block reward split",
				"miner", mc.minerName(workerName),
				"worker_address", workerName,
				"height", job.Template.Height,
				"block_value_sats", total,
				"pool_fee_sats", poolFee,
				"worker_payout_sats", minerAmt,
				"fee_percent", feePct,
			)
		}
	}

	var stats MinerStats
	if logger.Enabled(logLevelInfo) {
		stats = mc.snapshotStats()
	}
	if logger.Enabled(logLevelInfo) {
		logger.Info("block found",
			"miner", mc.minerName(workerName),
			"height", job.Template.Height,
			"hash", hashHex,
			"accepted_total", stats.Accepted,
			"rejected_total", stats.Rejected,
			"worker_difficulty", stats.TotalDifficulty,
		)
	}
	mc.writeTrueResponse(reqID)
}

func assembleSolvedBlock(job *Job, header, coinbase []byte) (string, error) {
	if job == nil {
		return "", fmt.Errorf("missing job for solved block")
	}
	if len(header) != 80 {
		return "", fmt.Errorf("solved block header must be 80 bytes, got %d", len(header))
	}
	if len(coinbase) == 0 {
		return "", fmt.Errorf("solved block coinbase is empty")
	}
	coinbase, err := coinbaseForBlock(coinbase)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	buf.Write(header)
	writeVarInt(&buf, uint64(1+len(job.Transactions)))
	buf.Write(coinbase)
	for _, tx := range job.Transactions {
		raw, err := hex.DecodeString(tx.Data)
		if err != nil {
			return "", fmt.Errorf("decode tx data: %w", err)
		}
		buf.Write(raw)
	}
	return hex.EncodeToString(buf.Bytes()), nil
}

// Capture the payout and share metadata while the advertised job and worker
// session are available. Recovery can then publish the same history record.
func (mc *MinerConn) pendingSubmissionRecord(job *Job, worker, hashHex, blockHex string, shareDiff float64) pendingSubmissionRecord {
	total := job.Template.CoinbaseValue
	feePct, payoutAddress := mc.jobPayoutPolicy(job)
	if feePct < 0 {
		feePct = 0
	}
	if feePct > 99.99 {
		feePct = 99.99
	}
	poolFee := max(int64(math.Round(float64(total)*feePct/100.0)), 0)
	if poolFee > total {
		poolFee = total
	}
	workerAmt := total - poolFee
	// If dual payout is disabled, treat the full reward as a worker payout
	// ("Single" mode = miner only). When dual payout is enabled but the
	// worker has no cached script or the worker and pool scripts resolve to
	// the same beneficiary, treat this block as pool-only and record the full
	// amount as pool_fee_sats with dual_payout_fallback=true.
	dualFallback := false
	workerScript := mc.workerPayoutScript(worker)
	// Check if we fell back to a single-output coinbase because no worker
	// script was available or both outputs had the same decoded beneficiary.
	if len(workerScript) == 0 || bytes.Equal(workerScript, job.PayoutScript) {
		poolFee = total
		workerAmt = 0
		dualFallback = true
	}

	return pendingSubmissionRecord{
		Timestamp:  time.Now().UTC(),
		Height:     job.Template.Height,
		Hash:       hashHex,
		Worker:     mc.minerName(worker),
		BlockHex:   blockHex,
		RPCURL:     mc.cfg.RPCURL,
		PayoutAddr: payoutAddress,
		Status:     pendingSubmissionStatusSubmitting,
		Details: &foundBlockDetails{
			ShareDiff:     shareDiff,
			JobID:         job.JobID,
			CoinbaseValue: total,
			PoolFee:       poolFee,
			WorkerPayout:  workerAmt,
			DualFallback:  dualFallback,
		},
	}
}
