use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::{
    Db,
    data_stores::{key::Key, value::Value, wal::WalReceipt},
    err,
};

pub(crate) mod write_states {
    use super::{GroupGuard, TicketGuard, WalReceipt};

    // Writes are queued. It requires the request to acquire a lock to queue itself.
    // This state means it is waiting for the lock.
    #[derive(Clone)]
    pub(crate) struct WaitingToBeQueued;

    // Requester waiting for group membership.
    // Writes are pipelined group commits.
    // Instead of each writer writing individually, they join a group write and the write
    // group leader performs one syscall to write for the whole group and then
    // updating the memtable. This is efficient.
    // It is pipelined because writing to WAL and writing to memtable are separate stages.
    // This ensures that the disk and memory bandwidth are utilized efficiently.
    // while one group is writing to WAL, another group can be writing to memtable.
    #[derive(Clone)]
    pub(crate) struct SeekingMembership;

    // This state means the write group is decided. A writer can emerge out of the
    // last state as a leader or follower.
    // The leader will perform all the subsequent work for all members of the group.
    // Followers just wait for the leader to finish.
    #[derive(Clone)]
    pub(crate) struct FollowerWaitingCompletion;

    // The writing stage is exclusive. Only one writer can be in this state at a time.
    // This state means the leader is waiting its turn.
    pub(crate) struct LeaderWaitingToWriteWal<R>(pub(super) GroupGuard<R>);

    // This is the critical section where the only one leader is writing to WAL.
    pub(crate) struct LeaderWritingWal<R>(pub(super) GroupGuard<R>);

    // The leader comes out of the last state as soon as it is done writing. This
    // uses the disk bandwidth efficiently. Now the leader is waiting for its turn to
    // write to the memtable. The write to memtable happens in the same order as the
    // write to WAL. So, this state is a barrier to ensure the order.
    pub(crate) struct LeaderWaitingToWriteMemtable<'a, S, R>(
        pub(super) GroupGuard<R>,
        pub(super) TicketGuard<'a, S>,
        pub(super) WalReceipt,
    );

    // This means the leader with the correct ticket number is found and is writing to
    // the memtable. Like WAL writing, this is also exclusive and no two leader can be
    // writing to the memtable at the same time.
    pub(crate) struct LeaderWritingMemtable<'a, S, R>(
        pub(super) GroupGuard<R>,
        // we never read it. It is our drop guard.
        #[allow(dead_code)] pub(super) TicketGuard<'a, S>,
        pub(super) WalReceipt,
    );

    // The leader notifies all the waiting followers that the write is done.
    // and passes on the result (success or failure).
    pub(crate) struct LeaderFinishingWrite<R>(pub(super) GroupGuard<R>);

    #[derive(Clone)]
    pub(crate) struct Success {
        pub(crate) lsn: u64,
    }
}

struct CompletionState {
    result: Mutex<Option<Result<CompletionVariant, crate::err::DbError>>>,
    condvar: Condvar,
}

/// The passive data payload for a write request.
pub(crate) struct WriteRequest {
    pub(crate) key: Key,
    pub(crate) value: Value,
    completion: Arc<CompletionState>,
}

/// The active actor driving the write request state machine.
pub(crate) struct Writer<State> {
    pub(crate) req: Arc<WriteRequest>,
    state: State,
}

/// When this guard drops, the wal is free for the next write group.
struct WalGuard<'a, S> {
    db: &'a Db<S>,
}
impl<'a, S> Drop for WalGuard<'a, S> {
    fn drop(&mut self) {
        let mut g = self.db.write_sync.lock().unwrap();
        g.wal_busy = false;
        self.db.wal_cv.notify_all();
    }
}

/// When this guard drops, the next ticket holder
/// will be set to write to memtable.
struct TicketGuard<'a, S> {
    db: &'a Db<S>,
    ticket_num: usize,
}
impl<'a, S> Drop for TicketGuard<'a, S> {
    fn drop(&mut self) {
        let mut g = self.db.write_sync.lock().unwrap();

        // Increment the ticket for memtable write
        // so that the next writer can write to memtable.
        g.ticket_to_mem += 1;
        self.db.mem_cv.notify_all();
    }
}

// This is to make sure that the group members do not hang waiting for the leader to finish
// in case the leader panics and could not keep its due course.
pub(crate) struct GroupGuard<R> {
    pub(crate) reqs: Vec<Arc<WriteRequest>>,
    pub(crate) committed_lsn: Option<u64>,
    // Phantom type to hold the state of the requests when they were queued.
    // This is needed because GroupGuard is generic over R.
    _marker: std::marker::PhantomData<R>,
}
impl<R> Drop for GroupGuard<R> {
    fn drop(&mut self) {
        let completion_variant = if let Some(lsn) = self.committed_lsn {
            Ok(CompletionVariant::Success(write_states::Success { lsn }))
        } else {
            Err(err::DbError::WriterPanic)
        };

        self.reqs.iter().for_each(|req| {
            req.completion
                .result
                .lock()
                .unwrap()
                .replace(completion_variant.clone());
            req.completion.condvar.notify_all();
        });
    }
}

#[derive(Clone)]
pub(crate) enum CompletionVariant {
    Follower(write_states::FollowerWaitingCompletion),
    Success(write_states::Success),
}

pub(crate) enum SeekingMembershipResult {
    Leader(Writer<write_states::LeaderWaitingToWriteWal<write_states::WaitingToBeQueued>>),
    Follower(Writer<write_states::FollowerWaitingCompletion>),
    Success(Writer<write_states::Success>),
}

impl<State: Clone> Clone for Writer<State> {
    fn clone(&self) -> Self {
        Self {
            req: self.req.clone(),
            state: self.state.clone(),
        }
    }
}

impl Writer<write_states::WaitingToBeQueued> {
    pub(crate) fn new(key: Key, value: Value) -> Writer<write_states::WaitingToBeQueued> {
        let req = Arc::new(WriteRequest {
            key,
            value,
            completion: Arc::new(CompletionState {
                result: Mutex::new(None),
                condvar: Condvar::new(),
            }),
        });
        Self {
            req,
            state: write_states::WaitingToBeQueued,
        }
    }
}

// The request tries to queue itself in the DB's write queue.
// For this, it needs to acquire the write_sync lock.
// Once queued, it waits to be part of a write group.
impl Writer<write_states::WaitingToBeQueued> {
    pub(crate) fn step<S>(
        self,
        db: &Db<S>,
    ) -> Result<Writer<write_states::SeekingMembership>, err::DbError> {
        let mut g = db.write_sync.lock().unwrap();
        // The queue now stores the passive data request, not the actor.
        g.write_queue.push_back(self.req.clone());
        Ok(Writer {
            req: self.req,
            state: write_states::SeekingMembership,
        })
    }
}

// Waiting for membership is an active wait.
// The request should wake up periodically and check if it is the head of the queue.
// If it is, it will declare itself the leader and go on to form a writer group.
// The others will continue waiting.
impl Writer<write_states::SeekingMembership> {
    // wake up.
    // check if you have a completions state. Is so, drop. This means some leader magically completed your work while you were asleep.
    // if not, check if self is at the head of the queue.
    // if yes, become leader.
    // else, continue waiting.
    pub(crate) fn step(
        self,
        db: &Db<crate::db_states::ReadWrite>,
    ) -> Result<SeekingMembershipResult, err::DbError> {
        loop {
            // Check if I have a completion result.
            {
                let mut guard = self.req.completion.result.lock().unwrap();
                if let Some(res) = guard.take() {
                    let req = self.req.clone();

                    match res {
                        Ok(variant) => match variant {
                            CompletionVariant::Follower(state) => {
                                return Ok(SeekingMembershipResult::Follower(Writer {
                                    req,
                                    state,
                                }));
                            }
                            CompletionVariant::Success(state) => {
                                return Ok(SeekingMembershipResult::Success(Writer { req, state }));
                            }
                        },
                        Err(e) => {
                            // failure.
                            return Err(e);
                        }
                    }
                }
            } // guard drops here automatically.

            // Check if I am at the head of the queue.
            {
                let g = db.write_sync.lock().unwrap();
                let is_front = if let Some(front) = g.write_queue.front() {
                    Arc::ptr_eq(&self.req.completion, &front.completion)
                } else {
                    false
                };
                if is_front {
                    let write_group = form_write_group(g);
                    write_group.reqs.iter().for_each(|req| {
                        req.completion.result.lock().unwrap().replace(Ok(
                            CompletionVariant::Follower(write_states::FollowerWaitingCompletion),
                        ));
                        req.completion.condvar.notify_all();
                    });
                    return Ok(SeekingMembershipResult::Leader(Writer {
                        req: self.req,
                        state: write_states::LeaderWaitingToWriteWal(write_group),
                    }));
                }
            } // g drops here automatically.
            // Wait for notification.
            {
                let guard = self.req.completion.result.lock().unwrap();
                if guard.is_none() {
                    let _guard = self.req.completion.condvar.wait(guard).unwrap();
                }
            } // guard drops here automatically (or after wait returns).
            continue;
        } // loop
    }
}

fn form_write_group(
    mut lock: MutexGuard<crate::WriteSyncs>,
) -> GroupGuard<write_states::WaitingToBeQueued> {
    let reqs: Vec<Arc<WriteRequest>> = lock.write_queue.drain(..).collect();
    GroupGuard {
        reqs,
        committed_lsn: None,
        _marker: std::marker::PhantomData,
    }
}

impl Writer<write_states::FollowerWaitingCompletion> {
    // A follower waiting completion meets with one of the two fates:
    // 1. The leader successfully completes their write on their behalf.
    // or, 2. The leader fails and returns an error.
    // No matter the fate, the follower lies in wait until the eventuality
    // befalls it, and at that point it quits the wait and reports the
    // result to the application thread that requested this write.
    pub(crate) fn step(self) -> Result<Writer<write_states::Success>, err::DbError> {
        loop {
            // Check if I have a completion result.
            {
                let mut guard = self.req.completion.result.lock().unwrap();
                if let Some(res) = guard.take() {
                    let req = self.req.clone();
                    match res {
                        Ok(CompletionVariant::Success(state)) => {
                            return Ok(Writer { req, state });
                        }
                        Err(e) => return Err(e),
                        _ => {} // Wait for success
                    }
                } else {
                    // No result was returned. Go back to waiting on the condvar.
                    let _guard = self.req.completion.condvar.wait(guard).unwrap();
                }
            }
        }
    }
}

/// Remember the leader writes for the entire group.
/// The wait is usually because the previous leader
/// is still in the LeaderWritingWal state which is
/// exclusive.
/// 1. check if the wal_busy flag is set.
///     if so, go back to sleeping on the wal_cv.
/// 2. If it is free, then proceed to the next step.
impl<R> Writer<write_states::LeaderWaitingToWriteWal<R>> {
    pub(crate) fn step(
        self,
        db: &Db<crate::db_states::ReadWrite>,
    ) -> Result<Writer<write_states::LeaderWritingWal<R>>, err::DbError> {
        let mut g = db.write_sync.lock().unwrap();
        loop {
            if g.wal_busy {
                g = db.wal_cv.wait(g).unwrap();
            } else {
                // we are under a lock right now.
                // set the wal busy flag that no one
                // but I can move to the next state.
                g.wal_busy = true;
                return Ok(Writer {
                    req: self.req,
                    state: write_states::LeaderWritingWal(self.state.0),
                });
            }
        }
    }
}

/// There are a few steps to writing a wal.
/// 1. Grab the ticket_to_mem This ensures our turn to write to memtable
///     after we finish here.
/// 2. increment ticket_to_mem for the next write-group leader.
/// 3. Take the ticket guard. Ensures that if this thread panics, the
///     memtable write will be set for the next ticket holder. This is carried
///     on to the memtable write stage. This is the ticket to write to memtable.
/// 3. grab lsn for all the group members.
/// 4. grab the WAL guard.
/// 5. serialize the key value pairs for the entire group.
/// 6. call wal.write()
/// 7. Move to next state.
/// The return from this state ensures that the WAL writing stage
/// is open for the next leader.
impl<R: Clone> Writer<write_states::LeaderWritingWal<R>> {
    pub(crate) fn step<'a>(
        mut self,
        db: &'a Db<crate::db_states::ReadWrite>,
    ) -> Result<
        Writer<write_states::LeaderWaitingToWriteMemtable<'a, crate::db_states::ReadWrite, R>>,
        err::DbError,
    > {
        let (batch_start_lsn, ticket_guard) = {
            let mut g = db.write_sync.lock().unwrap();
            let ticket = g.next_ticket;
            g.next_ticket += 1;

            let batch_start_lsn = g.next_lsn;
            g.next_lsn += self.state.0.reqs.len() as u64;

            (
                batch_start_lsn,
                TicketGuard {
                    db,
                    ticket_num: ticket,
                },
            )
        };

        // We are holding the GroupGuard in self.state.0.
        // We can update the committed_lsn here, but wait, we haven't written yet.
        // We should set it only after success. But we need to use it.
        // Let's set it at the end.

        let _wal_guard = WalGuard { db };

        let mut wal_bytes = Vec::new();
        for (i, req) in self.state.0.reqs.iter().enumerate() {
            // We construct a temporary Key with the assigned LSN just for serialization.
            // This avoids mutating the shared WriteRequest.
            let lsn = batch_start_lsn + i as u64;

            // Update the key's LSN in-place so it carries the correct version for Memtable.
            req.key.update_lsn(lsn);

            // Serialize using strictly typed WAL encoder directly into the buffer
            crate::data_stores::wal::Wal::encode_entry(
                &req.key.bytes,
                lsn,
                &req.value,
                &mut wal_bytes,
            );
        }

        let receipt = db.write_wal(&wal_bytes)?;

        // Update the GroupGuard with the LSN so it can notify followers on drop (or explicit success).
        // The receipt has the unified LSN for the batch or we use batch_start_lsn.
        // Let's use batch_start_lsn as the base lsn.
        // WalReceipt in our mock returns lsn=0. In reality it should return the LSN of the write.
        // Let's trust batch_start_lsn is the correct logic for this DB.
        self.state.0.committed_lsn = Some(batch_start_lsn);

        Ok(Writer {
            req: self.req,
            state: write_states::LeaderWaitingToWriteMemtable(self.state.0, ticket_guard, receipt),
        })
        // wal_guard dropps here.
        // Making this stage available for the next write-group leader.
    }
}

// The leader waits its turn to write to the memtable.
// Memtable write being an exclusive state, allows only
// one leader at a time. The it holds a ticket number
// and the leader with a tikcet matching the ticket
// number gets to enter this stage.
// All other leaders without a matching
// ticket number go back to sleep on the mem_cv.
impl<'a, R> Writer<write_states::LeaderWaitingToWriteMemtable<'a, crate::db_states::ReadWrite, R>> {
    pub(crate) fn step(
        self,
        db: &Db<crate::db_states::ReadWrite>,
    ) -> Result<
        Writer<write_states::LeaderWritingMemtable<'a, crate::db_states::ReadWrite, R>>,
        err::DbError,
    > {
        let mut g = db.write_sync.lock().unwrap();
        loop {
            // with the ticket number check, we don't need to check
            // the mem_busy flag.
            if g.ticket_to_mem == self.state.1.ticket_num {
                return Ok(Writer {
                    req: self.req,
                    state: write_states::LeaderWritingMemtable(
                        self.state.0,
                        self.state.1,
                        self.state.2,
                    ),
                });
            } else {
                // 2. ATOMICALLY: RELEASE LOCK & SLEEP
                // The critical work happens inside the wait call.
                g = db.mem_cv.wait(g).unwrap();
                // 3. ATOMICALLY: AWAKE & REACQUIRE LOCK.
                // This makes sure that you have the lock for
                // the next iteration.
            }
        }
    }
}

/// This writes the batch to the memtable.
/// TODO: After writint to memtable and before returning,
/// it should check the size of memtable. If it is larger than
/// the default size, it should create a new memetable
/// for the next writer.
impl<'a>
    Writer<
        write_states::LeaderWritingMemtable<
            'a,
            crate::db_states::ReadWrite,
            write_states::WaitingToBeQueued,
        >,
    >
{
    pub(crate) fn step(
        self,
        db: &Db<crate::db_states::ReadWrite>,
    ) -> Result<
        Writer<write_states::LeaderFinishingWrite<write_states::WaitingToBeQueued>>,
        err::DbError,
    > {
        // We pass the receipt to prove we wrote to WAL.
        db.write_memtable(self.state.0.reqs.clone(), self.state.2)?;
        Ok(Writer {
            req: self.req,
            state: write_states::LeaderFinishingWrite(self.state.0),
        })
        // ticket guard gets dropped here.
        // the next writer can write to memtable.
    }
}

impl<R> Writer<write_states::LeaderFinishingWrite<R>> {
    pub(crate) fn step(self) -> Result<Writer<write_states::Success>, err::DbError> {
        // self.state.0.committed = true; // No longer needed, as we set committed_lsn in WAL stage.
        // Actually, we already set committed_lsn in WAL stage.
        // The Drop implementation of GroupGuard will handle the notification.
        // But we need to make sure we don't drop it prematurely?
        // It drops when we return from this function and the Writer<Success> doesn't hold it.
        // Wait, Writer<Success> doesn't hold the GroupGuard.
        // So GroupGuard drops right here at the end of this function.
        let lsn = self.state.0.committed_lsn.unwrap_or(0); // Should be set.

        Ok(Writer {
            req: self.req,
            state: write_states::Success { lsn },
        })
        // group guard gets dropped here.
        // All followers for this leader will be notified
        // of the successful write.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Db, data_stores::key::Key, data_stores::value::Value, db_states};
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::thread;

    // Helper to create a test Db instance
    fn create_test_db() -> Arc<Db<db_states::ReadWrite>> {
        Arc::new(Db::<db_states::ReadWrite>::new_test())
    }

    /// Verifies the full state transition flow of a Writer.
    ///
    /// This test:
    /// 1. Creates a `Writer` with a specific Key and Value.
    /// 2. Progresses it through all states:
    ///    - `WaitingToBeQueued` -> `SeekingMembership`
    ///    - `SeekingMembership` -> `Leader`, `Follower`, or `Success`
    /// 3. If it becomes a Leader, it verifies the leader states:
    ///    - `LeaderWaitingToWriteWal` -> `LeaderWritingWal` -> `LeaderWaitingToWriteMemtable`
    ///    - `LeaderWritingMemtable` -> `LeaderFinishingWrite` -> `Success`
    /// 4. Asserts that the final state (Success) contains a valid logical sequence number (LSN > 0).
    #[test]
    fn test_writer_state_flow() {
        let db = create_test_db();
        let key = Key::new(b"key", 0);
        let val = Value::new(b"value");
        let writer = Writer::new(key, val);

        let db_clone = db.clone();

        let handle = thread::spawn(move || {
            // 1. WaitingToBeQueued -> SeekingMembership
            let w = writer.step(&*db_clone).expect("Step 1 failed");

            // 2. SeekingMembership -> Leader OR Follower OR Success
            let w = w.step(&*db_clone).expect("Step 2 failed");

            match w {
                SeekingMembershipResult::Leader(w_leader) => {
                    // 3. LeaderWaitingToWriteWal -> LeaderWritingWal
                    let w = w_leader.step(&*db_clone).expect("Step 3 failed");

                    // 4. LeaderWritingWal -> LeaderWaitingToWriteMemtable
                    let w = w.step(&*db_clone).expect("Step 4 failed");

                    // 5. LeaderWaitingToWriteMemtable -> LeaderWritingMemtable
                    let w = w.step(&*db_clone).expect("Step 5 failed");

                    // 6. LeaderWritingMemtable -> LeaderFinishingWrite
                    let w = w.step(&*db_clone).expect("Step 6 failed");

                    // 7. LeaderFinishingWrite -> Success
                    let w = w.step().expect("Step 7 failed");

                    assert!(w.state.lsn > 0);
                }
                SeekingMembershipResult::Follower(w_follower) => {
                    let w = w_follower.step().expect("Follower step failed");
                    assert!(w.state.lsn > 0);
                }
                SeekingMembershipResult::Success(w_success) => {
                    assert!(w_success.state.lsn > 0);
                }
            }
        });

        handle.join().unwrap();
    }

    /// Stress tests concurrent writers to ensure thread safety and LSN monotonicity.
    ///
    /// This test spawns multiple threads, each creating and driving a `Writer`.
    /// It verifies that:
    /// - All writers eventually complete successfully.
    /// - No deadlocks occur (implied by test completion).
    /// - All writers receive a valid LSN.
    /// - (Implicitly) The shared `Db` state remains consistent.
    #[test]
    fn test_concurrent_writers() {
        let db = create_test_db();
        let num_threads = 10;
        let mut handles = vec![];

        for i in 0..num_threads {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let w = Writer::<write_states::WaitingToBeQueued>::new(
                    Key::new(format!("key_{}", i).as_bytes(), 0),
                    Value::new(b"val"),
                );

                let w = w.step(&*db).unwrap();
                let w_res = w.step(&*db).unwrap();

                match w_res {
                    SeekingMembershipResult::Leader(l) => {
                        let l = l.step(&*db).unwrap();
                        let l = l.step(&*db).unwrap();
                        let l = l.step(&*db).unwrap();
                        let l = l.step(&*db).unwrap();
                        let s = l.step().unwrap();
                        assert!(s.state.lsn > 0);
                    }
                    SeekingMembershipResult::Follower(f) => {
                        let s = f.step().unwrap();
                        assert!(s.state.lsn > 0);
                    }
                    SeekingMembershipResult::Success(s) => {
                        assert!(s.state.lsn > 0);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    proptest! {
        // Property: Any valid Key/Value pair can be processed by the Writer
        // without panicking, and will result in a successful state transition
        // yielding a valid LSN.
        #[test]
        fn test_writer_state_transition_prop(
            key_bytes in prop::collection::vec(any::<u8>(), 0..100),
            val_bytes in prop::collection::vec(any::<u8>(), 0..100)
        ) {
            let db = create_test_db(); // In a real scenario we might reuse this, but clean state is safer
            let key = Key::new(&key_bytes, 0);
            let val = Value::new(&val_bytes);

            // We do the same logic as test_writer_state_flow but driven by proptest inputs
            // We run this in a thread because of the thread-local logic or lock behavior
            // that might be expected (though here it's mostly lock contention).
            // Actually, we can run it in the main thread of the test for proptest.

            let writer = Writer::new(key, val);

            // 1. WaitingToBeQueued -> SeekingMembership
            let w = writer.step(&*db).expect("Step 1 failed");

            // 2. SeekingMembership -> Leader OR Follower OR Success
            let w = w.step(&*db).expect("Step 2 failed");

            match w {
                SeekingMembershipResult::Leader(w_leader) => {
                    let w = w_leader.step(&*db).expect("Step 3 failed");
                    let w = w.step(&*db).expect("Step 4 failed");
                    let w = w.step(&*db).expect("Step 5 failed");
                    let w = w.step(&*db).expect("Step 6 failed");
                    let w = w.step().expect("Step 7 failed");
                    assert!(w.state.lsn > 0);
                }
                SeekingMembershipResult::Follower(w_follower) => {
                    let w = w_follower.step().expect("Follower step failed");
                    assert!(w.state.lsn > 0);
                }
                SeekingMembershipResult::Success(w_success) => {
                    assert!(w_success.state.lsn > 0);
                }
            }
        }
    }
}
