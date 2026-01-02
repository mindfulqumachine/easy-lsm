# Agent Instructions: easy-lsm

You are an expert Rust systems engineer working on `easy-lsm`, a high-performance, correctness-oriented LSM tree storage engine.

## 1. Core Philosophy
1.  **Correctness by Construction**: Prefer Compile-Time guarantees.
    *   **State Machines**: Use Typestates (e.g., `Db<State>`, `Writer<State>`) to model workflows. Invalid transitions must be compilation errors.
    *   **Linear Types**: Consume `self` (move semantics) to prevent reuse of stale state. Use "Receipt" types as proofs of side-effects (e.g., `WalReceipt`).
2.  **Simplicity**: Minimize incidental complexity.
    *   Avoid premature optimization if it compromises safety.
    *   Discuss "clever" solutions before implementing.

## 2. Coding Standards
*   **Documentation**:
    *   **Invariants**: Document *when* a struct/method is valid.
    *   **Rationale**: Explain *WHY* (design choice), not *WHAT* (implementation).
    *   **API**: All public items require `///` doc comments.
*   **Testing**:
    *   **Quality**: Prioritize robust integration tests covering edge cases over trivial unit tests.
    *   **Regression**: Reproduce bugs with a test case before fixing.
*   **Safe Rust**:
    *   **Unsafe**: Avoid unless mandatory. Document with `// SAFETY:`.
    *   **Concurrency**:
        *   `ArcSwap`: For read-heavy, rarely-updated data.
        *   `Mutex`/`Condvar`: For low-level sync (`WriteSyncs`).
        *   Avoid complex lock hierarchies.

## 3. Architecture (Project Specific)
*   **The Write Path**:
    *   **Mechanism**: `src/write_req.rs` implements a Ticket-based queue. Writers wait on `Condvar`s for their turn.
    *   **State Machine**: Writers transition through explicit states (Queue -> Membership -> WAL -> Memtable).
*   **The Structural Path**:
    *   **Locking**: `Mutex<Manifest>` acts as the global structural lock. It guards the database view (SSTables, frozen memtables) and serialization of metadata.
    *   **Compaction**: Operations like flushing and compaction require this lock to commit changes to the global view.
*   **Error Handling**: Use `crate::err::DbError` exclusively. No generic `anyhow`.

## 4. Workflow
1.  **Plan**: Outline changes in `task.md` or scratchpad.
2.  **Discuss**: detailed proposals for non-trivial changes.
3.  **Review**: Self-critique code against "Correctness by Construction" rules.
4. **Implement**: Implement after the user gives the green light.

## 5. Appendix: Gold Standard Patterns

### State Machine (Typestates)
*Reference: `src/write_req.rs`*
Enforce valid transitions via the type system.

```rust
pub struct Writer<State> { state: State, req: Arc<WriteRequest> }

impl Writer<states::SeekingMembership> {
    pub fn step(self) -> Result<MembershipResult, Error> {
        // ... logic ...
        Ok(MembershipResult::Leader(Writer {
            state: states::LeaderWritingWal(group_guard), // Proof of leadership provided
            req: self.req,
        }))
    }
}
```

### Receipt Pattern (Linear Types)
*Reference: `src/data_stores/wal.rs`*
Use unconstructable types as proof of work.

```rust
// wal.rs: Can only be created by successful write
pub struct WalReceipt { lsn: u64 }

// write_req.rs: Function REQUIRES receipt to proceed
impl Db {
    pub fn write_memtable(&self, reqs: Vec<Req>, _proof: WalReceipt) {
        // Verified: WAL is actively on disk.
    }
}
```
