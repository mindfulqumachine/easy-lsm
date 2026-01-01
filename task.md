Goal: To implement the WAL writes to disk.

Context:
1. README.md: For overall project description.
2. wal.rs
3. key.rs
4. value.rs

Task details:
1. I want to @wal.rs#L14 fn write().
    When complete it should write the bytes to the WAL file.
Verification:
    - A test that writes multiple key / values pairs to the WAL file. Then closes and reopens the WAL and reads it. The key value / pairs should be the same as written.

2. From the test description, it is clear we also need to implement the WAL read.

3. Key implementation, both the memory and the on-disk representation has to be implemented.
    look at the README's internal key-format and internal-value-format sections for more details.
    Also look at the structures on disk section's WAL sub-section for more details.

## General instructions.
1. Please discuss your thoughs before starting to implement.
2. The implementation should be safe by construction. reference write_req.rs for an example of correct by construction implementation. Your implementation has to be better than that in 
terms of correctness and clarity of implementation.
3. Please document the code and tests well.

    
