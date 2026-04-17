# Distributed Collaborative Editor

This is a distributed collaborative text editor implementing Operational Transformation (OT) on top of the Raft consensus algorithm. It ensures high availability, strong consistency, and correct convergence of concurrent edits even in the face of network latency or server crashes.

Based on the EasySync algorithm and embedding OT within the Raft state machine to solve the consensus/convergence problem gracefully.

## Architecture

1. **Client-Side OT**: A JavaScript client (`web/client.js`) maintains state `A` (server state), `X` (pending changes), and `Y` (unsubmitted changes).
2. **WebSocket Interface**: Clients communicate with the backend via WebSocket messages (`CONNECT`, `SUBMIT`, `ACK`, `BROADCAST`, `REDIRECT`).
3. **Raft Consensus**: Every server node runs `hashicorp/raft`. Raw submissions are placed into the Raft log.
4. **Deterministic State Machine**: When an entry commits in Raft, every node executes the exact same state machine `apply` function, performing OT mathematically to achieve convergence deterministically.
5. **Storage**: Raft log entries and metadata are stored persistently on disk using BoltDB.

## Requirements

- Go 1.21+
- Node/Browser for testing

## Building

To build the server executable:

```bash
go build -o server.exe ./cmd/server
```

## Running a Cluster Locally

You can spin up a 3-node cluster locally using different ports.

**Node 1 (Leader/Bootstrapper):**
```bash
go run ./cmd/server -id node-1 -grpc-addr localhost:12000 -ws-addr localhost:8080 -data-dir ./data/node-1 -peers localhost:12000,localhost:12001,localhost:12002 -bootstrap -init-text "Welcome to the Distributed Editor!"
```

**Node 2 (Follower):**
```bash
go run ./cmd/server -id node-2 -grpc-addr localhost:12001 -ws-addr localhost:8081 -data-dir ./data/node-2 -peers localhost:12000,localhost:12001,localhost:12002
```

**Node 3 (Follower):**
```bash
go run ./cmd/server -id node-3 -grpc-addr localhost:12002 -ws-addr localhost:8082 -data-dir ./data/node-3 -peers localhost:12000,localhost:12001,localhost:12002
```

Once running, you can connect clients by opening multiple browser tabs to any of the servers:
- `http://localhost:8080`
- `http://localhost:8081`
- `http://localhost:8082`

The follower nodes will correctly respond to client HTTP, provide catch-up connections, and `REDIRECT` writes to the leader.

## Clean Up
To start fresh, delete the data directory:

```bash
rm -rf ./data
```

plan

## The Central Insight: OT Embedded Inside the Raft State Machine

The reason you kept failing to find a consensus-free approach is exactly correct: you *do* need consensus, but not for the reason most people think. You don't need consensus to resolve conflicting edits (OT handles that). You need consensus to establish a **globally agreed-upon, append-only revision log**. Two servers must never disagree about what revision 47 is. That is the single invariant that makes everything else work.

The elegant solution is to make the OT transformation layer *part of the Raft state machine's `apply()` function*. Here is why this is so powerful: Raft guarantees that every node applies log entries in the same order. The `follow()` function is a pure, deterministic function with no side effects. Therefore, if every node receives the same sequence of `(client_id, base_rev, raw_changeset)` entries and applies the same deterministic `follow()` chain, every node reaches the exact same document state independently. You get both consensus and convergence without any coordination overhead beyond the Raft protocol itself.

Let me make this concrete before anything else.

```python
# This is what gets stored in the Raft log — the RAW, untransformed client submission.
# Note: NOT the transformed changeset. The transformation happens during apply().
class RaftLogEntry:
    client_id: UUID          # who submitted this
    submission_id: int       # monotonically increasing per client (for deduplication)
    base_rev: int            # which server revision the client's document was based on
    changeset: Changeset     # the raw changeset relative to base_rev

# This is the Raft state machine's state.
class DocumentStateMachine:
    head_rev: int                          # current revision number
    revision_log: list[RevisionRecord]     # append-only, persisted to disk
    head_text: str                         # current full text (a cache, derivable from log)
    client_last_submission: dict[UUID, int] # for deduplication

    def apply(self, entry: RaftLogEntry) -> Changeset | None:
        """Called by Raft on every node when an entry is committed.
        Returns the transformed changeset C', or None if duplicate."""

        # Idempotency guard: if we've already applied this submission, ignore it.
        last = self.client_last_submission.get(entry.client_id, -1)
        if entry.submission_id <= last:
            return None  # duplicate — leader crashed after committing but before ACKing

        C = entry.changeset

        # The OT transformation: catch C up from base_rev to head_rev.
        # This is deterministic — same entry always produces same C'.
        for r in range(entry.base_rev, self.head_rev):
            historical_changeset = self.revision_log[r].changeset
            C = follow(historical_changeset, C)

        # C is now C', relative to head_rev. Apply it.
        self.head_text = apply_changeset(self.head_text, C)
        self.revision_log.append(RevisionRecord(
            changeset=C,
            source=entry.client_id,
            rev_number=self.head_rev
        ))
        self.head_rev += 1
        self.client_last_submission[entry.client_id] = entry.submission_id

        return C  # Return C' so the server can broadcast it to connected clients
```

This is the entire heart of the system. Every server runs this exact same `apply()` function on every committed Raft entry, and they all produce the same `C'`, update to the same `head_text`, and increment to the same `head_rev`. The OT mathematics guarantees convergence; Raft guarantees ordering and fault tolerance.

Now let me draw the full architecture before going deeper.Now let me go through every layer of this system in complete detail.

---

## Part 1: The Raft State Machine in Full Detail

The most important design decision is what you put *in* the Raft log versus what the state machine computes. Many students make the mistake of trying to put the already-transformed `C'` into the Raft log. That is wrong, and here is why: if the leader transforms `C` to `C'` and then crashes after committing to a majority but before responding to the client, the client will re-submit `C` again. But if the log already contains `C'` (the transformed version), applying the identity-checked `C` a second time will transform it *again*, producing a wrong result. You must store the raw `(client_id, base_rev, C)` in the log, with the transformation happening deterministically in `apply()`, protected by the idempotency guard using `submission_id`.

The full `apply()` function I showed above has one subtle property worth dwelling on: the `follow()` loop iterates over `range(base_rev, head_rev)`. If a client submits a changeset based on revision 5 and the server is currently at revision 8, the loop runs `follow(rev5_changeset, C)`, then `follow(rev6_changeset, result)`, then `follow(rev7_changeset, result)`. This is the exact algorithm from the EasySync specification's Section 14.2, now running inside a replicated state machine. The mathematical convergence guarantee (`Af(A,B) = Bf(B,A)`) means that no matter which order two raw entries arrive and get committed by Raft, every server that applies both of them ends up with the same `head_text`. You should be able to prove this to your professor on demand.

---

## Part 2: The Network Protocol Stack

You need two completely separate communication channels, and they must not be confused.

**Server-to-server (Raft transport):** Use gRPC. This carries Raft RPCs: `AppendEntries` (the heartbeat and log replication call) and `RequestVote` (used during leader elections). gRPC gives you a well-defined schema, bidirectional streaming, and built-in connection management — exactly what Raft's transport layer needs. Every `AppendEntries` call carries the `RaftLogEntry` payload (the raw `client_id`, `base_rev`, and `changeset`). The recommended Raft library for Go is `hashicorp/raft`; for Python it is `pysyncobj`. Both allow you to supply your own gRPC transport.

**Client-to-server (collaboration protocol):** Use WebSockets. A WebSocket gives you the stateful, ordered, bidirectional channel that EasySync assumes. The six message types you need are:

```
CONNECT  → client sends: {client_id, last_known_rev}
           server replies: {head_text, head_rev}           ← reconnect catch-up

SUBMIT   → client sends: {client_id, submission_id, base_rev, changeset}
           (submission_id is a monotonically increasing int per client)

ACK      → server sends: {new_rev}                        ← your X is now part of A

BROADCAST→ server sends: {changeset, new_rev}             ← another client's commit

REDIRECT → server sends: {leader_addr}                    ← follower telling you to
                                                             go to the leader for writes
ERROR    → server sends: {code, message}
```

Notice the `REDIRECT` message. This is important. Clients can connect to any server — this is how you keep availability high even when the leader changes. But only the leader can propose to Raft. So when a follower receives a `SUBMIT` from a connected client, it has two options: forward the submission to the current leader on the client's behalf (transparent proxy), or send a `REDIRECT` and make the client reconnect to the leader. The forwarding approach is more seamless for the client but slightly more complex to implement. The redirect approach is simpler and equally correct. Start with redirect for your prototype and upgrade to forwarding later.

---

## Part 3: The Complete Client State Machine

The client state machine is almost exactly the EasySync specification, with two additions: it tracks `server_rev` (the revision its `A` state is based on), and it assigns `submission_id` to each submission for deduplication.

```python
class Client:
    def __init__(self, document_text, initial_rev):
        self.client_id = generate_uuid()
        self.A = text_to_changeset(document_text)
        self.X = IdentityChangeset()
        self.Y = IdentityChangeset()
        self.server_rev = initial_rev
        self.next_submission_id = 0
        self.waiting_for_ack = False

    def on_local_edit(self, edit_E):
        # User typed something. Fold it into Y.
        self.Y = compose(self.Y, edit_E)
        # Immediately show it on screen — never block the user.

    def on_submit_timer(self, network):
        # Called every ~500ms. Only one outstanding submission at a time.
        if self.waiting_for_ack or is_identity(self.Y):
            return
        self.X = self.Y
        self.Y = IdentityChangeset()
        self.waiting_for_ack = True
        network.send('SUBMIT', {
            'client_id': self.client_id,
            'submission_id': self.next_submission_id,
            'base_rev': self.server_rev,
            'changeset': self.X
        })
        self.next_submission_id += 1

    def on_ack(self, new_rev):
        # Server committed our X. It's now canonical history.
        self.A = compose(self.A, self.X)
        self.X = IdentityChangeset()
        self.server_rev = new_rev
        self.waiting_for_ack = False

    def on_broadcast(self, incoming_changeset, new_rev):
        # Another client's changeset was committed. Update our local state.
        B = incoming_changeset
        A_prime = compose(self.A, B)
        X_prime = follow(B, self.X)
        Y_prime = follow(follow(self.X, B), self.Y)
        D = follow(self.Y, follow(self.X, B))
        # D is what we apply to the screen to show the remote edit visually.
        apply_to_screen(D)
        self.A, self.X, self.Y = A_prime, X_prime, Y_prime
        self.server_rev = new_rev

    def on_reconnect(self, new_head_text, new_head_rev):
        # We disconnected and reconnected. The server sends us its current state.
        # Our old A, X, Y are now stale. We rebuild from scratch.
        # X and Y represent local changes we haven't pushed yet.
        # We must fold them on top of the fresh server state.
        # Since X was never ACKed, it is still pending. Y was never submitted.
        # Merge our pending local work on top of the new server state.
        new_A = text_to_changeset(new_head_text)
        # We need to transform X and Y against everything that happened
        # between our old server_rev and new_head_rev.
        # The server sends us the catch-up changesets for that range.
        # Those will arrive as a series of BROADCAST messages before CONNECT completes.
        self.A = new_A
        self.server_rev = new_head_rev
        # X and Y will be re-submitted normally after reconnect.
        self.waiting_for_ack = False
```

The reconnect case deserves special attention because this is precisely what your professor cares about — the "no going back in time" guarantee. When a client reconnects, the server sends `(head_text, head_rev)` in the `CONNECT` response. The client sets `A = head_text` and `server_rev = head_rev`. The client's `X` (unACKed submission) is then re-submitted with the *new* `base_rev`. The server's state machine will transform it forward from the new `base_rev`, which is correct. The client's `Y` (not yet submitted) will be submitted in the next 500ms timer tick. The key guarantee is: the client never sees an acknowledged change disappear. Anything already in `A` was committed to the Raft log and will survive any single server failure.

---

## Part 4: The Server-Side Handler

Each server runs two independent processes: its Raft engine (which handles replication) and its WebSocket handler (which talks to clients). They communicate through the state machine's `apply()` callback.

```python
class Server:
    def __init__(self, raft_node, state_machine):
        self.raft = raft_node                    # the Raft library instance
        self.sm = state_machine                  # our DocumentStateMachine
        self.connected_clients = {}              # client_id → WebSocket connection
        self.raft.set_apply_callback(self.on_commit)

    def on_client_connect(self, ws, client_id, last_known_rev):
        # Send catch-up: all revisions from last_known_rev to head_rev.
        catch_up = self.sm.revision_log[last_known_rev:]
        ws.send('CONNECT', {
            'head_text': self.sm.head_text,
            'head_rev': self.sm.head_rev,
            'catch_up': catch_up     # client applies these as broadcasts to reconstruct A
        })
        self.connected_clients[client_id] = ws

    def on_client_submit(self, client_id, submission_id, base_rev, changeset):
        if not self.raft.is_leader():
            # Tell the client to go to the leader.
            ws = self.connected_clients[client_id]
            ws.send('REDIRECT', {'leader_addr': self.raft.current_leader()})
            return
        # Propose the raw entry to Raft.
        entry = RaftLogEntry(client_id, submission_id, base_rev, changeset)
        self.raft.propose(entry)  # This returns quickly; commit happens asynchronously.

    def on_commit(self, entry: RaftLogEntry):
        # This is called by the Raft library on EVERY NODE when an entry is committed.
        C_prime = self.sm.apply(entry)
        if C_prime is None:
            return  # duplicate — idempotency guard fired

        new_rev = self.sm.head_rev  # apply() already incremented this

        # Send ACK to the originating client if they are connected to THIS server.
        origin_ws = self.connected_clients.get(entry.client_id)
        if origin_ws:
            origin_ws.send('ACK', {'new_rev': new_rev})

        # Broadcast C_prime to all OTHER connected clients.
        for cid, ws in self.connected_clients.items():
            if cid != entry.client_id:
                ws.send('BROADCAST', {'changeset': C_prime, 'new_rev': new_rev})
```

Notice that `on_commit` runs on every node. This means that if Client A is connected to Server 1 and Client B is connected to Server 2, when Client A's change is committed: Server 1's `on_commit` sends the ACK to Client A and broadcasts to its locally connected clients. Server 2's `on_commit` — firing independently, through Raft log replication — broadcasts the same `C_prime` to Client B. Every server handles its own connected clients independently. This is a beautiful property of embedding OT in the state machine.

---

## Part 5: Fault Tolerance Analysis

This is what your professor will grill you on hardest. You need to be able to answer for every possible failure scenario.

**Scenario 1: A follower crashes.** With 3 servers, you need a quorum of 2. Losing 1 follower leaves 2 servers alive (the leader + 1 follower), which is still a majority. Raft continues operating normally. `AppendEntries` RPCs from the leader to the dead follower will time out, but the leader does not wait for all followers — it only waits for a majority to ACK. Clients connected to the dead follower will get a TCP disconnect and must reconnect to another server. On reconnect, they send their `last_known_rev` and receive catch-up revisions.

**Scenario 2: The leader crashes.** Followers stop receiving heartbeats (`AppendEntries` with empty entries that Raft sends periodically). After the election timeout (typically 150–300ms), one follower starts a new election, wins, and becomes the new leader. Any Raft log entries that the old leader had proposed but not yet committed (not yet replicated to a majority) are effectively lost — Raft's safety property guarantees that uncommitted entries can be overwritten by a new leader. This means: if a client submitted a changeset and it was in the old leader's in-flight Raft proposal that was not yet committed, that changeset is lost. The client does not receive an ACK. After the client's WebSocket connection drops, it reconnects, resets, and re-submits the changeset from the new `base_rev`. This is correct behavior: the client's `X` was never ACKed, so the client still has it in memory.

**Scenario 3: Network partition.** Suppose Servers 1 and 2 are in one partition, Server 3 in another. Server 3 can never win an election (it can only vote for itself, not reach a majority of 2). Servers 1 and 2 form a quorum and elect a leader among themselves. The system continues serving writes. Server 3 cannot serve writes (its leader redirect will fail or its own election will not succeed). If a client is connected to Server 3, it will receive a redirect. This is the CP choice: during a partition, the minority partition loses write availability in exchange for never committing an inconsistent entry.

**What you give up (be explicit with the professor):** You are choosing CP over A in the CAP theorem. During a partition where no server can reach a majority, the system rejects all writes. You do *not* guarantee availability in the sense of every request receiving a response. You do guarantee that every committed write is durable and consistent. You also do not provide offline editing (unlike Etherpad's client-side fallback) — a disconnected client can type locally into `Y`, but those changes will not be committed until the client reconnects and a quorum is reachable.

---

## Part 6: Exact Formal Guarantees

Being mathematically precise here is what distinguishes a great project from a mediocre one. Write these down and be ready to defend each one.

**Invariant 1 (Revision log agreement):** At any time, for any two servers $S_i$ and $S_j$ that have applied Raft log entries up to index $k$, their `revision_log[0..k-1]` is identical. This follows directly from Raft's log matching property.

**Invariant 2 (OT convergence):** For any two committed changesets $C_i$ at revision $r_i$ and $C_j$ at revision $r_j$ (where $r_i < r_j$), the document state produced by applying both is unique. This follows from the follow-equivalence property: $C_i \cdot f(C_i, C_j) = C_j \cdot f(C_j, C_i)$.

**Invariant 3 (No rollback):** Once a client receives an `ACK` for submission with `new_rev = r`, that change will be at `revision_log[r-1]` on any server that has applied entries up to at least $r$, forever. This follows from Raft's commitment guarantee: a committed entry is never overwritten.

**Invariant 4 (Deterministic state):** Given the same sequence of committed Raft log entries, all servers compute the same `head_text` and `revision_log`. This follows from the determinism of `follow()` and `apply_changeset()`.

**Liveness (not a safety property):** As long as a majority of servers are alive and connected to each other, new submissions will eventually be committed. This is Raft's progress guarantee.

---

## Part 7: The Deduplication Problem (Critical)

This is a subtle bug that will appear in testing if you do not address it. Consider this scenario: Client A submits entry E. The leader proposes E to Raft. A majority ACKs the proposal (so E is committed). The leader's `on_commit` fires, sends `ACK` to Client A. But the ACK's TCP packet is lost because the leader crashes immediately after. Client A reconnects, does not know if E was committed, and re-submits E. If the new leader proposes E again, it gets committed *twice* — and the document gets the edit applied twice.

The fix is the `submission_id` field plus the `client_last_submission` map in the state machine. The `apply()` function checks: "have I already applied submission_id $n$ from client $X$? If yes, return `None`." This check is part of the *persistent* state machine state — it must survive server restarts. So `client_last_submission` must be persisted to disk (or reconstructed by replaying the revision log on startup).

---

## Part 8: Technology Stack Recommendation

For a term project at the level your professor expects, here is what I recommend.

**Language: Go.** The Raft libraries for Go are more mature, better documented, and more commonly studied in the distributed systems literature. `hashicorp/raft` is the clearest to understand because its API maps almost exactly to what the Raft paper describes. You supply a `FSM` interface with `Apply()`, `Snapshot()`, and `Restore()` methods — your `DocumentStateMachine` becomes the FSM implementation.

**Raft library: `hashicorp/raft`.** It handles leader election, log replication, log compaction (snapshotting), and cluster membership. You implement `FSM.Apply()` as your `DocumentStateMachine.apply()`. It handles everything else. Later, when you write your own Raft implementation, you just replace the library with your own struct that satisfies the same interface.

**OT engine: implement from scratch.** The `follow()`, `compose()`, and `apply_changeset()` functions are not large — around 150–200 lines of Go total. Writing them yourself is the right move because (a) you will understand them deeply and can answer any professor question about them, and (b) the existing libraries for EasySync-style OT are all in JavaScript and not idiomatic Go.

**Client-server transport: Gorilla WebSocket** (for Go). Simple, well-tested, handles the connection lifecycle cleanly.

**Server-server transport: gRPC.** `hashicorp/raft` lets you supply a custom transport; implement it using gRPC's bidirectional streaming. Alternatively, `hashicorp/raft-grpc-transport` is a community library that does this for you.

**Storage: BoltDB or BadgerDB.** `hashicorp/raft` uses a `LogStore` and `StableStore` interface. BoltDB is simple, embedded, and correct. Your `DocumentStateMachine` also needs persistent storage for the revision log and `client_last_submission` — use the same BoltDB instance.

---

## Part 9: Implementation Phases

**Phase 1 — OT core (1 week).** Implement `Changeset`, `follow()`, `compose()`, `apply_changeset()` in Go with a complete test suite. Test every scenario from the EasySync document (the baseball/basil example, simultaneous inserts, delete-vs-insert, the lexicographic tie-breaker). Your test suite should verify: $A \cdot f(A,B) = B \cdot f(B,A)$ for randomly generated changeset pairs. Do not proceed until this is mathematically solid — everything else rests on it.

**Phase 2 — Single-server version (1 week).** Implement the `DocumentStateMachine`, the WebSocket server, and the client JavaScript. Get multi-user editing working with one server. This is your baseline — if it works here, adding Raft is a clean addition rather than a debugging nightmare.

**Phase 3 — Raft integration (1–2 weeks).** Wrap the `DocumentStateMachine` in the `hashicorp/raft` FSM interface. Set up a 3-node cluster with gRPC transport. Test that all three nodes agree on `head_text` after concurrent writes. Test leader failover: kill the leader, verify that a new leader is elected within ~500ms, verify that clients reconnect and resume editing.

**Phase 4 — Client reconnection and deduplication (1 week).** Implement the `REDIRECT` message, the reconnect protocol (catch-up revisions), and the `submission_id`-based deduplication. This is where you test the "no rollback" invariant explicitly: submit a change, kill the leader before ACK, reconnect to a new leader, verify the change either committed once or not at all.

**Phase 5 — Snapshotting (optional but impressive).** Without snapshotting, the Raft log grows forever, and a restarted node has to replay the entire log to reconstruct `head_text`. Implement `FSM.Snapshot()` to write a snapshot of `(head_text, head_rev, client_last_submission)` to disk. Implement `FSM.Restore()` to load it. `hashicorp/raft` will automatically use snapshots when a new node joins or a crashed node restarts.

---

## Part 10: What to Tell Your Professor

When your professor asks about the design choices, here is the exact framing:

On active replication: "All three servers maintain a complete replica of the revision log and participate in every write via the Raft consensus protocol. A write is committed only after a majority of servers have accepted the log entry. There is no passive server that only receives updates — every server votes on every proposal." This is textbook active replication.

On the servers deciding the order: "Servers collectively decide the ordering of revisions through Raft's leader election and `AppendEntries` voting. The leader proposes an ordering; a majority must agree before any order is considered decided. No single server can unilaterally impose an order."

On OT and why you still need consensus: "Operational transformation guarantees that given a fixed starting document and a set of changesets, all orderings of those changesets produce the same result. But this does not eliminate the need for ordering — it only means that once an ordering is agreed upon, clients can reconstruct the final state even if they receive changesets in different orders locally. Consensus is necessary to ensure that the revision log — the ground truth of what happened and when — is the same on every server."

On the CAP theorem: "We choose CP. During a network partition where no quorum is reachable, the system rejects writes. This ensures that every committed revision is durable and that no server ever diverges from the canonical revision log. We trade write availability in partitioned scenarios for strong consistency."

On what Etherpad gives up: "The original Etherpad uses a single server, so there is no need for consensus. Our system introduces a replicated revision log, requiring us to give up true offline editing — a client's unsubmitted changes are not durable across server failures — and sacrificing write availability under partition. In exchange, we gain fault tolerance for the canonical document state."

This is a complete, mathematically grounded implementation plan. The phased approach lets you demonstrate correctness at each stage, which also means that if you run short on time, you have a working Phase 2 system to fall back on. Start with the OT core — get the math right first, and everything else becomes much cleaner.