# Dia

A highly consistent system to order distributed events, implemented in Rust. Based on the description of Concord in [State Machine Replication, and Why You Should Care](https://signalsandthreads.com/state-machine-replication-and-why-you-should-care/) from the Signals & Threads podcast.

## Traits
- Events have a global ordering
- Events are always ordered with respect to a single client (i.e. if a client sends two events, they will retain ordering)
