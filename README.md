# tokio-token-bucket

[![Crates.io](https://img.shields.io/crates/v/tokio-token-bucket.svg)](https://crates.io/crates/tokio-token-bucket)
[![Documentation](https://docs.rs/tokio-token-bucket/badge.svg)](https://docs.rs/tokio-token-bucket)
[![CI](https://github.com/legra-ai/tokio-token-bucket/actions/workflows/ci.yml/badge.svg)](https://github.com/legra-ai/tokio-token-bucket/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/tokio-token-bucket.svg)](https://github.com/legra-ai/tokio-token-bucket#license)

Bounded asynchronous rate limiting for Tokio tasks.

## What it does

`TokenBucket` controls how quickly work may begin. It starts with a bounded
number of permits, allowing an initial burst, then replenishes permits at the
configured rate. When no permit is available, `acquire().await` sleeps until
the next one is due.

```rust
use std::num::NonZeroU32;
use tokio_token_bucket::TokenBucket;

async fn send_one_probe() {
    let mut bucket = TokenBucket::new(NonZeroU32::new(4).expect("non-zero rate"));

    bucket.acquire().await;
    // Start exactly one operation after acquiring its permit.
    send_probe().await;
}

async fn send_probe() {}
```

The bucket itself:

- stores only counters and timestamps;
- allocates no request queue;
- spawns no background task;
- does not retain payloads or work items;
- consumes no permit when a waiting future is cancelled.

The caller owns the sequencing. Call `acquire().await` immediately before
starting each operation, so backpressure happens at the operation boundary.

## Rate and burst are separate

Use `with_burst` when the sustained rate and initial burst should differ:

```rust
use std::num::NonZeroU32;
use tokio_token_bucket::TokenBucket;

let mut bucket = TokenBucket::with_burst(
    NonZeroU32::new(10).expect("non-zero rate"),
    NonZeroU32::new(3).expect("non-zero burst"),
);
```

This permits at most three immediate operations and then replenishes at ten
permits per second.

## Rate limiting is not concurrency limiting

A token bucket limits the rate at which operations start. It does not limit
how many operations are simultaneously running. Use a Tokio semaphore for
concurrency limits, and acquire the rate permit before starting the operation.

`TokenBucket` uses `&mut self`, so it has no internal waiter queue or mutex.
If several tasks must share one bucket, put it behind the synchronization
primitive appropriate for the caller's ownership model.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
