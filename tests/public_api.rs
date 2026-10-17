//! Public rate-limit behavior must follow the executor's clock.
use std::num::NonZeroU32;
use std::time::Duration;

use tokio_token_bucket::TokenBucket;

#[tokio::test(start_paused = true)]
async fn refill_follows_the_tokio_clock() {
    let rate = NonZeroU32::new(1).expect("nonzero rate");
    let mut bucket = TokenBucket::new(rate);
    bucket.acquire().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::ZERO, bucket.acquire())
            .await
            .is_ok()
    );
}

#[tokio::test(start_paused = true)]
async fn cancelled_wait_does_not_spend_a_future_permit() {
    let rate = NonZeroU32::new(1).expect("nonzero rate");
    let mut bucket = TokenBucket::new(rate);
    bucket.acquire().await;
    assert!(
        tokio::time::timeout(Duration::ZERO, bucket.acquire())
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::ZERO, bucket.acquire())
            .await
            .is_ok()
    );
}
