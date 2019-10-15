# S3Watcher

A daemon to cache the object listings for [AWS
S3](https://aws.amazon.com/s3/) buckets. If you have a very large
bucket, then listing the entire bucket can take a long time. The daemon
can also watch an [SQS](https://aws.amazon.com/sqs/) queue for changes
in the bucket.
