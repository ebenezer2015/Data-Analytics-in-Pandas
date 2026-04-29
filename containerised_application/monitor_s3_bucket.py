# monitor_s3_bucket.py

import argparse

from s3_bucket_monitor_fast import S3BucketHandler


def monitor_s3_bucket(bucket_name, prefix="", interval=5):
    """
    Starts the optimised S3 monitoring loop.
    - Uses cached processed keys (persistent skip logic)
    - Uses prefix filtering
    - Uses background threads for heavy work
    - Polls S3 at the given interval
    """
    handler = S3BucketHandler(
        bucket_name=bucket_name,
        prefix=prefix,
        interval=interval
    )

    handler.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimised S3 bucket monitor")

    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="Name of the S3 bucket to monitor"
    )

    parser.add_argument(
        "--prefix",
        type=str,
        default="",
        help="Prefix to filter S3 objects (e.g., incoming/)"
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Polling interval in seconds (default: 5)"
    )

    args = parser.parse_args()

    monitor_s3_bucket(
        bucket_name=args.bucket,
        prefix=args.prefix,
        interval=args.interval
    )


# open powershell
# docker --version
# docker build -t s3-monitor .
# docker run --rm --env-file .env s3-monitor --bucket scetru-ml-bucket