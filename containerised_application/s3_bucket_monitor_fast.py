import json
import logging
import os
import threading
import time

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from model_pipeline import model_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

PROCESSED_KEYS_FILE = "processed_keys.json"

class S3BucketHandler:
    def __init__(self, bucket_name, prefix="", interval=5, credentials_file=".env"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.interval = interval
        self.credentials_file = credentials_file

        # Load AWS credentials
        self.s3 = self._load_credentials()

        # Load previously processed keys (persistent skip logic)
        self.processed_keys = self.load_processed_keys()

        # Thread safety
        self.lock = threading.Lock()

    # ---------------------------------------------------------
    # LOAD CREDENTIALS
    # ---------------------------------------------------------
    def _load_credentials(self):
        load_dotenv(self.credentials_file)
        return boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

    # ---------------------------------------------------------
    # PERSISTENT KEY STORAGE
    # ---------------------------------------------------------
    def load_processed_keys(self):
        """Load processed keys from local file."""
        if os.path.exists(PROCESSED_KEYS_FILE):
            try:
                with open(PROCESSED_KEYS_FILE, "r") as f:
                    return set(json.load(f))
            except Exception:
                return set()
        return set()

    def save_processed_keys(self):
        """Persist processed keys to local file."""
        try:
            with open(PROCESSED_KEYS_FILE, "w") as f:
                json.dump(list(self.processed_keys), f, indent=4)
        except Exception as e:
            logger.error(f"Error saving processed keys: {e}")

    # ---------------------------------------------------------
    # LIST NEW FILES (OPTIMISED)
    # ---------------------------------------------------------
    def list_new_files(self):
        paginator = self.s3.get_paginator("list_objects_v2")
        new_files = []

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key not in self.processed_keys:
                    new_files.append(key)

        return new_files

    # ---------------------------------------------------------
    # READERS (UNCHANGED)
    # ---------------------------------------------------------
    def _read_csv_from_s3(self, bucket_name, file_key):
        obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj["Body"])
        df["file_key"] = file_key
        return df

    def _read_json_from_s3(self, bucket_name, file_key):
        obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
        content = obj["Body"].read().decode("utf-8")

        try:
            data = json.loads(content)
            df = pd.DataFrame(data if isinstance(data, list) else [data])
        except json.JSONDecodeError:
            df = pd.DataFrame([json.loads(line) for line in content.splitlines()])

        df["file_key"] = file_key
        return df

    # ---------------------------------------------------------
    # BUSINESS LOGIC (ALL RESTORED EXACTLY)
    # ---------------------------------------------------------

    def join_trans_with_do_good(self, trans_data):
        do_good_table = self.collate_file("scetru-fcmb-do-good-table")
        cols = ['bvn', 'applicationID', 'date_of_default', 'outstanding_balance']
        do_good_table = do_good_table[cols]
        do_good_table['bvn'] = do_good_table['bvn'].astype("string")

        merged_df = trans_data.merge(do_good_table, on='bvn', how='left')
        merged_df['date_of_default'] = pd.to_datetime(merged_df['date_of_default'], errors='coerce')

        current_date = pd.Timestamp.now()
        days_since_default = (current_date - merged_df['date_of_default']).dt.days

        merged_df['default_in_last_90days'] = np.where(
            (days_since_default <= 90) & (merged_df['outstanding_balance'] != 0),
            'Y', 'N'
        )

        merged_df['has_it_make_it_good'] = np.where(
            (merged_df['outstanding_balance'] == 0) |
            (merged_df['default_in_last_90days'] == 'N'),
            'Y', 'N'
        )

        return merged_df.drop(columns=['date_of_default', 'outstanding_balance', 'applicationID'])

    def read_and_create_complete_table(self, outcome_table):
        complete_table = self.collate_file("complete-table")
        complete_table.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        complete_table['application_id'] = complete_table['application_id'].astype("string")
        complete_table['bvn'] = complete_table['bvn'].astype("string")

        mask = complete_table['decline_reason'].isna() & complete_table['amount_approved'].isna()
        complete_table = complete_table.loc[mask].reset_index(drop=True)

        complete_table = complete_table.drop(columns=['amount_approved', 'decline_reason'])

        merged = complete_table.merge(
            outcome_table[['bvn', 'application_id', 'amount_approved', 'decline_reason']],
            on=['bvn', 'application_id'],
            how='inner'
        )

        now = pd.Timestamp.now().floor('min')
        merged['updated_date'] = now.strftime('%Y-%m-%d %H:%M:%S')

        merged['loan_message'] = np.where(
            merged['amount_approved'] != 0.0, 'APPROVED', 'DECLINED'
        )

        return merged[
            [
                'bvn', 'dob', 'amount_requested', 'application_id', 'loan_tenure',
                'loan_repayment_structure', 'internal_id', 'amount_approved',
                'created_date', 'updated_date', 'decline_reason', 'loan_message',
                'file_key'
            ]
        ]

    def collate_file(self, bucket_name, prefix=""):
        files = self._list_bucket_contents(bucket_name, prefix)
        dfs = []

        for file in files:
            if file.endswith('.csv'):
                dfs.append(self._read_csv_from_s3(bucket_name, file))
            elif file.endswith('.json'):
                dfs.append(self._read_json_from_s3(bucket_name, file))

        dfs = [df for df in dfs if not df.empty and not df.isna().all().all()]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


    def _list_bucket_contents(self, bucket_name, prefix=""):
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError:
            return []

    def clean_ml_bucket(self, prefix=""):
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        all_objects = []
        for page in pages:
            all_objects.extend(page.get("Contents", []))

        if not all_objects:
            logger.info("No files found in prefix")
            return

        all_objects.sort(key=lambda x: x["LastModified"])
        file_to_keep = all_objects[-1]
        keys_to_delete = [obj["Key"] for obj in all_objects[:-1]]

        if not keys_to_delete:
            logger.info("Only one file present — nothing to delete")
            return

        try:
            self.s3.delete_objects(
                Bucket=self.bucket_name,
                Delete={"Objects": [{"Key": k} for k in keys_to_delete]}
            )
            logger.info(f"Deleted {len(keys_to_delete)} files, kept: {file_to_keep['Key']}")
        except ClientError as e:
            logger.error(f"Error deleting objects: {e}")

    def upload_files_to_s3_bucket(self, outcome_df, staging_location):
        S3BucketHandler.append_json_to_file(outcome_df)
        S3BucketHandler.save_processed_df_as_json(outcome_df)

        if not os.path.exists(staging_location):
            logger.info(f"Staging folder does not exist: {staging_location}")
            return

        application_ids = outcome_df["application_id"].astype(str).tolist()

        json_files = [
            f for f in os.listdir(staging_location)
            if f.endswith(".json") and any(f.startswith(app_id) for app_id in application_ids)
        ]

        if not json_files:
            logger.info(f"No JSON files found in staging folder: {staging_location}")
            return

        file_key = outcome_df["file_key"].iloc[0]
        s3_prefix = file_key.split("/")[0] if "/" in file_key else "default-prefix"

        bucket_name = "complete-table"

        for filename in json_files:
            local_path = os.path.join(staging_location, filename)
            s3_key = f"{s3_prefix}/{filename}"

            try:
                self.s3.upload_file(local_path, bucket_name, s3_key)
                logger.info(f"Uploaded: {local_path} → s3://{bucket_name}/{s3_key}")

            except Exception as e:
                logger.info(f"S3 upload error for {local_path}: {e}")

    # ---------------------------------------------------------
    # JSON HELPERS
    # ---------------------------------------------------------
    @staticmethod
    def convert_columns_type(df, column_types):
        for col, dtype in column_types.items():
            if col in df.columns:
                if dtype == 'date':
                    df[col] = pd.to_datetime(df[col]).dt.date
                else:
                    df[col] = df[col].astype(dtype)
        return df

    @staticmethod
    def append_json_to_file(df):
        file = "processed_complete_table.json"
        existing = S3BucketHandler.load_json(file)
        combined = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        combined.to_json(file, orient='records', indent=4)

    @staticmethod
    def load_json(path):
        try:
            return pd.read_json(path, orient='records')
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def save_processed_df_as_json(df):
        output_dir = "processed_loan_request"
        os.makedirs(output_dir, exist_ok=True)
        df = df.iloc[:, :-1]

        for app_id, group in df.groupby("application_id"):
            path = os.path.join(output_dir, f"{app_id}.json")
            group.to_json(path, orient='records', lines=True)

    # ---------------------------------------------------------
    # PROCESS FILE (HEAVY WORK IN THREAD)
    # ---------------------------------------------------------
    def process_file(self, key):
        logger.info(f"Processing new file: {key}")

        
        if key.endswith(".csv"):
            df = self._read_csv_from_s3(self.bucket_name, key)
        elif key.endswith(".json"):
            df = self._read_json_from_s3(self.bucket_name, key)
        else:
            logger.error(f"Unsupported file type: {key}")
            return

        column_types = {
            'bvn': 'str',
            'application_id': 'str',
            'amount_requested': 'float',
            'date_created': 'date',
            'airtime_in_90days': 'float',
            'bill_payment_in_90days': 'float',
            'cable_tv_in_90days': 'float',
            'deposit_in_90days': 'float',
            'easy_payment_in_90days': 'float',
            'farmer_in_90days': 'float',
            'inter_bank_in_90days': 'float',
            'mobile_in_90days': 'float',
            'utility_bills_in_90days': 'float',
            'withdrawal_in_90days': 'float',
        }

        logger.info("🔧 Converting column types...")
        df = self.convert_columns_type(df, column_types)
        df = df.drop_duplicates().fillna(0.0)

        logger.info("🔧 Joining with do-good table...")
        merged_df = self.join_trans_with_do_good(df)

        logger.info("🤖 Running model pipeline...")
        model_outcome = model_pipeline(merged_df)

        logger.info("🧩 Creating complete table...")
        complete_table = self.read_and_create_complete_table(model_outcome)

        logger.info("📤 Uploading processed results to S3...")
        if not complete_table.empty:
            self.upload_files_to_s3_bucket(
                outcome_df=complete_table,
                staging_location="processed_loan_request"
            )

        logger.info(f"Completed processing: {key}")


    def process_file_async(self, key):
        thread = threading.Thread(target=self.process_file, args=(key,))
        thread.daemon = True
        thread.start()

    # ---------------------------------------------------------
    # MAIN LOOP (WITH PERSISTENT KEY SAVING)
    # ---------------------------------------------------------
    def start(self):
        logger.info(f"Monitoring bucket '{self.bucket_name}' with prefix '{self.prefix}'")

        while True:
            try:
                new_files = self.list_new_files()

                for key in new_files:
                    with self.lock:
                        self.processed_keys.add(key)

                    self.process_file_async(key)

                # Persist processed keys
                self.save_processed_keys()

                time.sleep(self.interval)

            except KeyboardInterrupt:
                logger.info("Stopping monitor.")
                break

            except Exception as e:
                logger.info(f"Error: {e}")
                time.sleep(self.interval)
