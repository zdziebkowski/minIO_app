from minio import Minio
import json


def connect_minio():
    """Load config and connect to MinIO"""
    with open('config.json', 'r') as f:
        config = json.load(f)

    minio_config = config['minio']
    client = Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )

    bucket_name = minio_config['bucket_name']

    # Check bucket
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket created: {bucket_name}")
    else:
        print(f"Bucket exists: {bucket_name}")

    return client, bucket_name


def read_path_from_minio(client, bucket_name):
    """Read target path from MinIO"""
    try:
        response = client.get_object(bucket_name, "path_txt.txt")
        path_csv = response.read().decode('utf-8').strip()
        response.close()
        print(f"Target path: {path_csv}")
        return path_csv
    except Exception as e:
        print(f"Read error: {e}")
        return None


def upload_file(client, bucket_name, local_file_path, target_path):
    """Upload file to MinIO"""
    try:
        client.fput_object(bucket_name, target_path, local_file_path)

        stat = client.stat_object(bucket_name, target_path)
        print(f"Uploaded: {local_file_path} → {target_path}")
        print(f"Size: {stat.size} bytes")

    except FileNotFoundError:
        print(f"File not found: {local_file_path}")
    except Exception as e:
        print(f"Upload error: {e}")


def main():
    """Main function"""
    # Connect to MinIO
    client, bucket_name = connect_minio()

    # Read target path
    target_path = read_path_from_minio(client, bucket_name)
    if not target_path:
        return

    # Upload file
    local_csv = "supply_chain_deliveries.csv"
    upload_file(client, bucket_name, local_csv, target_path)

    print("Task completed!")


if __name__ == "__main__":
    main()