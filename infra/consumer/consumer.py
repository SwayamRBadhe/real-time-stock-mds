#import files required
import json
import time
from kafka import KafkaConsumer
import boto3

#initilize the minio and setup the connection
s3 = boto3.client( 
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)

bucket_name = "bronze-transactions"

# Ensure bucket exists (idempotent). if we dont do this then it will create another bucket with same name. we dont want data redudancy
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")

#Define Consumer and connect it to kafka
consumer = KafkaConsumer( #initializing the data in kafka to be send in minio
    "stock-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) #again converting to json from byte. since transfering from kafka to boto3
)

print("Consumerstreaming and saving to MinIO...")

#Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{int(time.time() * 1000)}.json" #this was giving error for everything. we got 139 objects in kafka by api fetch but when saving to minio it was giving less 25 objects coz of same timestamp. so to avoid that we multiplied it by 1000 to get unique timestamp for each object. prodcuer doesnt keep timestamp so we are taking current time. youtuber was ruinning side by side. since we stop producer at firstthen ran consumer and the producer limit was lost and the previouis fetch data were removed. so we got less data. 


    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")