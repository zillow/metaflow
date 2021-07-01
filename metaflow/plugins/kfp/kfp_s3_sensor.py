import boto3
import botocore
import time

s3 = boto3.resource('s3')

def wait_for_s3_path(bucket: str, key: str, prefix: str, timeout: int) -> None:
    start_time = time.time()
    while True:
        if bucket:
            try:
                s3.Object(bucket, key).load()
            except botocore.exceptions.ClientError as e:
                print("Object not found. Waiting...")
            else:
                print("Object found! Step complete.")
                break
        else:
            bucket_resource = s3.Bucket(bucket)
            # we limit the number of filtered objects to 1 for efficiency
            # .filter() returns a type of generator, but we check if the number
            # of objects in the generator is > 0. Performing list(s3_objects)
            # is much more efficient when the s3_objects contains just one element
            s3_objects = bucket_resource.objects.filter(Prefix=prefix).limit(1)
            s3_objects_list = list(s3_objects)

            if len(s3_objects_list) > 0:
                print("Path found. Step complete.")
                break
            else:
                print("Patt not found. Waiting...")

        current_time = time.time()
        elapsed_time = current_time - start_time
        if timeout is not -1 and elapsed_time > timeout:
            raise Exception("Timed out while waiting for S3 key.")

        time.sleep(60)
