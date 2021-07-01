def wait_for_s3_path(path: str, timeout: int) -> None:
    print("path: ", path)
    print("timeout: ", timeout)
    import time
    print("Waiting for sleep to end...")
    time.sleep(60)
    print("Sleep completed...")
