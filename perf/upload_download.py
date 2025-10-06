from pyremotedata.implicit_mount import *
import time
import tempfile
import os

test_size = 3 * 10**6
    
def make_mock_file(dir, size):
    with tempfile.NamedTemporaryFile(dir=dir, delete=False, suffix=".txt") as f:
        f.write(os.urandom(size))
        return f.name

def perf_test(share_link_id : str | None):
    with tempfile.TemporaryDirectory() as upload_dir, tempfile.TemporaryDirectory() as download_dir:
        mock_files = [make_mock_file(upload_dir, test_size) for _ in range(32)]
        total_data = test_size * len(mock_files)

        with IOHandler(user=share_link_id, password=share_link_id, verbose=True) as io:
            if not io.exists("testing", "directory"):
                io.execute_command("mkdir testing")
            io.cd("testing")
            # Set the local working directory to the directory which contains this script
            io.lcd(os.path.dirname(os.path.abspath(__file__)))

            start_upload = time.time()
            io.upload(upload_dir, os.path.split(upload_dir)[-1])
            end_upload = time.time()

            start_download = time.time()
            io.download(os.path.split(upload_dir)[-1], download_dir)
            end_download = time.time()

            start_delete = time.time()
            io.rm(os.path.split(upload_dir)[-1], force=True)
            end_delete = time.time()

        upload_time = end_upload - start_upload
        download_time = end_download - start_download
        delete_time = end_delete - start_delete
        
        upload_speed, download_speed, delete_speed = total_data/upload_time, total_data/download_time, total_data/delete_time

        print("Upload speed: {} MB/s".format(upload_speed/10**6))
        print("Download speed: {} MB/s".format(download_speed/10**6))
        print("Delete speed: {} MB/s".format(delete_speed/10**6))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("perf_updown")
    parser.add_argument(
        "-I", "--ID", type=str, required=False,
        help="ERDA Share link ID for SSH-less testing"
    )
    args = parser.parse_args()
    perf_test(args.ID)