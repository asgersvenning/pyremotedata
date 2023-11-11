from pyremotedata.implicit_mount import *
import time
import tempfile
import os

test_size = 10**9

def get_mock_file(dir, size):
    """
    Creates a mock file of size `size` (bytes) in directory `dir`
    At the moment the function merely assumes that the file "sample.txt" exists in the directory of this script.
    it can be created with "openssl rand -out sample.txt -base64 $(( 2**30 * 3/4 ))" (takes a couple of seconds)
    """
    return "sample.txt"

def perf_test():
    upload_dir_obj = tempfile.TemporaryDirectory()
    download_dir_obj = tempfile.TemporaryDirectory()

    upload_dir = upload_dir_obj.name
    download_dir = download_dir_obj.name

    mock_file = get_mock_file(upload_dir, size=test_size)

    mount = ImplicitMount()
    mount.mount()
    mount.cd("testing")
    # Set the local working directory to the directory which contains this script
    mount.lcd(os.path.dirname(os.path.abspath(__file__)))

    start_upload = time.time()
    mount.put(mock_file)
    end_upload = time.time()

    start_download = time.time()
    mount.pget(mock_file, download_dir)
    end_download = time.time()

    start_delete = time.time()
    mount.execute_command("rm {}".format(mock_file))
    end_delete = time.time()

    upload_time = end_upload - start_upload
    download_time = end_download - start_download
    delete_time = end_delete - start_delete

    upload_speed, download_speed, delete_speed = test_size/upload_time, test_size/download_time, test_size/delete_time

    print("Upload speed: {} MB/s".format(upload_speed/10**6))
    print("Download speed: {} MB/s".format(download_speed/10**6))
    print("Delete speed: {} MB/s".format(delete_speed/10**6))

    mount.unmount()

    upload_dir_obj.cleanup()
    download_dir_obj.cleanup()

perf_test()