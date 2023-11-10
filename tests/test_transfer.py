# ## Strategy to setup a mock sftp server for testing
# ssh-keygen -t rsa -b 4096 -f /tmp/temp_sftp_key -q -N ""
# eval $(ssh-agent -s)
# mkdir -p /tmp
# ssh-add /tmp/temp_sftp_key

# mkdir -p /tmp/upload
# chmod ugo+rwx /tmp/upload
# sudo docker run --name mock_sftp_server -p 0.0.0.0:2222:22 -d \
#     -v /tmp/temp_sftp_key.pub:/home/foo/.ssh/keys/temp_sftp_key.pub:ro \
#     -v /tmp/upload:/home/foo/upload \
#     atmoz/sftp foo::1001
# for i in {1..10}; do # tries up to 10 times
#     ssh-keyscan -p 2222 -H 0.0.0.0 >> ~/.ssh/known_hosts && break
#     echo "Waiting for SFTP server to be ready..."
#     sleep 1 # wait for 1 second before retrying
# done

# RUN TESTS

# ssh-keygen -R 0.0.0.0
# sudo docker stop mock_sftp_server
# sudo docker rm mock_sftp_server
# ssh-add -d /tmp/temp_sftp_key
# rm /tmp/temp_sftp_key /tmp/temp_sftp_key.pub
# eval "$(ssh-agent -k)"
# rm -rf /tmp/upload

##### PSA: This test has external dependencies 'wrapt_timeout_decorator' and 'unittest' #####
##### OBS: I strongly discourage running this test on a production machine. #####

import wrapt_timeout_decorator
from unittest.mock import patch
import os

@wrapt_timeout_decorator.timeout(10)
def test_implicit_mount():
    with patch.dict('os.environ', {
        'PYREMOTEDATA_REMOTE_USERNAME': 'foo',
        'PYREMOTEDATA_REMOTE_URI': '0.0.0.0',
        'PYREMOTEDATA_REMOTE_DIRECTORY': 'upload',
        'PYREMOTEDATA_AUTO': 'yes'
    }):
        # Import the module
        from remote_data.implicit_mount import IOHandler
        # Open the connection
        handler = IOHandler()
        handler.start()
        # Run some commands
        print(handler.pwd())
        print(handler.ls())
        # Cleanup
        handler.stop()
        from remote_data.config import remove_config
        remove_config()

@wrapt_timeout_decorator.timeout(25)
def test_upload_download():
    with patch.dict('os.environ', {
        'PYREMOTEDATA_REMOTE_USERNAME': 'foo',
        'PYREMOTEDATA_REMOTE_URI': '0.0.0.0',
        'PYREMOTEDATA_REMOTE_DIRECTORY': 'upload',
        'PYREMOTEDATA_AUTO': 'yes'
    }):
        # Import the module
        from remote_data.implicit_mount import IOHandler
        # Open the connection
        handler = IOHandler()
        handler.start()
        print(handler.pwd())
        # Upload a test file to the mock SFTP server
        upload_result = handler.put("https://link.testfile.org/15MB", "testfile.txt")
        # Download the test file from the mock SFTP server
        download_result = handler.download("testfile.txt", "testfile.txt")
        # Get the local directory where the file should be downloaded to
        local_directory = handler.lpwd()
        # Sanity checks
        local_file_exists = os.path.exists(os.path.join(local_directory, 'testfile.txt'))
        local_file_size = os.path.getsize(os.path.join(local_directory, 'testfile.txt')) / 10**6
        if not local_file_exists:
            raise RuntimeError("Something went wrong with the download. The file does not exist locally.")
        if not (local_file_size > 10 and local_file_size < 20):
            raise RuntimeError("Something went wrong with the download. The file size is not correct.")
        # Cleanup
        handler.stop()        
        from remote_data.config import remove_config
        remove_config()

try:
    test_implicit_mount()
except Exception as e:
    print("Basic functionality test failed.")
    raise e

try:
    test_upload_download()
except Exception as e:
    print("Upload/download test failed.")
    raise e