import os
import src.remote_data.config as config

def test_get_config():
    c =  config.get_config()
    print(c)
    assert c is not None

os.putenv("PYREMOTEDATA_REMOTE_USERNAME", "test")
os.putenv("PYREMOTEDATA_REMOTE_URI", "test.com")
os.putenv("PYREMOTEDATA_REMOTE_DIRECTORY", "test")

test_get_config()