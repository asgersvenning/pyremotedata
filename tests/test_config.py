from unittest.mock import patch
import src.remote_data.config as config

def test_config():
    with patch.dict('os.environ', {
        'PYREMOTEDATA_REMOTE_USERNAME': 'test',
        'PYREMOTEDATA_REMOTE_URI': 'test.com',
        'PYREMOTEDATA_REMOTE_DIRECTORY': 'test',
        'PYREMOTEDATA_AUTO': 'yes'
    }):
        c = config.get_config()
        print(c)
        assert c is not None

test_config()