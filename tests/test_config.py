from unittest.mock import patch

def test_config():
    print("Running config test.")
    with patch.dict('os.environ', {
        'PYREMOTEDATA_REMOTE_USERNAME': 'test',
        'PYREMOTEDATA_REMOTE_URI': 'test.com',
        'PYREMOTEDATA_REMOTE_DIRECTORY': 'test',
        'PYREMOTEDATA_AUTO': 'yes'
    }):
        import pyremotedata.config as config
        c = config.get_config()
        print(c)
        assert c is not None
        # Cleanup
        config.remove_config()
    print("Config test passed.")

test_config()