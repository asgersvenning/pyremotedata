import src.remote_data.config as config

def test_get_config():
    c =  config.get_config()
    print(c)
    assert c is not None

test_get_config()