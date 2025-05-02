import os

import yaml

from pyremotedata import module_logger


def ask_user(question, interactive=True):
    if not interactive:
        raise RuntimeError("Cannot ask user for input when interactive=False: " + question)
    return input(question)

ENVIRONMENT_VARIABLES = (
    "PYREMOTEDATA_REMOTE_USERNAME", 
    "PYREMOTEDATA_REMOTE_URI", 
    "PYREMOTEDATA_LOCAL_DIRECTORY", 
    "PYREMOTEDATA_REMOTE_DIRECTORY"
)

def get_environment_variables(interactive=True):
    remote_username = os.getenv(ENVIRONMENT_VARIABLES[0], None) or ask_user(f"{ENVIRONMENT_VARIABLES[0]} not set. Enter your remote name: ", interactive)
    remote_uri = os.getenv(ENVIRONMENT_VARIABLES[1], None) or (ask_user(f"{ENVIRONMENT_VARIABLES[1]} not set. Enter your remote URI (leave empty for 'io.erda.au.dk'): ", interactive) or 'io.erda.au.dk')
    local_directory = os.getenv(ENVIRONMENT_VARIABLES[2], "")
    remote_directory = os.getenv(ENVIRONMENT_VARIABLES[3], None) or ask_user(f"{ENVIRONMENT_VARIABLES[3]} not set. Enter your remote directory: ", interactive)
    
    return remote_username, remote_uri, local_directory, remote_directory

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pyremotedata_config.yaml')

def remove_config():
    if os.path.exists(CONFIG_PATH):
        os.remove(CONFIG_PATH)
        module_logger.info("Removed config file at {}".format(CONFIG_PATH))
    else:
        module_logger.info("No config file found at {}".format(CONFIG_PATH))

def create_default_config(interactive=True):
    remote_username, remote_uri, local_directory, remote_directory = get_environment_variables(interactive)

    yaml_content = f"""
# IMPORTANT: If you want to change this config manually and permanently, set this to 'false'
validate: true

implicit_mount:
    # Remote configuration
    user: "{remote_username}"
    remote: "{remote_uri}"
    local_dir: "{local_directory}" # Leave empty to use the default local directory
    default_remote_dir : "{remote_directory}"

    # Lftp configuration (Can be left as-is)
    lftp:
        'mirror:use-pget-n': 5  # Enable this to split a single file into multiple chunks and download them in parallel, when using mirror
        'net:limit-rate': 0  # No limit on transfer rate, maximizing throughput.
        'xfer:parallel': 5  # Enable this to split a single file into multiple chunks and download them in parallel
        'mirror:parallel-directories': "on"  # Enable this to download multiple directories in parallel
        'ftp:sync-mode': 'off'  # Enable this to disable synchronization mode, which is used to prevent data corruption when downloading multiple files in parallel
        'cmd:parallel': 1  # If you write bespoke scripts that execute multiple commands in parallel, you can increase this value.
        'net:connection-limit': 0  # No limit on connections, maximizing throughput.
        'cmd:verify-path': "off"  # To reduce latency, we skip path verification.
        'cmd:verify-host': "on"  # For initial security, it's good to verify the host.
        'sftp:size-read': 0x5000  # Increased block size for better read throughput.
        'sftp:size-write': 0x5000  # Increased block size for better write throughput.
        'sftp:max-packets-in-flight' : 512  # Increased number of packets in flight for better throughput.
        'xfer:verify': "off"  # Disabling this for maximum speed.
        'cmd:interactive': "false"  # Disabled for automated transfers.
        'cmd:trace': "false"  # Disabled unless debugging is required.
        'xfer:clobber': "true"  # Overwrite existing files.

"""
    
    with open(CONFIG_PATH, "w") as config_file:
        config_file.write(yaml_content)
    
    module_logger.info("Created default config file at {}".format(CONFIG_PATH))
    module_logger.info("OBS: It is **strongly** recommended that you **check the config file** and make sure that it is correct before using pyRemoteData.")

def get_config(validate : bool=True):  
    if not os.path.exists(CONFIG_PATH):
        interactive = os.getenv("PYREMOTEDATA_AUTO", "yes").lower().strip() != "yes"
        if not interactive or ask_user("Config file not found. Create default config file? (y/n): ", interactive).lower().strip() == 'y':
            create_default_config(interactive)
        else:
            raise FileNotFoundError("Config file not found at {}".format(CONFIG_PATH))

    with open(CONFIG_PATH, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            module_logger.error(exc)
            return None

    # Early return if no validation is applied
    if not validate or not config_data["validate"]:
        return config_data

    # Check if environment variables match config (config/cache invalidation)
    invalid = False
    for k, ek, v in zip(
        ["user", "remote", "local_dir", "default_remote_dir"], 
        ENVIRONMENT_VARIABLES, 
        get_environment_variables()
    ):
        expected = config_data["implicit_mount"][k]
        if expected != v and not (expected is None and v == ""):
            module_logger.warning(f"Invalid config detected, auto regenerating from scratch: Expected '{expected}' for '{k}' ({ek}), but got '{v}'.")
            invalid = True
    
    if invalid:
        remove_config()
        if interactive:
            return get_config()
        else:
            raise RuntimeError(f'Aborted due to invalid config.')

    return config_data

def get_this_config(this, **kwargs):
    if not isinstance(this, str):
        raise TypeError("Expected string, got {}".format(type(this)))
    # Load config
    cfg = get_config(**kwargs)
    if this not in cfg:
        raise ValueError("Key {} not found in config".format(this))
    return cfg[this]

def get_mount_config(**kwargs):
    return get_this_config('mount', **kwargs)

def get_dataloader_config(**kwargs):
    return get_this_config('dataloader', **kwargs)

def get_implicit_mount_config(**kwargs):
    return get_this_config('implicit_mount', **kwargs)

def deparse_args(config, what):
    if not isinstance(what, str):
        raise TypeError("Expected string, got {}".format(type(what)))
    if what not in config:
        raise ValueError("Key {} not found in config".format(what))
    args = config[what]
    if not isinstance(args, dict):
        raise TypeError("Expected dict, got {}".format(type(args)))
    
    try:
        arg_str = ""
        for key, value in args.items():
            if not isinstance(key, str):
                raise TypeError("Expected string, got {}".format(type(key)))
            if isinstance(value, bool):
                arg_str += " --{}".format(key)
            elif isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
                arg_str += " --{} {}".format(key, value)
            else:
                raise TypeError("Expected string, int, float or bool, got {}".format(type(value)))
    except Exception as e:
        raise yaml.YAMLError("Error while parsing args: {}".format(e))
    
    return arg_str

