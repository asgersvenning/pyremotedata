import os
import yaml

# Interanl imports
from pyremotedata import main_logger, module_logger

config = None

def ask_user(question, interactive=True):
    if not interactive:
        raise RuntimeError("Cannot ask user for input when interactive=False")
    return input(question)

def create_default_config(interactive=True):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')

    # Check for environment variables or ask for user input
    remote_username = os.getenv('PYREMOTEDATA_REMOTE_USERNAME', None) or ask_user("Enter your remote name: ", interactive)
    remote_uri = os.getenv('PYREMOTEDATA_REMOTE_URI', None) or (ask_user("Enter your remote URI (leave empty for 'io.erda.au.dk'): ", interactive) or 'io.erda.au.dk')
    local_dir = os.getenv('PYREMOTEDATA_LOCAL_DIR', "")
    remote_directory = os.getenv('PYREMOTEDATA_REMOTE_DIRECTORY', None) or ask_user("Enter your remote directory: ", interactive)
    if isinstance(local_dir, str) and local_dir != "":
        local_dir = f'"{local_dir}"'
    if isinstance(remote_directory, str) and remote_directory != "":
        remote_directory = f'"{remote_directory}"'

    # TODO: Remove unnecessary config options!
    yaml_content = f"""
# Mounting configuration (NOT USED AT THE MOMENT - TODO: Implement or remove)
mount:
    # Remote configuration
    remote: "YOUR_REMOTE_NAME_HERE"
    remote_subdir: "YOUR_REMOTE_DIRECTORY_HERE"
    local: "YOUR_LOCAL_MOUNT_POINT_HERE"

    # Rclone configuration (Can be left as-is)
    rclone:
        vfs-cache-mode: "full"
        vfs-read-chunk-size: "1M"
        vfs-cache-max-age: "10h"
        vfs-cache-max-size: "500G"
        max-read-ahead: "1M"
        dir-cache-time: "15m"
        fast-list: true
        transfers: 10
        daemon: true

implicit_mount:
    # Remote configuration
    user: "{remote_username}"
    remote: "{remote_uri}"
    local_dir: {local_dir} # Leave empty to use the default local directory
    default_remote_dir : {remote_directory}

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
    
    with open(config_path, "w") as config_file:
        config_file.write(yaml_content)
    
    module_logger.info("Created default config file at {}".format(config_path))
    module_logger.info("OBS: It is **strongly** recommended that you **check the config file** and make sure that it is correct before using pyRemoteData.")

def get_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')
    
    if not os.path.exists(config_path):
        interactive = os.getenv("PYREMOTEDATA_AUTO", "no").lower().strip() != "yes"
        if not interactive or ask_user("Config file not found. Create default config file? (y/n): ", interactive).lower().strip() == 'y':
            create_default_config(interactive)
        else:
            raise FileNotFoundError("Config file not found at {}".format(config_path))

    with open(config_path, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            module_logger.error(exc)
            return None
    
    return config_data

config = get_config()

def get_this_config(this):
    global config
    if not isinstance(this, str):
        raise TypeError("Expected string, got {}".format(type(this)))
    # Check if config is loaded
    if config is None:
        # Load config
        config = get_config()
    if this not in config:
        raise ValueError("Key {} not found in config".format(this))
    return config[this]

def get_mount_config():
    return get_this_config('mount')

def get_dataloader_config():
    return get_this_config('dataloader')

def get_implicit_mount_config():
    return get_this_config('implicit_mount')

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

def remove_config():
    global config
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')
    if os.path.exists(config_path):
        os.remove(config_path)
        module_logger.info("Removed config file at {}".format(config_path))
    else:
        module_logger.info("No config file found at {}".format(config_path))
    # Reset global config
    config = None

def config_path():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')
    return config_path