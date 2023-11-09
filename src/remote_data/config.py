import os
import yaml

def get_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')
    
    if not os.path.exists(config_path):
        if input("Config file not found. Create default config file? (y/n): ").lower() == 'y':
            create_default_config()
        else:
            raise FileNotFoundError("Config file not found at {}".format(config_path))

    with open(config_path, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None
    
    return config_data

config = get_config()

def get_this_config(this):
    if this not in config:
        raise ValueError("Key {} not found in config".format(this))
    return config[this]

def get_mount_config():
    return config['mount']

def get_dataloader_config():
    return config['dataloader']

def get_implicit_mount_config():
    return config['implicit_mount']

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
    
def create_default_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, 'pyremotedata_config.yaml')

    yaml_content = """
        # Mounting configuration
        mount: {
            # Remote configuration
            remote: "YOUR_REMOTE_NAME_HERE",
            remote_subdir: "YOUR_REMOTE_DIRECTORY_HERE",
            local: "YOUR_LOCAL_MOUNT_POINT_HERE",

            # Rclone configuration (Can be left as-is)
            rclone: {
                vfs-cache-mode: "full",
                vfs-read-chunk-size: "1M",
                vfs-cache-max-age: "10h",
                vfs-cache-max-size: "500G",
                max-read-ahead: "1M",
                dir-cache-time: "15m",
                fast-list: true,
                transfers: 10,
                daemon: true
            }
        }

        implicit_mount: {
            # Remote configuration
            user: "YOUR_REMOTE_USERNAME_HERE",
            remote: "YOUR_REMOTE_URI_HERE",
            local_dir: , # Leave empty to use the default local directory
            default_remote_dir : "YOUR_REMOTE_DIRECTORY_HERE",

            # Lftp configuration (Can be left as-is)
            lftp: {
                'mirror:use-pget-n': 5, # Enable this to split a single file into multiple chunks and download them in parallel, when using mirror
                'net:limit-rate': 0, # No limit on transfer rate, maximizing throughput.
                'xfer:parallel': 5, # Enable this to split a single file into multiple chunks and download them in parallel
                'mirror:parallel-directories': "on", # Enable this to download multiple directories in parallel
                'ftp:sync-mode': 'off', # Enable this to disable synchronization mode, which is used to prevent data corruption when downloading multiple files in parallel
                'cmd:parallel': 1,  # If you write bespoke scripts that execute multiple commands in parallel, you can increase this value.
                'net:connection-limit': 0,  # No limit on connections, maximizing throughput.
                'cmd:verify-path': "off",  # To reduce latency, we skip path verification.
                'cmd:verify-host': "on",  # For initial security, it's good to verify the host.
                'sftp:size-read': 0x5000,  # Increased block size for better read throughput.
                'sftp:size-write': 0x5000,  # Increased block size for better write throughput.
                'sftp:max-packets-in-flight' : 512,  # Increased number of packets in flight for better throughput.
                'xfer:verify': "off",  # Disabling this for maximum speed.
                'cmd:interactive': "false",  # Disabled for automated transfers.
                'cmd:trace': "false",  # Disabled unless debugging is required.
                'xfer:clobber': "true",  # Overwrite existing files.
            }
        }

        """
    
    with open(config_path, "w") as config_file:
        config_file.write(yaml_content)
    
    print("Created default config file at {}".format(config_path))
    print("Please edit the config file and fill in the required fields.")
