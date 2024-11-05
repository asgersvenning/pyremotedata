"""
This module provides a pythonic interface for downloading files from a remote directory using the SFTP protocol.

The main functionality of this package is provided through the use of the ImplicitMount, IOHandler and RemotePathIterator classes:

* The **ImplicitMount** class provides a low-level wrapper for the LFTP shell, which is used to communicate with the remote directory, and should only be used directly by advanced users.

* The **IOHandler** class provides a high-level wrapper for the ImplicitMount class, which provides human-friendly methods for downloading files from a remote directory without the need to have technical knowledge on how to use LFTP.

* The **RemotePathIterator** class provides a high-level wrapper for the IOHandler class, and handles asynchronous streaming of files from a remote directory to a local directory using thread-safe buffers.
"""

# Standard library imports
import os
import re
import shutil
import tempfile
import time
import uuid
import logging
from random import shuffle
from typing import List, Set, Tuple, Union

# Threading and subprocess imports
import queue
import subprocess
import threading
from queue import Queue

# Internal import
from pyremotedata import main_logger, module_logger
from pyremotedata.config import get_implicit_mount_config

class ImplicitMount:
    """
    This is a low-level wrapper of LFTP, which provides a pythonic interface for executing LFTP commands and reading the output.
    It provides a robust and efficient backend for communicating with a remote storage server using the SFTP protocol, using a persistent LFTP shell handled in the background by a subprocess.
    It is designed to be used as a base class for higher-level wrappers, such as the IOHandler class, or as a standalone class for users familiar with LFTP.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    TODO: This class relies on a proper SSH setup on your machine (and the remote server) for passwordless SFTP. 
    Thoroughly test this on a fresh install, and add instructions, and the possibility for automatic setup, for setting up passwordless SFTP and SSH keys, as well as proper error handling for when this is not set up correctly.

    TODO: Add further arguments such as port, password, etc. to the constructor.

    Args:
        user (str): The username to use for connecting to the remote directory.
        remote (str): The remote server to connect to.
        verbose (bool): If True, print the commands executed by the class.

    .. <Sphinx comment
    Methods:
        format_options(): Format a dictionary of options into a string of command line arguments.
        execute_command(): Execute a command on the LFTP shell.
        mount(): Mount the remote directory.
        unmount(): Unmount the remote directory.
        pget(): Download a single file from the remote directory using multiple connections.
        put(): Upload a single file to the remote directory.
        ls(): List the contents of a directory (on the remote). OBS: In contrast to the other LFTP commands, this function has a lot of additional functionality, such as recursive listing and caching.
        lls(): Locally list the contents of a directory.
        cd(): Change the current directory (on the remote).
        pwd(): Get the current directory (on the remote).
        lcd(): Change the current directory (locally).
        lpwd(): Get the current directory (locally).
        mirror(): Download a directory from the remote.
    .. Sphinx comment>
    """

    time_stamp_pattern = re.compile(r"^\s*(\S+\s+){8}") # This is used to strip the timestamp from the output of the lftp shell
    END_OF_OUTPUT = '# LFTP_END_OF_OUTPUT_IDENTIFIER {uuid} #'  # This is used to signal the end of output when reading from stdout

    def __init__(self, user: str= None, remote: str=None, verbose: bool=main_logger.isEnabledFor(logging.DEBUG)):
        # Default argument configuration and type checking
        self.default_config = get_implicit_mount_config()
        if user is None:
            user = self.default_config['user']
        if remote is None:
            remote = self.default_config['remote']
        if not isinstance(user, str):
            raise TypeError("Expected str, got {}".format(type(user)))
        if not isinstance(remote, str):
            raise TypeError("Expected str, got {}".format(type(remote)))
        if not isinstance(verbose, bool):
            raise TypeError("Expected bool, got {}".format(type(verbose)))
        
        # Set attributes
        self.user = user
        self.password = "" # Assume we use passwordless lftp (authentication is handled by ssh keys, not sftp)
        self.remote = remote
        self.lftp_shell = None
        self.verbose = verbose

        self.stdout_queue = Queue()
        self.stderr_queue = Queue()
        self.lock = threading.Lock() 

    @staticmethod
    def format_options(**kwargs) -> str:
        """
        Takes a dictionary of options and formats them into a string of command line arguments suitable for LFTP.

        Args:
            **kwargs: Keyword arguments to format.

        Returns:
            str: The formatted arguments.
        """
        options = []
        for key, value in kwargs.items():
            # main_logger.debug(f'key: |{key}|, value: |{value}|')
            prefix = "-" if len(key) == 1 else "--"
            this_option = f"{prefix}{key}"
            
            if value is not None and value != "":
                this_option += f" {str(value)}"
                
            options.append(this_option)
            
        options = " ".join(options)
        return options
    
    def _readerthread(self, stream, queue: Queue):  # No longer static
        while True:
            output = stream.readline()
            if output:
                queue.put(output)
            else:
                break

    def _read_stdout(self, timeout: float = 0, strip_timestamp: bool = True, uuid_str: str = None) -> List[str]:
        EoU = self.END_OF_OUTPUT.format(uuid=uuid_str)
        lines = []
        start_time = time.time()
        while True:
            if timeout and (time.time() - start_time > timeout):
                raise TimeoutError("Timeout while reading stdout")
            if not self.stdout_queue.empty():
                line = self.stdout_queue.get()
                if EoU in line:
                    break
                if strip_timestamp:
                    line = re.sub(self.time_stamp_pattern, "", line)
                # if not line.startswith("wait "):
                if self.verbose:
                    main_logger.info(line.strip())
                lines.append(line.strip())
            else:
                err = self._read_stderr()
                if err and not err.startswith("wait: no current job"):
                    raise Exception(f"Error while executing command: {err}")
                time.sleep(0.001)  # Moved to the else clause
        return lines

    def _read_stderr(self) -> str:
        errors = []
        while not self.stderr_queue.empty():
            errors.append(self.stderr_queue.get())
        return ''.join(errors)
    
    def execute_command(self, command: str, output: bool=True, blocking: bool=True, execute: bool=True, default_args: Union[dict, None]=None, **kwargs) -> Union[str, List[str], None]:
        """
        Executes a command on the LFTP shell.

        Args:
            command (str): The command to execute.
            output (bool): If True, the function will return the output of the command. 
            blocking (bool): If True, the function will block until the command is complete. If output is True, blocking must also be True.
            execute (bool): If True, the function will execute the command, otherwise it will return the command as a string.
            default_args (Union[dict, None]): A dictionary of default arguments to use for the command. If None, no default arguments will be used.
            **kwargs: Keyword arguments to pass to the command.

        Returns:
            Union[str, List[str], None]: If execute is False, the function will return the command as a string. If output is True, returns a string, list or None depending of the number of output lines, otherwise returns None.
        """
        # Merge default arguments and keyword arguments. Keyword arguments will override default arguments.
        if not isinstance(default_args, dict) and default_args is not None:
            raise TypeError("Expected dict or None, got {}".format(type(default_args)))
        
        # Combine default arguments and keyword arguments
        if default_args is None:
            default_args = {}
        # Remove optional "uuid_str" argument from kwargs
        if "uuid_str" in kwargs:
            uuid_str = kwargs.pop("uuid_str")
        else:
            uuid_str = None
        # Combine default arguments and kwargs
        args = {**default_args, **kwargs} 
        
        # Format arguments
        formatted_args = self.format_options(**args)

        # Combine command and arguments
        full_command = f"{command} {formatted_args}" if formatted_args else command
        if output:
            if uuid_str is None:
                uuid_str = str(uuid.uuid4())
        if not blocking:
            full_command += " &"

        if not execute:
            return full_command
        else:
            output = self._execute_command(full_command, output=output, blocking=blocking, uuid_str=uuid_str)
            if isinstance(output, list):
                return output
            elif output is None:
                return []
            else:
                raise TypeError("Expected list or None, got {}".format(type(output)))

    def _execute_command(self, command: str, output: bool=True, blocking: bool=True, uuid_str: str=None) -> Union[List[str], None]:
        """
        ## DO NOT USE THIS FUNCTION DIRECTLY, USE `ImplicitMount.execute_command` INSTEAD.

        Executes a command on the LFTP shell.

        Args:
            command (str): The command to execute.
            output (bool): If True, the function will return the output of the command. 
            blocking (bool): If True, the function will block until the command is complete. If output is True, blocking must also be True.
            uuid_str (str): A unique identifier for the command. Must be specified if output is True.

        Returns:
            Union[List[str], None]: If output is True, the function will return a list of strings each containing one line of the output of the command, otherwise it will return None.
        """
        if output and not blocking:
            raise ValueError("Non-blocking output is not supported.")
        if uuid_str is None and (blocking or output):
            uuid_str = str(uuid.uuid4())
            # raise ValueError("uuid_str must be specified if output is True.")
        if not command.endswith("\n"):
            command += "\n"
        with self.lock: 
            # TODO: Is it safe to assume that the order of the output is the same as the order of the commands? Why is it "<command> <end_of_output> wait" and not "<command> wait <end_of_output>"?
            ## Assemble command
            # Blocking and end of output logic
            if blocking or output:
                command += f"echo {self.END_OF_OUTPUT.format(uuid=uuid_str)}\n"
            if blocking:
                command += "wait\n"
            # Execute command
            if self.verbose:
                main_logger.info(f"Executing command: {command}")
            self.lftp_shell.stdin.write(command)
            self.lftp_shell.stdin.flush()
            # Read output
            if output:
                return self._read_stdout(uuid_str=uuid_str)
            elif blocking:
                self._read_stdout(uuid_str=uuid_str)
                return None

    def mount(self, lftp_settings: Union[dict, None]=None) -> None:
        """
        Mount the remote directory.

        Args:
            lftp_settings (Union[dict, None]): A dictionary of LFTP settings to use for mounting the remote directory. If None, the default settings will be used.

        Raises:
            Exception: If the subprocess fails to start.
            RuntimeError: If the connection to the remote directory fails.
        """
        # set mirror:use-pget-n 5;set net:limit-rate 0;set xfer:parallel 5;set mirror:parallel-directories true;set ftp:sync-mode off;"
        # Merge default settings and user settings. User settings will override default settings.
        lftp_settings = {**self.default_config['lftp'], **lftp_settings} if lftp_settings is not None else self.default_config['lftp']
        # Format settings
        lftp_settings_str = ""
        for key, value in lftp_settings.items():
            lftp_settings_str += f" set {key} {value};"
        if self.verbose:
            main_logger.info(f"Mounting {self.remote} as {self.user}")
        # "Mount" the remote directory using an lftp shell with the sftp protocol and the specified user and remote, and the specified lftp settings
        lftp_mount_cmd = f'open -u {self.user},{self.password} -p 2222 sftp://{self.remote};{lftp_settings_str}'
        if self.verbose:
            main_logger.info(f"Executing command: lftp")
        # Start the lftp shell
        try:
            self.lftp_shell = subprocess.Popen(
                executable="lftp",
                args=[],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=0,
            )
        except Exception as e:
            # TODO: The exception should probably be a bit more specific
            raise Exception("Failed to start subprocess.") from e

        # Start the stdout and stderr reader threads
        self.stdout_thread = threading.Thread(
            target=self._readerthread,
            args=(self.lftp_shell.stdout, self.stdout_queue)
        )
        self.stderr_thread = threading.Thread(
            target=self._readerthread,
            args=(self.lftp_shell.stderr, self.stderr_queue)
        )

        # Set the threads as daemon threads so that they will be terminated when the main thread terminates
        self.stdout_thread.daemon = True
        self.stderr_thread.daemon = True
        
        # Start the threads
        self.stdout_thread.start()
        self.stderr_thread.start()

        # Execute the mount command on the lftp shell (connect to the remote directory)
        self.execute_command(lftp_mount_cmd, output=False, blocking=False)
        self.cd(self.default_config['default_remote_dir'])

        if self.verbose:
            main_logger.info("Waiting for connection...")
        # Check if we are connected to the remote directory by executing the pwd command on the lftp shell
        connected_path = self.pwd()
        if self.verbose:
            main_logger.info(f"Connected to {connected_path}")
        if not connected_path:
            self.unmount()
            raise RuntimeError(f"Failed to connect. Check internet connection or if {self.remote} is online.")

    def unmount(self, timeout: float = 1) -> None:
        """
        Unmount the remote directory.

        Args:
            timeout (float): The maximum time to wait for the lftp shell to terminate. If the timeout is exceeded, the lftp shell will be forcefully terminated.
        """
        # Close Popen streams explicitly
        self.execute_command("exit kill top", output=False, blocking=False)
        waited = 0
        while True:
            if self.lftp_shell.poll() is not None:
                break
            time.sleep(0.1)
            waited += 0.1
            if waited > timeout:
                self.lftp_shell.terminate()
                break
        self.lftp_shell.stdout.close()
        self.lftp_shell.stdin.close()
        self.lftp_shell.stderr.close()
        self.lftp_shell = None

    def pget(self, remote_path: str, local_destination: str, blocking: bool=True, execute: bool=True, output: Union[bool, None]=None, **kwargs):
        """
        Download a single file from the remote directory using the LFTP command `pget`.

        Args:
            remote_path (str): The remote file to download.
            local_destination (str): The local destination to download the file to.
            blocking (bool): If True, the function will block until the download is complete.
            execute (bool): If True, the function will execute the pget command, otherwise it will return the command as a string.
            output (Union[bool, None]): If True, the function will return the absolute local path of the downloaded file, otherwise it will return None.

        Returns:
            Union[None, str]: If output is True, the function will return the absolute local path of the downloaded file, otherwise it will return None.
        """
        if output is None:
            output = blocking
        default_args = {'n': 5}
        args = {**default_args, **kwargs}
        formatted_args = self.format_options(**args)
        full_command = f'pget {formatted_args} "{remote_path}" -o "{local_destination}"'
        exec_output = self.execute_command(
            full_command, 
            output=output, 
            blocking=blocking,
            execute=execute
        )
        if not execute:
            return exec_output
        
        # Construct and return the absolute local path
        file_name = os.path.basename(remote_path)
        abs_local_path = os.path.abspath(os.path.join(local_destination, file_name))
        return abs_local_path
    
    def put(self, local_path: str, remote_destination: Union[str, None]=None, blocking: bool=True, execute: bool=True, output: Union[bool, None]=None, **kwargs):
        """
        Upload a single file to the remote directory using the LFTP command `put`.

        TODO: Is it really just a single file?

        Args:
            local_path (str): The local file to upload.
            remote_destination (str, optional): The remote destination to upload the file to. If None, the file will be uploaded to the current remote directory.
            blocking (bool): If True, the function will block until the upload is complete.
            execute (bool): If True, the function will execute the put command, otherwise it will return the command as a string.
            output (Union[bool, None]): If True, the function will return the absolute remote path of the uploaded file, otherwise it will return None.
            **kwargs: Keyword arguments to pass to the put command.
        """
        def source_destination(local_path: Union[str, List[str]], remote_destination: Union[str, List[str], None]=None) -> str:
            if isinstance(local_path, str):
                local_path = [local_path]
            if not isinstance(local_path, list):
                raise TypeError("Expected list or str, got {}".format(type(local_path)))
            if remote_destination is None:
                if isinstance(local_path, list):
                    remote_destination = [os.path.basename(p) for p in local_path]
                elif isinstance(local_path, str):
                    remote_destination = os.path.basename(local_path)
                else:
                    raise TypeError("Expected list or str, got {}".format(type(local_path)))
            elif isinstance(remote_destination, str):
                remote_destination = [remote_destination]
            if not isinstance(remote_destination, list):
                raise TypeError("Expected list or str, got {}".format(type(remote_destination)))
            if len(local_path) != len(remote_destination):
                raise ValueError("Expected local_path and remote_destination to have the same length, got {} and {} instead.".format(len(local_path), len(remote_destination)))
            return remote_destination, " ".join([f'"{l}" -o "{r}""'for l, r in zip(local_path, remote_destination)])
        
        if output is None:
            output = blocking
        # OBS: The online manual for LFTP is invalid for put (at least on ERDA); the included "P" option for the put command does not exist
        default_args = {}
        args = {**default_args, **kwargs}
        formatted_args = self.format_options(**args)
        remote_destination, src_to_dst = source_destination(local_path, remote_destination)
        full_command = f"put {formatted_args} {src_to_dst}"
        exec_output = self.execute_command(
            full_command, 
            output=output, 
            blocking=blocking,
            execute=execute
        )
        if not execute:
            return exec_output
        
        # Construct and return the absolute remote path
        file_name = os.path.basename(os.path.abspath(local_path))
        rpwd = self.pwd()
        abs_remote_path = [rpwd + "/" + r for r in remote_destination]
        return abs_remote_path

    def ls(self, path: str = ".", recursive: bool=False, use_cache: bool=True) -> List[str]:
        """
        Find all files in the given remote directory using the LFTP command `cls`. Can be used recursively, even though LFTP does not support recursive listing with the `cls` command.

        Args:
            path (str): The remote directory to search in.
            recursive (bool): If True, the function will search recursively.
            use_cache (bool): If True, the function will use `cls`, otherwise it will use `recls`. `recls` forces a refresh of the cache.

        Returns:
            Union[None, str, List[str]]: If the directory is empty, the function will return None, if the directory contains one file, the function will return a string, otherwise it will return a list of strings.
        """
        if path.startswith(".."):
            raise NotImplementedError("ls does not support relative backtracing paths yet.")
        elif path.startswith("./"):
            path = path[2:]
        elif path == ".":
            path = ""
        
        # This function is used to sanitize the output of the lftp shell
        # It is quite inefficient, but it is only used for the ls command, which is not performance critical?
        # Folder index files should be used instead of ls in most cases, 
        # but this function is still useful for debugging and for creating the folder index files
        def sanitize_path(l, path) -> None:
            # Empty case (base case 1)
            if not path:
                pass
            # Single path case (base case 2)
            elif isinstance(path, str):
                # Skip "." and ".." paths and folder index files (these are created by the folder index command, and should be treated as hidden files)
                if len(path) >= 2 and not "folder_index.txt" in path:
                    # Remove leading "./" from paths
                    if path.startswith("."):
                        path = path[2:]
                    # Append path to list (this a mutative operation)
                    l += [path]
            # Multiple paths case (recursive case)
            elif isinstance(path, list):
                for p in path:
                    sanitize_path(l, p)
            else:
                raise TypeError("Expected list, str or None, got {}".format(type(path)))
            return l
        
        # Recursive ls is implemented by using the "cls" command, which returns a list of permissions and paths
        # and then recursively calling ls on each of the paths that are directories, 
        # which is determined by checking if the permission starts with "d"
        if recursive:
            recls = "" if use_cache else "re"
            this_level = self.execute_command(f'{recls}cls "{path}" -1 --perm')
            # If the directory contains one or no files
            if isinstance(this_level, str) or this_level is None:
                this_level = sanitize_path([], this_level)
            output = []
            for perm_path in this_level:
                if not " " in perm_path:
                    continue
                perm, path = perm_path.split(" ", 1)
                if perm.startswith("d"):
                    output += self.ls(path, recursive=True)
                else:
                    sanitize_path(output, path)
        # Non-recursive case
        else:
            output = sanitize_path([], self.execute_command(f'cls "{path}" -1'))

        # Check if the output is a list
        if not isinstance(output, list):
            TypeError("Expected list, got {}".format(type(output)))
        
        return output
    
    def lls(self, local_path: str, **kwargs) -> List[str]:
        """
        Find all files in the given local directory using the LFTP command `!ls` or `!find`.

        OBS: This function should probably not be used, just use the standard OS commands instead.
        """
        recursive = kwargs.get("R", kwargs.get("recursive", False))
        if recursive:
            if local_path == "":
                local_path = "."
            output = self.execute_command(f'!find "{local_path}" -type f -exec realpath --relative-to="{local_path}" {{}} \\;')
        else: 
            output = self.execute_command(f'!ls "{local_path}"', **kwargs)
        
        # Check if the output is a list
        if not isinstance(output, list):
            raise TypeError("Expected list, got {}".format(type(output)))
        
        return output

    def cd(self, remote_path: str, **kwargs):
        self.execute_command(f'cd "{remote_path}"', output=False, **kwargs)

    def pwd(self) -> str:
        """
        Get the current remote directory using the LFTP command `pwd`.

        Returns:
            str: The current remote directory.
        """
        output = self.execute_command("pwd")
        if isinstance(output, list) and len(output) == 1:
            return output[0]
        else:
            raise TypeError("Expected list of length 1, got {}: {}".format(type(output), output))

    def lcd(self, local_path: str) -> str:
        """
        Changes the current local directory using the LFTP command `lcd`.

        Args:
            local_path (str): The local directory to change to.
        """
        self.execute_command(f"lcd {local_path}", output=False)

    def lpwd(self) -> str:
        """
        Get the current local directory using the LFTP command `lpwd`.

        Returns:
            str: The current local directory.
        """
        output = self.execute_command("lpwd")
        if isinstance(output, list) and len(output) == 1:
            return output[0]
        else:
            raise TypeError("Expected list of length 1, got {}: {}".format(type(output), output))
    
    def _get_current_files(self, dir_path: str) -> List[str]:
        return self.lls(dir_path, R="")

    def mirror(self, remote_path: str, local_destination: str, blocking: bool=True, execute: bool=True, do_return: bool=True, **kwargs) -> Union[None, List[str]]:
        """
        Download a directory from the remote directory to the given local destination using the LFTP mirror command.

        Args:
            remote_path (str): The remote directory to download.
            local_destination (str): The local destination to download the directory to.
            blocking (bool): If True, the function will block until the download is complete.
            execute (bool): If True, the function will execute the mirror command, otherwise it will return the command as a string.
            do_return (bool): If True, the function will return a list of the newly downloaded files.
            **kwargs: Keyword arguments to pass to the mirror command.

        Returns:
            Union[None, List[str]]: If do_return is True, the function will return a list of the newly downloaded files, otherwise it will return None.
        """
        if do_return:
            # Capture the state of the directory before the operation
            pre_existing_files = self._get_current_files(local_destination)
            # Ensure that the pre_existing_files list is unique
            pre_existing_files = set(pre_existing_files)

        # Execute the mirror command
        default_args = {'P': 5, 'use-cache': None}
        exec_output = self.execute_command(
            f'mirror "{remote_path}" "{local_destination}"', 
            output=blocking, 
            blocking=blocking, 
            execute=execute,
            default_args=default_args, 
            **kwargs
        )
        if not execute:
            return exec_output
        
        if do_return:
            # Capture the state of the directory after the operation
            post_download_files = self._get_current_files(local_destination)
            # Ensure that the post_download_files list is unique
            post_download_files = set(post_download_files)

            # Calculate the set difference to get the newly downloaded files
            new_files = post_download_files - pre_existing_files
            
            return list(new_files)
        else:
            return None

class IOHandler(ImplicitMount):
    """
    This is a high-level wrapper for the ImplicitMount class, which provides human-friendly methods for downloading files from a remote directory without the need to have technical knowledge on how to use LFTP.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    Args:
        local_dir (str): The local directory to use for downloading files. If None, a temporary directory will be used (suggested, unless truly necessary).
        user_confirmation (bool): If True, the user will be asked for confirmation before deleting files. (strongly suggested for debugging and testing)
        clean (bool): If True, the local directory will be cleaned after the context manager is exited. (suggested, if not it may lead to rapid exhaustion of disk space)
        **kwargs: Keyword arguments to pass to the ImplicitMount constructor.

    .. <Sphinx comment
    Methods:
        download(): Download the given remote path to the given local destination.
        multi_download(): Download the given remote paths to the given local destinations.
        clone(): Clone the current remote directory to the given local destination.
        get_file_index(): Get a list of files in the current directory.
        cache_file_index(): Cache the file index for the current directory.
        store_last(): TODO: NOT IMPLEMENTED! Move the last downloaded file or directory to the given destination.
        clean(): Clean the local directory.
        clean_last(): Clean the last downloaded file or directory.
    .. Sphinx comment>
    """
    def __init__(self, local_dir: Union[str, None]=None, user_confirmation: bool=False, clean: bool=False, **kwargs):
        super().__init__(**kwargs)
        if local_dir is None:
            if self.default_config['local_dir'] is None:
                local_dir = tempfile.TemporaryDirectory().name
            else:
                local_dir = self.default_config['local_dir']
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.original_local_dir = os.path.abspath(local_dir)
        self.local_dir = local_dir
        self.user_confirmation = user_confirmation
        self.do_clean = clean
        self.last_download = None
        self.last_type = None
        self.cache = {}

    def __enter__(self) -> "IOHandler":
        self.mount()
        self.lcd(self.local_dir)

        # Print local directory:
        # if the local directory is not specified in the config, 
        # it is a temporary directory, so it is nice to know where it is located
        main_logger.debug(f"Local directory: {self.lpwd()}")

        # Return self
        return self

    def __exit__(self, *args, **kwargs):
        # Positional and keyword arguments simply catch any arguments passed to the function to be ignored
        if self.do_clean:
            self.clean()
        self.unmount()

    # Methods for using the IOHandler without context management
    def start(self) -> None:
        """
        Initialize the connection to the remote directory.

        Very useful for interactive use, but shouldn't be used in scripts, using a context manager is safer and does the same.
        """
        self.__enter__()

        main_logger.warning("IOHandler.start() is unsafe. Use IOHandler.__enter__() instead if possible.")
        main_logger.warning("OBS: Remember to call IOHandler.stop() when you are done.")

    def stop(self) -> None:
        """
        Close the connection to the remote directory.
        """
        self.__exit__()

    def download(self, remote_path: Union[str, List[str]], local_destination: Union[str, List[str], None]=None, blocking: bool=True, **kwargs) -> Union[str, List[str]]:
        """
        Downloads one or more files or a directory from the remote directory to the given local destination.

        Args:
            remote_path (Union[str, List[str]]): The remote path(s) to download.
            local_destination (Union[str, List[str]]): The local destination to download the file(s) to. If None, the file(s) will be downloaded to the current local directory.
            blocking (bool): If True, the function will block until the download is complete.
            **kwargs: Extra keyword arguments are passed to the IOHandler.multi_download, IOHandler.pget or IOHandler.mirror functions depending on the type of the remote path(s).

        Returns:
            (List of) The local path of the downloaded file(s) or directory.
        """
        # If multiple remote paths are specified, use multi_download instead of download, 
        # this function is more flexible than mirror (works for files from different directories) and much faster than executing multiple pget commands
        if not isinstance(remote_path, str) and len(remote_path) > 1:
            return self.multi_download(remote_path, local_destination, **kwargs)
        if len(remote_path) == 1:
            remote_path = remote_path[0]
            if not isinstance(remote_path, str):
                raise TypeError("Expected str, got {}".format(type(remote_path)))
        
        if local_destination is None:
            local_destination = self.lpwd()

        # Check if remote and local have file extensions:
        # The function assumes files have extensions and directories do not.
        remote_has_ext, local_has_ext = os.path.splitext(remote_path)[1] != "", os.path.splitext(local_destination)[1] != ""
        # If both remote and local have file extensions, the local destination should be a file path.
        if remote_has_ext and local_has_ext and os.path.isdir(local_destination):
            raise ValueError("Destination must be a file path if both remote and local have file extensions.")
        # If the remote does not have a file extension, the local destination should be a directory.
        if not remote_has_ext and not os.path.isdir(local_destination):
            raise ValueError("Destination must be a directory if remote does not have a file extension.")
        
        # Download cases;
        # If the remote is a single file, use pget.
        if remote_has_ext:
            local_result = self.pget(remote_path, local_destination, blocking, **kwargs)
            self.last_type = "file"
        # Otherwise use mirror.
        else:
            if not os.path.exists(local_destination):
                try:
                    os.makedirs(local_destination)
                except FileExistsError:
                    pass
            local_result = self.mirror(remote_path, local_destination, blocking, **kwargs)
            self.last_type = "directory"
        
        # TODO: Check local_result == local_destination (if it can be done in a relatively efficient way)

        # Store the last download for later use (nice for debugging)
        self.last_download = local_result
        # Return the local path of the downloaded file or directory
        return local_result
    
    def multi_download(self, remote_paths: List[str], local_destination: Union[str, List[str]], blocking: bool=True, n: int=5, **kwargs) -> List[str]:
        """
        Downloads a list of files from the remote directory to the given local destination.

        Args:
            remote_paths (List[str]): A list of remote paths to download.
            local_destination (Union[str, List[str]]): The local destination to download the files to. If None, the files will be downloaded to the current local directory.
            blocking (bool): If True, the function will block until the download is complete.
            n (int): The number of connections to use for downloading each file.
            **kwargs: Extra keyword arguments are ignored.
        
        Returns:
            A list of the local paths of the downloaded files.
        """
        # TODO: This function should really wrap an IOHandler.mget function, which should be implemented in the ImplicitMount class
        # Type checking and default argument configuration
        if not isinstance(remote_paths, list):
            raise TypeError("Expected list, got {}".format(type(remote_paths)))
        if not (isinstance(local_destination, str) or isinstance(local_destination, list) or local_destination is None):
            raise TypeError("Expected str or list, got {}".format(type(local_destination)))
        if isinstance(local_destination, str):
            local_destination = [local_destination + os.sep + os.path.basename(r) for r in remote_paths]
        elif local_destination is None:
            local_destination = self.lpwd()
            local_destination = [local_destination + os.sep + os.path.basename(r) for r in remote_paths]
        if len(remote_paths) != len(local_destination):
            raise ValueError("remote_paths and local_destination must have the same length.")
        if any([os.path.splitext(l)[1] != os.path.splitext(r)[1] for l, r in zip(local_destination, remote_paths)]):
            raise ValueError("Local and remote file extensions must match.")
        
        # Assemble the mget command, options and arguments
        multi_command = f'mget -P {n} ' + ' '.join([f'"{r}"' for r in remote_paths])
        # Execute the mget command
        self.execute_command(multi_command, output=blocking, blocking=blocking)
        # Check if the files were downloaded TODO: is this too slow? Should we just assume that the files were downloaded for efficiency?
        for l in local_destination:
            if not os.path.exists(l):
                raise RuntimeError(f"Failed to download {l}")

        # Store the last download for later use (nice for debugging)
        self.last_download = local_destination
        self.last_type = "multi"
        # Return the local paths of the downloaded files
        return local_destination

    def clone(self, local_destination: Union[None, str], blocking: bool=True, **kwargs) -> any:
        """
        Clones the current remote directory to the given local destination.

        Args:
            local_destination (str): The local destination to clone the current remote directory to.
            blocking (bool): If True, the function will block until the download is complete.
            **kwargs: Keyword arguments to pass to the mirror function.
        
        Returns:
            The output of ImplicitMount.mirror, which depend on the arguments passed to the function. Most likely a list of the newly downloaded files.
        """
        if not isinstance(local_destination, str) and local_destination is not None:
            raise TypeError("Expected str or None, got {}".format(type(local_destination)))
        if local_destination is None:
            local_destination = self.lpwd()
        local_destination = os.path.abspath(local_destination + os.sep + self.pwd().split("/")[-1])
        if not os.path.exists(local_destination):
            try:
                os.makedirs(local_destination)
            except FileExistsError:
                pass
        return self.mirror(".", local_destination, blocking, **kwargs)
    
    def get_file_index(self, skip: int=0, nmax: Union[int, None]=None, override: bool=False, store: bool=True, pattern : Union[None, str]= None) -> List[str]:
        """
        Get a list of files in the current remote directory.

        Args:
            skip (int): The number of files to skip.
            nmax (int): The maximum number of files to include.
            override (bool): If True, the file index will be overridden if it already exists.
            store (bool): If True, the file index will be stored on the remote directory.
            pattern (str): A regular expression pattern to filter the file names by, e.g. "\\.txt$" to only include files with the ".txt" extension.
        Returns:
            A list of files in the current remote directory.
        """
        if override and not store:
            raise ValueError("override cannot be 'True' if store is 'False'!")
        # Check if file index exists
        file_index_exists = self.execute_command('glob -f --exist *folder_index.txt && echo "YES" || echo "NO"') == "YES"
        if not file_index_exists and self.verbose:
            main_logger.debug(f"Folder index does not exist in {self.pwd()}")
        # If override is True, delete the file index if it exists
        if override and file_index_exists:
            assert store is False, RuntimeError(f"'override' is '{override}' and 'store' is '{store}', this is not allowed and should not happen!")
            self.execute_command("rm folder_index.txt")
            # Now the file index does not exist (duh)
            file_index_exists = False
        # If the file index does not exist, create it
        if not file_index_exists:
            main_logger.debug("Creating folder index...")
            # Traverse the remote directory and write the file index to a file
            files = self.ls(recursive=True, use_cache=False)
            local_index_path = f"{self.lpwd()}folder_index.txt"
            with open(local_index_path, "w") as f:
                for file in files:
                    f.write(file + "\n")
            # Store the file index on the remote if 'store' is True, otherwise delete it
            if store:
                # Self has an implicit reference to the local working directory, however the scripts does not necessarily have the same working directory
                self.put("folder_index.txt")
                os.remove(local_index_path)
        
        # Download the file index if 'store' is True or it already exists on the remote, otherwise read it from the local directory
        if store or file_index_exists:
            file_index_path = self.download("folder_index.txt")
        else:
            file_index_path = local_index_path
        # Read the file index
        file_index = []
        with open(file_index_path, "r") as f:
            for i, line in enumerate(f):
                if i < skip:
                    continue
                if nmax is not None and i >= (skip + nmax):
                    break
                if len(line) < 3 or "folder_index.txt" == line[:16]:
                    continue
                if pattern is not None and re.search(pattern, line) is None:
                    continue
                file_index.append(line.strip())
        # Delete the file index if 'store' is True, otherwise return the path to the file index
        if not store:
            os.remove(file_index_path)
        return file_index
    
    def cache_file_index(self, skip: int=0, nmax: Union[int, None]=None, override: bool=False) -> None:
        self.cache["file_index"] = self.get_file_index(skip, nmax, override)

    def clean(self):
        if self.user_confirmation:
            # Ask for confirmation
            confirmation = input(f"Are you sure you want to delete all files in the current directory {self.lpwd()}? (y/n)")
            if confirmation.lower() != "y":
                main_logger.debug("Aborted")
                return

        main_logger.debug("Cleaning up...")
        for path in os.listdir(self.lpwd()):
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except:
                    main_logger.debug("Error")
        try:
            shutil.rmtree(self.lpwd())
        except Exception as e:
            main_logger.error("Error while cleaning local backend directory!")
            files_in_dir = os.listdir(self.lpwd())
            if files_in_dir:
                n_files = len(files_in_dir)
                main_logger.error(f"{n_files} files in local directory ({self.lpwd()}):")
                if n_files > 5:
                    main_logger.error("\t" + "\n\t".join(files_in_dir[:5]) + "\n\t...")
                else:
                    main_logger.error("\t" + "\n\t".join(files_in_dir))
            raise e

class RemotePathIterator:
    """
    This function provides a high-level buffered iterator for downloading files from a remote directory.
    All heavy computation is done in a separate thread, to avoid blocking the main thread unnecessarily.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    Args:
        io_handler (IOHandler): A backend object of class "IOHandler" to use for downloading files.
        batch_size (int): The number of files to download in each batch. Larger batches are more efficient, but may cause memory issues.
        batch_parallel (int): The number of files to download in parallel in each batch. Larger values may be more efficient, but can cause excessive loads on the remote server.
        max_queued_batches (int): The batches are processed sequentially from a queue, which is filled on request. This parameter specifies the maximum number of batches in the queue. Larger values can ensure a stable streaming rate, but may require more files to be stored locally.
        n_local_files (int): The number of files to store locally. OBS: This MUST be larger than batch_size * max_queued_batches (I suggest twice that), otherwise files may be deleted before they are consumed. 
        clear_local (bool): If True, the local directory will be cleared after the iterator is stopped.
        **kwargs: Keyword arguments to pass to the IOHandler.get_file_index() function. Set 'store' to False to avoid altering the remote directory (this is much slower if you intent to use the iterator multiple times, however it may be necessary if the remote directory is read-only). PSA: If 'store' is False, 'override' must also be False.

    Yields:
        Tuple[str, str]: A tuple containing the local path and the remote path of the downloaded file.

    .. <Sphinx comment
    Methods:
        shuffle(): Shuffle the remote paths.
        subset(): Subset the remote paths.
        split(): Split the remote paths into multiple iterators, that share the same backend. These CANNOT be used in parallel. TODO: Can they be used concurrently?
        download_files(): Download files in batches.
    .. Sphinx comment> 
    """
    def __init__(self, io_handler: "IOHandler", batch_size: int=64, batch_parallel: int=10, max_queued_batches: int=3, n_local_files: int=2*3*64, clear_local: bool=False, **kwargs):
        self.io_handler = io_handler
        if "file_index" not in self.io_handler.cache:
            self.remote_paths = self.io_handler.get_file_index(**kwargs)
        else:
            if kwargs:
                main_logger.warning(f'Using cached file index. [{", ".join(kwargs.keys())}] will be ignored.')
            self.remote_paths = self.io_handler.cache["file_index"]
        self.temp_dir = self.io_handler.lpwd()
        self.batch_size = batch_size
        self.batch_parallel = batch_parallel
        self.max_queued_batches = max_queued_batches
        self.n_local_files = n_local_files
        if self.n_local_files < self.batch_size:
            main_logger.warning(f"n_local_files ({self.n_local_files}) is less than batch_size ({self.batch_size}). This may cause files to be deleted before they are consumed. Consider increasing n_local_files. Recommended value: {2 * self.batch_size * self.max_queued_batches}")
        self.idx = 0
        self.download_queue = Queue()
        self.delete_queue = Queue()
        self.stop_requested = False
        self.not_cleaned = True
        self.clear_local = clear_local

        # State variables
        self.download_thread = None
        self.last_item = None
        self.last_batch_consumed = 0
        self.consumed_files = 0

    def shuffle(self) -> None:
        """
        Shuffle the remote paths.

        Shuffles the remote paths in-place. This function should not be called while iterating.
        """
        if self.download_thread is not None:
            raise RuntimeError("Cannot shuffle while iterating.")
        shuffle(self.remote_paths)

    def subset(self, indices: List[int]) -> None:
        """
        Subset the remote paths.

        Subsets the remote paths in-place. This function should not be called while iterating.

        Args:
            indices (List[int]): A list of indices to keep.
        """
        if self.download_thread is not None:
            raise RuntimeError("Cannot subset while iterating.")
        # TODO: It is fine that this works with a list of indices, but it should also work with a single index or a slice.
        self.remote_paths = [self.remote_paths[i] for i in indices]

    def split(self, proportion: Union[float, None]=None, indices: Union[List[List[int]], None]=None) -> List["RemotePathIterator"]:
        """
        Split the remote paths into multiple iterators, that share the same backend. These CANNOT be used in parallel. 

        Either, but not both, of proportion and indices must be specified.

        Args:
            proportion (Union[float, None]): A list of proportions to split the remote paths into. If None, indices must be specified.
            indices (Union[List[List[int]], None]): A list of lists of indices to split the remote paths into. If None, proportion must be specified.

        Returns:
            List[RemotePathIterator]: A list of RemotePathIterator objects.
        """
        if self.download_thread is not None:
            raise RuntimeError("Cannot split while iterating.")
        if proportion is None and indices is None:
            raise ValueError("Either proportion or indices must be specified.")
        if proportion is not None and indices is not None:
            raise ValueError("Only one of proportion or indices must be specified.")
        if proportion is not None:
            if not isinstance(proportion, list):
                raise TypeError("proportion must be a list.")
            if any([not isinstance(i, float) for i in proportion]):
                raise TypeError("All proportions must be floats.")
            if any([i < 0 or i > 1 for i in proportion]):
                raise ValueError("All proportions must be between 0 and 1.")
            if sum(proportion) != 1:
                proportion = [p / sum(proportion) for p in proportion]
            # Assume we have the correct imports from random already
            from random import choices, choice
            allocation = choices(list(range(len(proportion))), weights=proportion, k=len(self.remote_paths))
            indices = [[] for _ in range(len(proportion))]
            for i, a in enumerate(allocation):
                indices[a].append(i)
        if indices is not None:
            if not isinstance(indices, list):
                raise TypeError("indices must be a list.")
            if any([not isinstance(i, list) for i in indices]):
                raise TypeError("indices must be a list of lists.")
            if any([any([not isinstance(j, int) for j in i]) for i in indices]):
                raise TypeError("indices must be a list of lists of ints.")
            if any([any([j < 0 or j >= len(self.remote_paths) for j in i]) for i in indices]):
                raise ValueError("indices must be a list of lists of ints in the range [0, len(remote_paths)).")
            if any([len(i) == 0 for i in indices]):
                raise ValueError("All indices must be non-empty.")
        else:
            raise RuntimeError("This should never happen.")
            
        iterators = []
        for i in indices:
            this = RemotePathIterator(self.io_handler, batch_size=self.batch_size, batch_parallel=self.batch_parallel, max_queued_batches=self.max_queued_batches, n_local_files=self.n_local_files)
            this.subset(i)
            iterators.append(this)

        return iterators

    def download_files(self):
        """
        Download the entire list of remote paths in batches, and stores the local paths in a queue (self.download_queue).

        The function is not intended to be called directly, but there is no good reason why it should not and useful for debugging and testing.
        """
        queued_batches = 0
        for i in range(0, len(self.remote_paths), self.batch_size):
            if self.stop_requested:
                break
                
            while queued_batches >= self.max_queued_batches and not self.stop_requested:
                # Wait until a batch has been consumed (or multiple batches, if the consumer is fast and the producer is slow) before downloading another batch
                if self.last_batch_consumed > 0:
                    self.last_batch_consumed -= 1
                    break
                time.sleep(0.2)  # Wait until a batch has been consumed

            batch = self.remote_paths[i:i + self.batch_size]
            try:
                local_paths = self.io_handler.download(batch, n = self.batch_parallel)
            except Exception as e:
                main_logger.error(f"Failed to download batch {i} - {i + self.batch_size}: {e}")
                main_logger.warning("Skipping batch...")
                continue
            for local_path, remote_path in zip(local_paths, batch):
                self.download_queue.put((local_path, remote_path))
                
            queued_batches += 1
            
            # Deletion logic moved to __next__ to maintain minimal queued files

    def start_download_queue(self) -> None:
        """
        Creates a new thread and call the self.download_files() function in the new thread.

        Ensures non-blocking download of the files in self.remote_paths.
        """
        self.download_thread = threading.Thread(target=self.download_files)
        self.download_thread.start()

    def __iter__(self) -> "RemotePathIterator":
        # Force reset state
        self.not_cleaned = True
        self.__del__(force=True)
        self.idx = 0

        # Prepare state for iteration
        self.stop_requested = False
        self.not_cleaned = True
        
        # Start the download thread
        self.start_download_queue()

        # Return self
        return self
    
    def __len__(self) -> int:
        return len(self.remote_paths)

    def __next__(self) -> Tuple[str, str]:
        """
        Returns: 
            Tuple[str, str]: Tuple of the local path and the remote path.
        """
        # Handle stop request and end of iteration
        if self.stop_requested or self.idx >= len(self.remote_paths):
            if self.clear_local:
                self.__del__()
            self.stop_requested = False
            raise StopIteration

        # Delete files if the queue is too large
        while self.delete_queue.qsize() > self.n_local_files:
            try:
                os.remove(self.delete_queue.get())
            except Exception as e:
                main_logger.warning(f"Failed to remove file: {e}")

        # Get next item from queue or raise error if queue is empty
        try:
            next_item = self.download_queue.get() # Timeout not applicable, since there is no guarantees on the size of the files or the speed of the connection
            # Update state to ensure that the producer keeps the queue prefilled
            # It is a bit complicated because the logic must be able to handle the case where the consumer is faster than the producer,
            # in this case the producer may be multiple batches behind the consumer.
            self.consumed_files += 1
            if self.consumed_files >= self.batch_size:
                self.consumed_files -= self.batch_size
                self.last_batch_consumed += 1
        except queue.Empty: # TODO: Can this happen?
            if self.stop_requested:
                self.__del__()
                raise StopIteration
            else:
                raise RuntimeError("Download queue is empty but no stop was requested. Check the download thread.")

        # Update state
        self.idx += 1
        self.delete_queue.put(next_item[0])
        
        # Return next item (local path, remote path => can be parsed to get the class label)
        return next_item

    def __del__(self, force=False) -> None:
        if self.not_cleaned or force:
            # Force the iterator to stop if it is not already stopped
            self.stop_requested = True
            # Wait for the download thread to finish
            if self.download_thread is not None:
                self.download_thread.join(timeout=1)
                self.download_thread = None
            # Clean up the temporary directory
            while not self.download_queue.empty():
                f = self.download_queue.get()
                try:
                    os.remove(f)
                except Exception as e:
                    main_logger.warning(f"Failed to remove file ({f}): {e}")
            if self.clear_local:
                while not self.delete_queue.empty():
                    f = self.delete_queue.get()
                    try:
                        os.remove(f)
                    except Exception as e:
                        main_logger.warning(f"Failed to remove file ({f}): {e}")
            else:
                with self.delete_queue.mutex:
                    self.delete_queue.queue.clear()
            
            # Remove any remaining files in the temporary directory
            for f in os.listdir(self.temp_dir):
                if "folder_index.txt" in f:
                    continue
                try:
                    os.remove(f)
                except:
                    main_logger.warning(f"Failed to remove file: {f}")
                    pass

            ## TODO: DOUBLE CHECK - THIS SHOULD NOT BE NECESSARY (it is at the moment though!)
            # Check if the download thread is still running
            if self.download_thread is not None:
                main_logger.error("Download thread is still running. This should not happen.")
                self.stop_requested = True
                self.download_thread.join(timeout=1)
                self.download_thread = None
            # Check if the download queue is empty
            if not self.download_queue.empty():
                main_logger.error("Download queue is not empty. This should not happen.")
                with self.download_queue.mutex:
                    self.download_queue.queue.clear()
            # Check if the delete queue is empty
            if not self.delete_queue.empty() and self.clear_local:
                main_logger.error("Delete queue is not empty. This should not happen.")
                with self.download_queue.mutex:
                    self.download_queue.queue.clear()
        else:
            main_logger.debug("Already cleaned up")