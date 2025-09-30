"""
This module provides a pythonic interface for downloading files from a remote directory using the SFTP protocol.

The main functionality of this package is provided through the use of the ImplicitMount, IOHandler and RemotePathIterator classes:

* The **ImplicitMount** class provides a low-level wrapper for the LFTP shell, which is used to communicate with the remote directory, and should only be used directly by advanced users.

* The **IOHandler** class provides a high-level wrapper for the ImplicitMount class, which provides human-friendly methods for downloading files from a remote directory without the need to have technical knowledge on how to use LFTP.

* The **RemotePathIterator** class provides a high-level wrapper for the IOHandler class, and handles asynchronous streaming of files from a remote directory to a local directory using thread-safe buffers.
"""

import logging
import os
import queue
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import uuid
from queue import Queue
from random import choices, shuffle
from tqdm.auto import trange

from pyremotedata import CLEAR_LINE, ESC_EOL, main_logger
from pyremotedata.config import get_implicit_mount_config

BENIGN_ERR = re.compile("(wait|fg): no current job")

def delete_file_or_dir(
        path : str | os.PathLike[str],
        *,
        missing_ok : bool=True,
        force : bool=False,
    ) -> None:
    p = os.fspath(path)
    try:
        st = os.lstat(p)
    except FileNotFoundError:
        if missing_ok:
            return
        raise

    mode = st.st_mode

    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        _unlink(p, force)
        return

    with os.scandir(p) as it:
        for entry in it:
            delete_file_or_dir(entry.path, missing_ok=missing_ok, force=force)
    try:
        os.rmdir(p)
    except PermissionError:
        if not force:
            raise
        os.chmod(p, st.st_mode | stat.S_IWUSR)
        os.rmdir(p)

def _unlink(p : str, force : bool) -> None:
    try:
        os.unlink(p)
    except PermissionError:
        if not force:
            raise
        os.chmod(p, stat.S_IWUSR)
        os.unlink(p)

class ImplicitMount:
    """
    This is a low-level wrapper of LFTP, which provides a pythonic interface for executing LFTP commands and reading the output.
    It provides a robust and efficient backend for communicating with a remote storage server using the SFTP protocol, using a persistent LFTP shell handled in the background by a subprocess.
    It is designed to be used as a base class for higher-level wrappers, such as the :py:class:`pyremotedata.implicit_mount.IOHandler` class, or as a standalone class for users familiar with LFTP.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    Args:
        user: The username to use for connecting to the remote directory.
        password: The *SFTP* password to possibly use when connecting to the remote host.
        remote: The remote server to connect to.
        port: The port to connect to (default: 2222).
        verbose: If True, print the commands executed by the class.

    .. <Sphinx comment
    Methods:
        format_options(): Format a dictionary of options into a string of command line arguments.
        execute_command(): Execute a command on the LFTP shell.
        mount(): Mount the remote directory.
        unmount(): Unmount the remote directory.
        pget(): Download a single file from the remote directory using multiple connections.
        put(): Upload a single file to the remote directory.
        ls(): list the contents of a directory (on the remote). OBS: In contrast to the other LFTP commands, this function has a lot of additional functionality, such as recursive listing and caching.
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

    def __init__(
            self, 
            user : str | None=None, 
            password : str | None=None,
            remote : str | None=None, 
            port : int=2222,
            verbose : bool=main_logger.isEnabledFor(logging.DEBUG)
        ):
        # Default argument configuration and type checking
        self.default_config = get_implicit_mount_config(validate=(isinstance(user, str) and isinstance(remote, str)))
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
        self.password = password or ""
        self.remote = remote
        self.port = port
        self.lftp_shell = None
        self.verbose = verbose

        self.stdout_queue = Queue()
        self.stderr_queue = Queue()
        self.lock = threading.Lock() 

    @staticmethod
    def format_options(**kwargs) -> str:
        """Format keyword options as LFTP command-line arguments.

        Args:
            **kwargs: Keyword arguments to format.

        Returns:
            The formatted argument string.
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
    
    def _readerthread(self, stream, queue : Queue):  # No longer static
        while True:
            output = stream.readline()
            if output:
                queue.put(output)
            else:
                break

    def _read_stdout(
            self, 
            uuid_str : str, 
            timeout : float = 0, 
            strip_timestamp : bool = True
        ) -> list[str]:
        if self.time_stamp_pattern is None or self.time_stamp_pattern == "":
            strip_timestamp = False

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
                self._check_error()
                time.sleep(0.001)  # Moved to the else clause
        self._check_error()
        return lines

    def _read_stderr(self) -> str:
        errors = []
        while not self.stderr_queue.empty():
            errors.append(self.stderr_queue.get())
        return ''.join(errors)
    
    def _check_error(self, strict : bool=True):
        self.lftp_shell.stderr.flush()
        err = self._read_stderr()
        if err:
            if re.match(BENIGN_ERR, err):
                return self._check_error()
            if strict:
                raise Exception(f"Error while executing command: {err}")
            return False
        return True

    def _execute_command(
            self, 
            command : str, 
            output : bool=True, 
            blocking : bool=True, 
            uuid_str : str | None=None
        ) -> list[str] | None:
        """Execute a command in the LFTP shell (internal).

        Notes:
            Do not use this function directly. Use
            :meth:`ImplicitMount.execute_command` instead.

        Args:
            command: The command to execute.
            output: If True, return the command output.
            blocking: If True, wait for command completion. If
                ``output`` is True, ``blocking`` must also be True.
            uuid_str: Unique identifier used for delimiting
                output. Required if ``output`` is True.

        Returns:
            Output lines if ``output`` is True, else
            None.
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
            with self.stderr_queue.mutex:
                self.stderr_queue.queue.clear()
            with self.stdout_queue.mutex:
                self.stdout_queue.queue.clear()
            self.lftp_shell.stdin.write(command)
            self.lftp_shell.stdin.flush()
            # Read output
            if output:
                return self._read_stdout(uuid_str=uuid_str)
            elif blocking:
                self._read_stdout(uuid_str=uuid_str)
                return None

    def execute_command(
            self, 
            command : str, 
            output : bool=True,
            blocking : bool=True,
            execute : bool=True,
            default_args : dict | None=None, 
            **kwargs
        ) -> str | list[str] | None:
        """Build and optionally execute an LFTP command.

        Args:
            command: The LFTP verb (and possibly arguments) to run.
            output: If True, return the command output.
            blocking: If True, wait for completion. If ``output`` is
                True, ``blocking`` must also be True.
            execute: If False, return the fully formatted command
                string instead of executing.
            default_args: Default options merged with
                ``**kwargs``. Keyword arguments override defaults.
            **kwargs: Additional options formatted via :meth:`format_options`.

        Returns:
            If ``execute`` is False, the formatted command string. 
            If ``output`` is True, a list of output lines (or
            an empty list). Otherwise, None.
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

    def mount(self, lftp_settings : dict | None=None) -> None:
        """Open a persistent LFTP session and connect to the remote host.

        Args:
            lftp_settings: LFTP settings to apply. If None,
                defaults from configuration are used.

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
        lftp_mount_cmd = f'open -u {self.user},{self.password} -p {self.port} sftp://{self.remote};{lftp_settings_str}'
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
        """Terminate the LFTP session and release resources.

        Args:
            timeout: Maximum time to wait before forcefully
                terminating the process.
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

    def pget(
            self, 
            remote_path : str, 
            local_destination : str, 
            blocking : bool=True,
            execute : bool=True,
            output : bool | None=None, 
            **kwargs
        ) -> str | None:
        """Download a single file using LFTP ``pget``.

        Args:
            remote_path: Remote file path to download.
            local_destination: Local directory to store the file.
            blocking: If True, wait for completion.
            execute: If False, return the command string instead of
                executing.
            output: If True, return the absolute local path
                of the downloaded file. If None, inferred from ``blocking``.

        Returns:
            Absolute local path if ``output`` is True, otherwise
            None. If ``execute`` is False, returns the command string.
        """
        if output is None:
            output = blocking
        # Construct and return the absolute local path
        file_name = os.path.basename(remote_path)
        abs_local_path = os.path.abspath(os.path.join(local_destination, file_name))
        if os.path.exists(abs_local_path):
            while os.path.exists(subdir := os.path.join(local_destination, str(uuid.uuid4()))):
                pass
            os.makedirs(subdir)
            return self.pget(remote_path=remote_path, local_destination=subdir, blocking=blocking, execute=execute, output=output, **kwargs)

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
        
        return abs_local_path
    
    def put(
            self,
            local_path : str,
            remote_destination : str | None=None,
            blocking : bool=True,
            execute : bool=True,
            output : bool | None=None,
            **kwargs
        ):
        """Upload file(s) using LFTP ``put``.

        Args:
            local_path: Local file path to upload.
            remote_destination: Remote destination path. If
                None, uploads to the current remote directory.
            blocking: If True, wait for completion.
            execute: If False, return the command string.
            output: If True, return absolute remote path(s).
            **kwargs: Additional options forwarded to ``put``.

        Returns:
            Command string when ``execute`` is False; otherwise, list of
            absolute remote paths of the uploaded file(s).
        """
        def source_destination(local_path: str | list[str], remote_destination: str | list[str] | None=None):
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

    def ls(
            self, 
            path : str=".",
            recursive : bool=False,
            use_cache : bool=True,
            pbar : int=0,
            _top : bool=True
        ) -> list[str]:
        """List files in a remote directory (optionally recursively).

        Uses LFTP ``cls`` (and manual traversal for recursion) to return file
        paths relative to ``path``.

        Args:
            path: Remote directory to search.
            recursive: If True, search recursively.
            use_cache: If True, use ``cls`` (cached). If False, use ``recls`` to force refresh.
            pbar: Progress heartbeat for recursive listings (0 to disable).

        Returns:
           Relative file paths.
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
                    if path.startswith("./"):
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
            if pbar:
                main_logger.info(f"{CLEAR_LINE}Retrieving file list{'.'*pbar}{ESC_EOL}")
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
                    pbar = (pbar % 10) + 1 if pbar else pbar
                    output += self.ls(path, recursive=True, pbar=pbar, _top=False)
                else:
                    sanitize_path(output, path)
        # Non-recursive case
        else:
            output = sanitize_path([], self.execute_command(f'cls "{path}" -1'))
        
        # Clear the progress bar if end of top-level
        if pbar and _top:
            main_logger.info(f"{CLEAR_LINE}{ESC_EOL}\n")

        # Check if the output is a list
        if not isinstance(output, list):
            TypeError("Expected list, got {}".format(type(output)))
        
        return output
    
    def lls(self, local_path: str = "", **kwargs) -> list[str]:
        """List files in a local directory via the LFTP shell.

        Notes:
            Prefer native Python/OS listing. This is mainly useful for
            consistency and debugging through the same LFTP session.
        """
        recursive = kwargs.get("R", kwargs.get("recursive", False))
        if not (recursive is False):
            if local_path == "":
                local_path = "."
            output = self.execute_command(f'!find "{local_path}" -type f -exec realpath --relative-to="{local_path}" {{}} \\;')
        else: 
            output = self.execute_command(f'!ls "{local_path}"', **kwargs)
        
        # Check if the output is a list
        if not isinstance(output, list):
            raise TypeError("Expected list, got {}".format(type(output)))
        
        return output

    def cd(self, remote_path : str, **kwargs):
        self.execute_command(f'cd "{remote_path}" && cd .', output=False, **kwargs)

    def pwd(self) -> str:
        """Return the current remote directory (LFTP ``pwd``)."""
        output = self.execute_command("pwd")
        if isinstance(output, list) and len(output) == 1:
            return output[0]
        else:
            raise TypeError("Expected list of length 1, got {}: {}".format(type(output), output))

    def lcd(self, local_path : str) -> str:
        """Change the current local directory (LFTP ``lcd``).

        Args:
           local_path: Local directory to change to.
        """
        self.execute_command(f"lcd {local_path}", output=False)

    def lpwd(self) -> str:
        """
        Get the current local directory using the LFTP command `lpwd`.

        Returns:
           The current local directory.
        """
        output = self.execute_command("lpwd")
        if isinstance(output, list) and len(output) == 1:
            return output[0]
        else:
            raise TypeError("Expected list of length 1, got {}: {}".format(type(output), output))
    
    def _get_current_files(self, dir_path : str) -> list[str]:
        return self.lls(dir_path, R="")

    def mirror(
            self, 
            remote_path : str,
            local_destination : str, 
            blocking : bool=True,
            execute : bool=True,
            do_return : bool=True,
            **kwargs
        ) -> list[str] | None:
        """Mirror a remote directory to a local destination (LFTP ``mirror``).

        Args:
            remote_path: Remote directory to download.
            local_destination: Local destination directory.
            blocking: If True, wait for completion.
            execute: If False, return the command string.
            do_return: If True, return the newly downloaded files.
            **kwargs: Additional options forwarded to ``mirror``.

        Returns:
           List of newly downloaded files if ``do_return`` is True, otherwise None. 
           If ``execute`` is False, returns the command string.
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
    This is a high-level wrapper for the :py:class:`pyremotedata.implicit_mount.ImplicitMount` class, which provides human-friendly methods for downloading files from a remote directory without the need to have technical knowledge on how to use LFTP.

    To avoid SSH setup use `lftp_settings = {'sftp:connect-program' : 'ssh -a -x -i <keyfile>'}, user = <USER>, remote = <REMOTE>`.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    Args:
        local_dir: The local directory to use for downloading files. If None, a temporary directory will be used (suggested, unless truly necessary).
        user_confirmation: If True, the user will be asked for confirmation before deleting files. (strongly suggested for debugging and testing)
        clean: If True, the local directory will be cleaned after the context manager is exited. (suggested, if not it may lead to rapid exhaustion of disk space)
        lftp_settings: Add any additional settings or setting overrides (see https://lftp.yar.ru/lftp-man.html). 
            The most common usecase is properly to use `lftp_settings = {'sftp:connect-program' : 'ssh -a -x -i <keyfile>'}`.
            The defaults can also be overwritten by changing the `PyRemoteData` config file.
        user: The username to use for connecting to the remote directory.
        password: The *SFTP* password to possibly use when connecting to the remote host.
        remote: The remote server to connect to.
        **kwargs: Keyword arguments to pass to the :py:class:`pyremotedata.implicit_mount.ImplicitMount` constructor.

    .. <Sphinx comment
    Methods:
        download(): Download the given remote path(s) to the given local destination(s).
        clone(): Clone the current remote directory to the given local destination.
        get_file_index(): Get a list of files in the current directory.
        cache_file_index(): Cache the file index for the current directory.
        clean(): Clean the local directory.
        clean_last(): Clean the last downloaded file or directory.
    .. Sphinx comment>
    """
    def __init__(
            self, 
            local_dir : str | None=None, 
            user_confirmation : bool=False, 
            clean: bool=False, 
            user : str | None=None, 
            password : str | None=None,
            remote : str | None=None, 
            lftp_settings : dict[str, str] | None=None,
            **kwargs
        ):
        super().__init__(
            user=user,
            password=password,
            remote=remote,
            **kwargs
        )
        if local_dir is None or local_dir == "":
            if self.default_config['local_dir'] is None or self.default_config['local_dir'] == "":
                local_dir = tempfile.TemporaryDirectory().name
            else:
                local_dir = self.default_config['local_dir']
            assert isinstance(local_dir, str)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.original_local_dir = os.path.abspath(local_dir)
        self.local_dir = self.original_local_dir
        self.user_confirmation = user_confirmation
        self.do_clean = clean
        self.lftp_settings = lftp_settings
        self.last_download = None
        self.last_type = None
        self.cache : dict[str, list[str]] = {}

    def lcd(self, local_path : str):
        self.local_dir = os.path.abspath(os.path.join(self.local_dir, local_path))
        super().lcd(self.local_dir)

    def lpwd(self):
        raise TypeError("'lpwd()' should not be used for 'IOHandler' objects, use the 'local_dir' attribute instead.")

    def __enter__(self) -> "IOHandler":
        """
        Opens the remote connection in a background shell.
        """
        self.mount(self.lftp_settings or {})
        self.lcd(self.local_dir)

        # Print local directory:
        # if the local directory is not specified in the config, 
        # it is a temporary directory, so it is nice to know where it is located
        main_logger.debug(f"Local directory: {self.local_dir}")

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

    def download(
            self, 
            remote_path : str | list[str] | tuple[str, ...],
            local_destination : str | list[str] | tuple[str, ...] | None=None,
            blocking : bool=True,
            **kwargs
        ) -> str | list[str]:
        """
        Downloads one or more files or a directory from the remote directory to the given local destination.

        Args:
            remote_path: The remote path(s) to download.
            local_destination: The local destination(s) to download the file(s) to. If None, the file(s) will be downloaded to the current local directory.
            blocking: If True, the function will block until the download is complete.
            **kwargs: Extra keyword arguments are passed to the IOHandler.multi_download, IOHandler.pget or IOHandler.mirror functions depending on the type of the remote path(s).

        Returns:
           The local path of the downloaded file(s) or directory.
        """
        if len(remote_path) == 0:
            raise ValueError(f'Downloading zero-length source is not valid: {remote_path=}')
        # If multiple remote paths are specified, use multi_download instead of download, 
        # this function is more flexible than mirror (works for files from different directories) and much faster than executing multiple pget commands
        if not isinstance(remote_path, str) and len(remote_path) > 1:
            if isinstance(local_destination, (list, tuple)) and len(set(local_destination)) == 1:
                local_destination = local_destination[0]
            if not isinstance(local_destination, (list, tuple)):
                return self._multi_download(remote_path, local_destination, **kwargs)

            # If there are multiple local destinations, split into separate subcalls
            if len(remote_path) != len(local_destination):
                raise ValueError(f'When supplying multiple local destionations, they are expected to match 1-1 with remote paths. Got {len(remote_path)=} != {len(local_destination)=}')
            result = [None for _ in range(len(remote_path))]
            reindex = {rp : i for i, rp in enumerate(remote_path)}
            if len(reindex) != len(remote_path):
                raise ValueError("Duplicate remote paths supplied.")
            for this_ldir in set(local_destination):
                this_ldir_paths = [rp for rp, ld in zip(remote_path, local_destination) if ld == this_ldir]
                if len(this_ldir_paths) == 0:
                    continue
                for rp, lp in zip(this_ldir_paths, self._multi_download(this_ldir_paths, this_ldir, **kwargs)):
                    result[reindex[rp]] = lp
            if any(map(lambda x : x is None, result)):
                raise RuntimeError("One or more files failed to download.")
            return result
        
        if not isinstance(remote_path, str) and len(remote_path) == 1:
            remote_path = remote_path[0]
        if not isinstance(remote_path, str):
            raise TypeError("Expected str, got {}".format(type(remote_path)))
        
        if local_destination is None:
            local_destination = self.local_dir
        assert isinstance(local_destination, str)

        # Check if remote and local have file extensions:
        # The function assumes files have extensions and directories do not.
        remote_has_ext, local_has_ext = os.path.splitext(remote_path)[1] != "", os.path.splitext(local_destination)[1] != ""
        # If both remote and local have file extensions, the local destination should be a file path.
        if remote_has_ext and local_has_ext and os.path.isdir(local_destination):
            raise ValueError("Destination must be a file path if both remote and local have file extensions.")
        # If the remote does not have a file extension, the local destination should be a directory.
        if not remote_has_ext and not os.path.isdir(local_destination):
            raise ValueError("Destination must be a directory if remote does not have a file extension.")
        
        if not os.path.exists(local_destination):
            os.makedirs(local_destination, exist_ok=True)
        
        # Download cases;
        # If the remote is a single file, use pget.
        if remote_has_ext:
            local_result = self.pget(remote_path, local_destination, blocking, **kwargs)
            self.last_type = "file"
        # Otherwise use mirror.
        else:
            local_result = self.mirror(remote_path, local_destination, blocking, **kwargs)
            self.last_type = "directory"
        
        # TODO: Check local_result == local_destination (if it can be done in a relatively efficient way)
        
        # Store the last download for later use (nice for debugging)
        self.last_download = local_result
        # Return the local path of the downloaded file or directory
        return local_result
    
    def _multi_download(
            self, 
            remote_paths : list[str] | tuple[str, ...],
            local_destination : str | None,
            blocking : bool=True,
            n : int=15, 
            **kwargs
        ) -> list[str]:
        """
        Downloads a list of files from the remote directory to the given local destination.

        Args:
            remote_paths: A list of remote paths to download.
            local_destination: The local destination to download the files to. If None, the files will be downloaded to the current local directory.
            blocking: If True, the function will block until the download is complete.
            n: Number of files to download in parallel.
            **kwargs: Extra keyword arguments are ignored.
        
        Returns:
           A list of the local paths of the downloaded files.
        """
        # TODO: This function should really wrap an IOHandler.mget function, which should be implemented in the ImplicitMount class
        # Type checking and default argument configuration
        if not isinstance(remote_paths, (list, tuple)):
            raise TypeError("Expected list or tuple, got {}".format(type(remote_paths)))
        if len(remote_paths) == 0:
            raise ValueError("Expected non-empty list or tuple for `remote_paths`, got {}".format(remote_paths))
        if not (isinstance(local_destination, str) or local_destination is None):
            raise TypeError("Expected str or None, got {}".format(type(local_destination)))
        if local_destination is None:
            local_destination = self.local_dir
        local_files = [os.path.join(local_destination, os.path.basename(r)) for r in remote_paths]
        if any(map(os.path.exists, local_files)):
            while os.path.exists(subdir := os.path.join(local_destination, str(uuid.uuid4()))):
                continue
            return self._multi_download(remote_paths, subdir, blocking=blocking, n=n, **kwargs)
        os.makedirs(local_destination, exist_ok=True)

        # Assemble the mget command, options and arguments
        multi_command = f'mget -O "{local_destination}" -P {max(1, min(n, len(remote_paths)))} ' + ' '.join([f'"{r}"' for r in remote_paths])
        # Execute the mget command
        self.execute_command(multi_command, output=blocking, blocking=blocking)
        
        # Check if the files were downloaded TODO: is this too slow? Should we just assume that the files were downloaded for efficiency?
        missing_files = [l for l in local_files if not os.path.exists(l)]
        if missing_files:
            raise RuntimeError(f"Failed to download files {missing_files}")

        # Store the last download for later use (nice for debugging)
        self.last_download = local_destination
        self.last_type = "multi"
        
        # Return the local paths of the downloaded files
        return local_files

    def sync(
            self, 
            local_destination : str | None=None, 
            progress : bool=False,
            batch_size : int=128,
            replace_local : bool=False,
            **kwargs
        ) -> list[str] | None:
        """
        Synchronized the current remote directory to the given local destination.

        Args:
            local_destination: The local destination to synchronize the current remote directory to.
            progress: Show a progress bar.
            batch_size: Number of files passed to each download call.
            replace_local: By default existing files are skipped, if this is enabled, existing files are deleted and refetched.
            **kwargs: Passed to IOHandler.download.
        
        Returns:
           The output of ``ImplicitMount.mirror``, which depend on the arguments passed to the function. Most likely a list of the newly downloaded files.
        """
        if not isinstance(local_destination, str) and local_destination is not None:
            raise TypeError("Expected str or None, got {}".format(type(local_destination)))
        if local_destination is None:
            local_destination = self.local_dir
        local_destination = os.path.abspath(os.path.join(local_destination, self.pwd().split("/")[-1]))
        if not os.path.exists(local_destination):
            try:
                os.makedirs(local_destination)
            except FileExistsError:
                pass
        if self.pwd() not in self.cache:
            self.cache_file_index()
        files = self.cache[self.pwd()]
        ldest = [os.path.join(local_destination, *f.split("/")) for f in files]
        ldest, files = zip(*sorted([(l, f) for l, f in zip(ldest, files) if replace_local or not os.path.exists(l)]))
        ldir = [os.path.dirname(dst) for dst in ldest]
        for bi in trange(-(-len(files)//batch_size), desc=f"Synchronizing: ({self.pwd()}) ==> ({local_destination}) ...", unit="file", unit_scale=batch_size, disable=not progress, dynamic_ncols=True):
            bs, be = bi*batch_size, min((bi+1)*batch_size, len(files))
            if replace_local:
                for ld in ldest[bs:be]:
                    if os.path.exists(ld):
                        os.remove(ld)
            lres = self.download(files[bs:be], ldir[bs:be], **kwargs)
            if isinstance(lres, str):
                lres = [lres]
            for r, e, f in zip(lres, ldest[bs:be], files[bs:be]):
                if r != e:
                    raise RuntimeError(f'Unexpected download location for {f}, expected {e}, but got {r}?!')
        return ldest
        # return self.mirror(".", local_destination, blocking, **kwargs)
    
    def get_file_index(
            self, 
            skip : int=0, 
            nmax : int | None=None,
            override : bool=False, 
            store : bool=True,
            pattern : str | None=None
        ) -> list[str]:
        """
        Get a list of files in the current remote directory.

        Args:
            skip: The number of files to skip.
            nmax: The maximum number of files to include.
            override: If True, the file index will be overridden if it already exists.
            store: If True, the file index will be stored on the remote directory.
            pattern: A regular expression pattern to filter the file names by, e.g. "\\.txt$" to only include files with the ".txt" extension.
        Returns:
           A list of files in the current remote directory.
        """
        if override and not store:
            raise ValueError("override cannot be 'True' if store is 'False'!")
        # Check if file index exists
        glob_result = self.execute_command('glob -f --exist *folder_index.txt && echo "YES" || echo "NO"')
        if isinstance(glob_result, list) and len(glob_result) == 1:
            glob_result = glob_result[0]
        file_index_exists = glob_result == "YES"
        if not file_index_exists and self.verbose:
            main_logger.debug(f"Folder index does not exist in {self.pwd()}")
        # If override is True, delete the file index if it exists
        if override and file_index_exists:
            self.execute_command("rm folder_index.txt")
            # Now the file index does not exist (duh)
            file_index_exists = False
        # If the file index does not exist, create it
        if not file_index_exists:
            main_logger.debug("Creating folder index...")
            # Traverse the remote directory and write the file index to a file
            files = self.ls(recursive=True, use_cache=False, pbar=True)
            local_index_path = os.path.join(self.local_dir, "folder_index.txt")
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
    
    def cache_file_index(
            self, 
            skip : int=0, 
            nmax : int | None=None, 
            override : bool=False
        ) -> None:
        self.cache[self.pwd()] = self.get_file_index(skip, nmax, override)

    def clean(self):
        if self.user_confirmation:
            # Ask for confirmation
            confirmation = input(f"Are you sure you want to delete all files in the current directory {self.local_dir}? (y/n)")
            if confirmation.lower() != "y":
                main_logger.debug("Aborted")
                return

        main_logger.debug("Cleaning up...")
        for path in os.listdir(self.local_dir):
            try:
                delete_file_or_dir(path)
            except Exception as e:
                main_logger.debug(f"Error while deleting {path}: {e}")
        try:
            shutil.rmtree(self.local_dir)
        except Exception as e:
            main_logger.error("Error while cleaning local backend directory!")
            files_in_dir = os.listdir(self.local_dir)
            if files_in_dir:
                n_files = len(files_in_dir)
                main_logger.error(f"{n_files} files in local directory ({self.local_dir}):")
                if n_files > 5:
                    main_logger.error("\t" + "\n\t".join(files_in_dir[:5]) + "\n\t...")
                else:
                    main_logger.error("\t" + "\n\t".join(files_in_dir))
            raise e

class RemotePathIterator:
    """Buffered iterator for streaming many remote files efficiently.

    Downloads are performed in a background thread and yielded as
    ``(local_path, remote_path)`` tuples for consumption by the caller.

    Args:
        io_handler: Active :py:class:`pyremotedata.implicit_mount.IOHandler` used for all transfers.
        batch_size: Files to download per batch. Larger batches can be
            more efficient but use more memory.
        batch_parallel: Parallel transfers per batch. Tune for fairness
            and server limits.
        max_queued_batches: Number of prefetched batches to keep queued.
            Higher values smooth throughput but require more local storage.
        n_local_files: Maximum number of local files to keep before
            deleting consumed files. Must exceed
            ``batch_size * max_queued_batches`` (2x recommended).
        clear_local: If True, delete consumed files to free space.
        retry_base_delay: Initial backoff (seconds) for single-item retry.
        retry_max_delay: Maximum backoff (seconds) per retry step.
        retry_timeout: Per-file hard timeout (seconds) before raising.
        **kwargs: Forwarded to ``IOHandler.get_file_index`` to build the file
            index. Set ``store=False`` for read-only remotes (slower on first
            run). If ``store=False``, ``override`` must also be False.

    Yields:
       `(local_path, remote_path)`: for each downloaded file.
    """
    def __init__(
            self,
            io_handler : "IOHandler",
            batch_size : int=64,
            batch_parallel : int=10,
            max_queued_batches : int=3,
            n_local_files : int=2*3*64,
            clear_local : bool=True,
            retry_base_delay : float=0.5,
            retry_max_delay : float=30.0,
            retry_timeout : float=120.0,
            **kwargs
        ):
        self.io_handler = io_handler
        if self.io_handler.pwd() not in self.io_handler.cache:
            self.remote_paths = self.io_handler.get_file_index(**kwargs)
        else:
            if kwargs:
                main_logger.warning(f'Using cached file index. [{", ".join(kwargs.keys())}] will be ignored.')
            self.remote_paths = self.io_handler.cache[self.io_handler.pwd()]
        self.remote_paths = list(self.remote_paths) # Ensure locality
        self.temp_dir = self.io_handler.local_dir
        self.batch_size = batch_size
        self.batch_parallel = batch_parallel
        self.max_queued_batches = max_queued_batches
        self.n_local_files = n_local_files
        if self.n_local_files < self.batch_size:
            main_logger.warning(f"n_local_files ({self.n_local_files}) is less than batch_size ({self.batch_size}). This may cause files to be deleted before they are consumed. Consider increasing n_local_files. Recommended value: {2 * self.batch_size * self.max_queued_batches}")
        self.download_queue = Queue()
        self.delete_queue = Queue()
        self.stop_requested = False
        self.not_cleaned = True
        self.clear_local = clear_local

        # State variables
        self.download_thread = None
        self.last_batch_consumed = 0
        self.consumed_files = 0

        # Retry/backoff configuration
        self.retry_base_delay : float = float(retry_base_delay)
        self.retry_max_delay : float = float(retry_max_delay)
        self.retry_timeout : float = float(retry_timeout)
        self._error : BaseException | None = None  # set if a file ultimately times out

    def __len__(self) -> int:
        return len(self.remote_paths)

    def shuffle(self) -> None:
        """Shuffle the internal list of remote paths in-place.

        Raises:
            RuntimeError: If called while iterating.
        """
        if self.download_thread is not None:
            raise RuntimeError("Cannot shuffle while iterating.")
        shuffle(self.remote_paths)

    def subset(self, indices : list[int]) -> None:
        """Restrict the iterator to a subset of indices (in-place).

        Args:
            indices: Indices to keep. Accepts a list, a single int,
                or a slice.

        Raises:
            RuntimeError: If called while iterating.
            TypeError: If ``indices`` has an unsupported type.
        """
        if self.download_thread is not None:
            raise RuntimeError("Cannot subset while iterating.")
        if isinstance(indices, int):
            indices = [indices]
        if isinstance(indices, list):
            self.remote_paths = [self.remote_paths[i] for i in indices]
        elif isinstance(indices, slice):
            self.remote_paths = self.remote_paths[indices]
        else:
            raise TypeError(f'Expected `indices` to be a single or a list of indices (int, or list[int]), or a slice, but got {type(indices)}.')

    def split(
            self,
            proportion : list[float | int] | None=None,
            indices : list[list[int]] | None=None
        ) -> list["RemotePathIterator"]:
        """Split into multiple iterators that share the same backend.

        Either ``proportion`` or ``indices`` must be provided (exclusively).
        The resulting iterators must not be used in parallel.

        Args:
            proportion: Proportions used to
                allocate items to splits. Will be normalized if needed.
            indices: Explicit index lists for
                each split.

        Returns:
           Independent iterators over disjoint
            subsets of the original paths.
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
            if any([not isinstance(i, (float, int)) for i in proportion]):
                raise TypeError("All proportions must be numeric (int or float).")
            total = float(sum(proportion))
            if total <= 0:
                raise ValueError("Sum of proportions must be > 0.")
            proportion = [float(p) / total for p in proportion]
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

        iterators = []
        for i in indices:
            this = RemotePathIterator(
                self.io_handler,
                batch_size=self.batch_size,
                batch_parallel=self.batch_parallel,
                max_queued_batches=self.max_queued_batches,
                n_local_files=self.n_local_files
            )
            this.subset(i)
            iterators.append(this)

        return iterators

    def _download_batch_with_retry(self, batch : list[str]) -> list[str]:
        start = time.monotonic()
        attempt = 0
        delay = self.retry_base_delay
        last_exc : BaseException | None = None
        while True:
            try:
                local_paths = self.io_handler.download(batch, n=self.batch_parallel)
                if not isinstance(local_paths, (list, tuple)) or len(local_paths) != len(batch):
                    raise RuntimeError(f"Downloader returned invalid result length ({len(local_paths)}) for batch of size {len(batch)}")
                return list(local_paths)
            except Exception as e:
                last_exc = e
                attempt += 1
                elapsed = time.monotonic() - start
                remaining = self.retry_timeout - elapsed
                if remaining <= 0:
                    main_logger.error(f"Batch download timed out after {elapsed:.1f}s and {attempt} attempt(s).")
                    raise TimeoutError(f"Timed out downloading batch of {len(batch)} items") from e
                wait = min(delay, self.retry_max_delay, remaining)
                main_logger.warning(
                    f"Batch download failed (attempt {attempt}); waiting {wait:.2f}s. "
                    f"Time left ≈ {remaining:.1f}s."
                )
                time.sleep(wait)
                delay = min(delay * 2.0, self.retry_max_delay)

    def download_files(self) -> None:
        queued_batches = 0
        n = len(self.remote_paths)
        for start in range(0, n, self.batch_size):
            if self.stop_requested or self._error is not None:
                break
    
            while queued_batches >= self.max_queued_batches and not self.stop_requested and self._error is None:
                if self.last_batch_consumed > 0:
                    self.last_batch_consumed -= 1
                    break
                time.sleep(0.2)
    
            if self.stop_requested or self._error is not None:
                break
    
            batch = self.remote_paths[start:start + self.batch_size]
            if len(batch) == 0:
                raise RuntimeError('Invalid zero-length batch encountered! This is a bug.')
            try:
                local_paths = self._download_batch_with_retry(batch)
            except Exception as e:
                self._error = e
                self.stop_requested = True
                break
    
            for lp, rp in zip(local_paths, batch):
                self.download_queue.put((lp, rp))
    
            queued_batches += 1

    def start_download_queue(self) -> None:
        """Start the background thread that performs batch downloads."""
        self.download_thread = threading.Thread(target=self.download_files, daemon=True)
        self.download_thread.start()

    def __iter__(self):
        """Return an iterator that yields ``(local_path, remote_path)`` pairs."""
        self.not_cleaned = True
        self._cleanup()

        self.stop_requested = False
        self.not_cleaned = True
        self._error = None

        self.start_download_queue()

        try:
            total = len(self.remote_paths)
            for _ in range(total):
                if self.stop_requested:
                    break

                # process delete queue
                while self.clear_local and self.delete_queue.qsize() > self.n_local_files:
                    try:
                        delete_file_or_dir(self.delete_queue.get_nowait())
                    except queue.Empty:
                        break
                    except Exception as e:
                        main_logger.warning(f"Failed to remove file: {e}")
                
                # wait here until we either get an item or detect a dead producer
                while True:
                    if self.download_queue.empty() and not self.download_thread.is_alive():
                        self.stop_requested = True
                        if self._error is not None:
                            raise self._error
                        raise RuntimeError("Download thread died before iteration finished.")
                    try:
                        next_item: tuple[str, str] = self.download_queue.get(timeout=5.0)
                        break  # got an item
                    except queue.Empty:
                        # producer may just be slow; only error if the thread has died
                        if not self.download_thread or not self.download_thread.is_alive():
                            self.stop_requested = True
                            if self._error is not None:
                                raise self._error
                            raise RuntimeError("Download thread died before iteration finished.")
                        # else: keep polling this same loop iteration
                
                self.consumed_files += 1
                if self.consumed_files >= self.batch_size:
                    self.consumed_files -= self.batch_size
                    self.last_batch_consumed += 1
                
                self.delete_queue.put(next_item[0])
                yield next_item
            
        finally:
            self._cleanup()

    def _cleanup(self, force : bool=False) -> None:
        """Stop background work and remove temporary files if requested.

        Args:
            force: If True, cleanup even if already performed.
        """
        if self.not_cleaned or force:
            self.stop_requested = True
            self._error = None

            t = self.download_thread
            self.download_thread = None
            if t is not None:
                t.join(timeout=1)
                if t.is_alive():
                    main_logger.error("Download thread did not terminate within 1s; it may still be running.")
                time.sleep(0.01)

            while self.clear_local and not self.download_queue.empty():
                self.delete_queue.put(self.download_queue.get()[0])
            while not self.delete_queue.empty():
                f = self.delete_queue.get()
                try:
                    delete_file_or_dir(f)
                except Exception as e:
                    main_logger.warning(f"Failed to remove file ({f}): {e}")

            if self.clear_local and os.path.isdir(self.temp_dir):
                for f in os.listdir(self.temp_dir):
                    if "folder_index.txt" in f:
                        continue
                    p = os.path.join(self.temp_dir, f)
                    try:
                        delete_file_or_dir(p)
                    except Exception as e:
                        main_logger.warning(f"Failed to remove file: {p} ({e})")

            if not self.download_queue.empty():
                main_logger.error("Download queue is not empty. This should not happen.")
                with self.download_queue.mutex:
                    self.download_queue.queue.clear()
            if not self.delete_queue.empty() and self.clear_local:
                main_logger.error("Delete queue is not empty. This should not happen.")
                with self.delete_queue.mutex:
                    self.delete_queue.queue.clear()
            self.not_cleaned = False
        else:
            main_logger.debug("Already cleaned up")

