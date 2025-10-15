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
from enum import Enum
from queue import Queue
from random import choices, shuffle

from tqdm.auto import trange

from pyremotedata import CLEAR_LINE, ESC_EOL, main_logger
from pyremotedata.config import DEFAULT_LFTP_CONFIG, get_implicit_mount_config

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

IMMUTABLE_DIRECTORIES = ("", ".", "..", "./", "/")

class RemoteType(int, Enum):
    MISSING = 0
    FILE = 1
    DIRECTORY = 2
    @classmethod
    def _missing_(cls, value):
        if value is None:
            return cls.NONE
        if isinstance(value, str):
            name = value.strip().upper()
            try:
                return cls[name]
            except KeyError:
                raise ValueError(f'Only remote modes "MISSING", "FILE" and "DIRECTORY" are defined, not {value!r}.')
        # let IntEnum's default handling raise for bad ints:
        return super()._missing_(value)

class ImplicitMount:
    """
    This is a low-level wrapper of LFTP, which provides a pythonic interface for executing LFTP commands and reading the output.
    It provides a robust and efficient backend for communicating with a remote storage server using the SFTP protocol, using a persistent LFTP shell handled in the background by a subprocess.
    It is designed to be used as a base class for higher-level wrappers, such as the :py:class:`pyremotedata.implicit_mount.IOHandler` class, or as a standalone class for users familiar with LFTP.

    OBS: The attributes of this method should not be used unless for development or advanced use cases, all responsibility in this case is on the user.

    Args:
        user: The username to use for connecting to the remote directory.
        password: The *SFTP* password to possibly use when connecting to the remote host.
        remote: The remote server to connect to. If `user` and `password` are supplied, this will default to 'io.erda.au.dk' for convenience.
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
            verbose : bool=main_logger.isEnabledFor(logging.DEBUG),
            **kwargs
        ):
        # Default argument configuration and type checking
        if not (user is None or password is None or port is None):
            if remote is None:
                # Since 99.9% of use-cases are for ERDA at the moment, the decrease in friction this 
                # default adds is worth the non-generality (may be changed in the future)
                main_logger.info("Defaulting to 'io.erda.au.dk' because user and password have been manually passed!")
            self.default_config = {
                "default_remote_dir" : "/",
                "local_dir" : None,
                "remote" : "io.erda.au.dk",
                "lftp": DEFAULT_LFTP_CONFIG.copy()
            }
        else:
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
        
        local_dir = kwargs.get("local_dir", self.default_config.get("local_dir", None))
        if not isinstance(local_dir, str) or local_dir == "":
            local_dir = tempfile.TemporaryDirectory().name
            self.default_config["local_dir"] = local_dir
        assert isinstance(local_dir, str)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self._local_dir = local_dir
        self.original_local_dir = os.path.abspath(self._local_dir)
        
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
        if " " in command:
            cmd, other = command.split(" ", 1)
        else:
            cmd, other = command, ""
        full_command = f"{cmd} {formatted_args} {other}" if formatted_args else command
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
        # Merge default settings and user settings. User settings will override default settings.
        lftp_settings = {**self.default_config['lftp'], **lftp_settings} if lftp_settings is not None else self.default_config['lftp']
        
        # Password connect:
        if isinstance(self.password, str) and len(self.password) > 0 and self.password != "":
            if "sftp:connect-program" in lftp_settings:
                raise KeyError(f"LFTP setting 'sftp:connect-program' cannot be used with password connection!")
            lftp_settings["sftp:connect-program"] = "ssh -a -x -o PreferredAuthentications=password -o PubkeyAuthentication=no"

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
        self.execute_command(f"lcd {self._local_dir}")

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

    def exists(
        self,
        path : str,
        mode : str="any",
        execute : bool=True,
        **kwargs
        ):
        """
        Check if a path (file/directory/link) exists.

        Args:
            path: Path to check.
            mode: Which types of file to match. Not-case sensitive, 
                currently "any" (default), "file" and "directory" are supported.
            **kwargs: Additional arguments passed to execute_command.
        
        Returns:
            Boolean if `execute=True` else the command.
        """
        if not isinstance(mode, str):
            raise TypeError(f'Unexpected exist type: {type(mode)}')
        mode = mode.strip().lower()
        # Some paths are "base-cases" that should exist in all cases:
        # - Current directory
        # - Parent directory of current directory
        # - Root directory
        if path in [".", "..", "/", "/.", "./"]:
            if not execute:
                return "NO CMD"
            return mode in ["any", "directory"]
        parts = path.split("/")
        # Recursively check if the parent directories exist to avoid error in glob expansion
        if len(parts) > 1:
            if not self.exists("/".join(parts[:-1]), mode="directory"):
                return False
        else:
            parts.insert(0, ".")
        # Get glob mode-argument
        match mode:
            case "any":
                type_arg = "-a"
            case "file":
                type_arg = "-f"
            case "directory":
                type_arg = "-d"
        # Check if file is hidden, needs special handling: https://en.wikipedia.org/wiki/Glob_(programming)#Origin
        hidden = parts[-1].startswith(".")
        exist_glob = f'{"/".join(parts[:-1])}/{"." if hidden else ""}*{parts[-1].removeprefix(".")}'
        glob_result = self.execute_command(f'glob {type_arg} --exist "{exist_glob}" && echo "YES" || echo "NO"')
        if not execute:
            return glob_result
        if isinstance(glob_result, list) and len(glob_result) == 1:
            glob_result = glob_result[0]
        else:
            raise RuntimeError(f'Unexpected return value: {glob_result}')
        return glob_result == "YES"
    
    def pathtype(
        self,
        remote_path : str
        ):
        if not self.exists(remote_path, mode="any"):
            return RemoteType.MISSING
        if self.exists(remote_path, mode="directory"):
            return RemoteType.DIRECTORY
        if self.exists(remote_path, mode="file"):
            return RemoteType.FILE
        raise RuntimeError(f'Unknown remote path type "{remote_path}" exists, but is not a file or directory?')

    def get(
            self,
            remote_path : str | list[str] | tuple[str, ...],
            local_path : str | list[str] | tuple[str, ...] | None=None,
            execute : bool=True,
            **kwargs
        ):
        """Download file(s) using LFTP ``get``.

        Args:
            remote_path: Remote file path to download.
            local_path: Local download destination path. If
                None, downloads to the current local directory.
            blocking: If True, wait for completion.
            execute: If False, return the command string.
            output: If True, return absolute remote path(s).
            **kwargs: Additional options forwarded to ``put``.

        Returns:
            Command string(s) when ``execute`` is False; otherwise, local destination path(s) based on the type of `remote_path`.
        """
        rettype = type(remote_path)
        if not isinstance(remote_path, (str, list, tuple)):
            raise TypeError(f'Invalid download path type: {type(remote_path)}')
        if isinstance(remote_path, str):
            remote_path = [remote_path]
        if local_path is None:
            local_path = [os.path.basename(rp) for rp in remote_path]
        elif isinstance(local_path, str):
            local_path = [local_path]
        if len(remote_path) != len(local_path):
            raise ValueError(f'Number of local paths (destination) must match number of remote paths (source), but: {len(remote_path)=} & {len(local_path)=}')
        if len(remote_path) == 0:
            return rettype()
        if len(remote_path) > 1:
            retorder = {rp : None for rp in remote_path}
            cmds = []
            local_destination_dirs = ["/".join(lp.split("/")[:-1]) or "." for lp in local_path]
            for local_destination_dir in set(local_destination_dirs):
                dir_remote_paths = [rp for rp, ldd in zip(remote_path, local_destination_dirs) if ldd == local_destination_dir]
                dir_retval = self.mget(dir_remote_paths, local_destination_dir=local_destination_dir, execute=execute, **kwargs)
                if not execute:
                    cmds.append(dir_retval)
                    continue
                for rv, rp in zip(dir_retval, dir_remote_paths):
                    retorder[rp] = rv
            if not execute:
                return cmds
            retval = [retorder[rp] for rp in remote_path]
            return rettype(retval)
        kwargs.pop("P", None)
        remote_path, local_path = remote_path[0], local_path[0]
        local_dir = os.path.normpath(os.path.dirname(local_path))
        if local_dir not in IMMUTABLE_DIRECTORIES and (local_dir != local_path) and not os.path.exists(local_dir):
            os.makedirs(local_dir)
        exec_output = self.execute_command(
            # f"get " + " ".join(f'{rp} -o {lp}' for rp, lp in zip(remote_path, local_path)),
            f'get "{remote_path}" -o "{local_path}"',
            execute=execute,
            **kwargs
        )
        if not execute:
            return exec_output
        retval = local_path if local_path.startswith("/") else f'{self.lpwd()}/{local_path}'
        if rettype != str:
            return rettype([retval])
        return retval
    
    def mget(
        self,
        remote_paths : list[str] | tuple[str, ...],
        local_destination_dir : str,
        execute : bool=True,
        default_args : dict | None=None,
        **kwargs
        ):
        if (
            local_destination_dir not in IMMUTABLE_DIRECTORIES and 
            (local_destination_dir != os.path.dirname(local_destination_dir)) and 
            not os.path.exists(local_destination_dir)
        ):
            os.makedirs(local_destination_dir)
        elif not os.path.isdir(local_destination_dir):
            raise ValueError(f'`local_destination_dir`: {local_destination_dir} is not a directory.')
        default_args = default_args or {}
        default_args = {"P" : 5, **default_args}
        exec_output = self.execute_command(
            "mget " + " ".join([f'"{rp}"' for rp in remote_paths]),
            execute=execute,
            default_args=default_args,
            O=local_destination_dir,
            **kwargs
        )
        if not execute:
            return exec_output
        ldst = local_destination_dir if local_destination_dir.startswith("/") else f'{self.lpwd()}/{local_destination_dir}'
        return [f'{ldst}/{os.path.basename(rp)}' for rp in remote_paths]
        

    def pget(
            self, 
            remote_path : str, 
            local_path : str | None, 
            blocking : bool=True,
            execute : bool=True,
            output : bool | None=None, 
            default_args : dict | None=None,
            **kwargs
        ) -> str | None:
        """Download a single file using LFTP ``pget``.

        Args:
            remote_path: Remote file path to download.
            local_path: Local path destination, defaults to remote basename in current local directory.
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
        file_name = remote_path.split("/")[-1]
        if local_path is None:
            local_path = os.path.basename(file_name)
        if os.path.exists(local_path):
            while os.path.exists(subdir := os.path.join(os.path.dirname(local_path), str(uuid.uuid4()))):
                pass
            os.makedirs(subdir)
            return self.pget(remote_path=remote_path, local_path=os.path.join(subdir, file_name), blocking=blocking, execute=execute, output=output, default_args=default_args, **kwargs)

        local_dir = os.path.dirname(local_path)
        if local_dir not in IMMUTABLE_DIRECTORIES and not os.path.exists(local_dir):
            os.makedirs(local_dir)

        default_args = default_args or {}
        default_args = {"n" : 5, **default_args}

        full_command = f'pget "{remote_path}" -o "{local_path}"'
        exec_output = self.execute_command(
            full_command, 
            output=output, 
            blocking=blocking,
            execute=execute,
            default_args=default_args,
            **kwargs
        )
        if not execute:
            return exec_output
        
        return local_path if local_path.startswith("/") else f'{self.lpwd()}/{local_path}'
    
    def put(
            self,
            local_path : str | list[str] | tuple[str, ...],
            remote_path : str | list[str] | tuple[str, ...] | None=None,
            execute : bool=True,
            **kwargs
        ):
        """Upload file(s) using LFTP ``put``.

        Args:
            local_path: Local file(s) to upload.
            remote_path: Remote file path(s) to upload to. If
                None, uploads to the current remote directory.
            blocking: If True, wait for completion.
            execute: If False, return the command string.
            output: If True, return absolute remote path(s).
            **kwargs: Additional options forwarded to ``put``.

        Returns:
            Command string(s) when ``execute`` is False; otherwise, remote destination path(s) based on the type of `remote_path`.
        """
        rettype = type(local_path)
        if not isinstance(local_path, (str, list, tuple)):
            raise TypeError(f'Invalid upload path type: {type(local_path)}')
        if isinstance(local_path, str):
            local_path = [local_path]
        if remote_path is None:
            remote_path = [os.path.basename(lp) for lp in local_path]
        elif isinstance(remote_path, str):
            remote_path = [remote_path]
        if len(remote_path) != len(local_path):
            raise ValueError(f'Number of local paths (destination) must match number of remote paths (source), but: {len(remote_path)=} & {len(local_path)=}')
        if len(local_path) == 0:
            return rettype()
        if len(local_path) > 1:
            retorder = {lp : None for lp in local_path}
            cmds = []
            remote_destination_dirs = ["/".join(rp.split("/")[:-1]) or "." for rp in remote_path]
            for remote_destination_dir in set(remote_destination_dirs):
                dir_local_paths = [lp for lp, rdd in zip(local_path, remote_destination_dirs) if rdd == remote_destination_dir]
                dir_retval = self.mput(dir_local_paths, remote_destination_dir=remote_destination_dir, execute=execute, **kwargs)
                if not execute:
                    cmds.append(dir_retval)
                    continue
                for rv, lp in zip(dir_retval, dir_local_paths):
                    retorder[lp] = rv
            if not execute:
                return cmds
            retval = [retorder[lp] for lp in local_path]
            return rettype(retval)
        remote_path, local_path = remote_path[0], local_path[0]
        remote_dir = "/".join(remote_path.split("/")[:-1])
        if remote_dir not in IMMUTABLE_DIRECTORIES and remote_dir != remote_path:
            self.execute_command(f'mkdir -p "{remote_dir}"')
        kwargs.pop("P", None)
        exec_output = self.execute_command(
            f'put "{local_path}" -o "{remote_path}"',
            execute=execute,
            **kwargs
        )
        if not execute:
            return exec_output
        retval = remote_path if remote_path.startswith("/") else f'{self.pwd()}/{remote_path}'
        if rettype != str:
            return rettype([retval])
        return retval
    
    def mput(
        self,
        local_paths : list[str] | tuple[str, ...],
        remote_destination_dir : str,
        execute : bool=True,
        default_args : dict | None=None,
        **kwargs
        ):
        if remote_destination_dir not in IMMUTABLE_DIRECTORIES:
            self.execute_command(f'mkdir -p "{remote_destination_dir}"')
        default_args = default_args or {}
        default_args = {"P" : 5, **default_args}
        exec_output = self.execute_command(
            "mput " + " ".join(local_paths),
            execute=execute,
            default_args=default_args,
            O=remote_destination_dir,
            **kwargs
        )
        if not execute:
            return exec_output
        ldst = remote_destination_dir if remote_destination_dir.startswith("/") else f'{self.pwd()}/{remote_destination_dir}'
        return [f'{ldst}/{os.path.basename(lp)}' for lp in local_paths]
    
    def rm(
        self,
        path : str | list[str] | tuple[str, ...],
        force : bool=False,
        execute : bool=True,
        blocking : bool=True,
        **kwargs
        ) -> str | None:
        """
        Remove a file or directory.

        Args:
            path: File or directory to delete.
            force: Force deletion of path (rm -rf), directories can only be deleted with this argument (empty or not).
            **kwargs: Additional arguments passed to execute_command.
        
        Returns:
            The command `execute=False` else None
        """
        if isinstance(path, str):
            path = [path]
        args = {"r" : None, "f" : None}
        if not force:
            args.pop("r", None)
            args.pop("f", None)
        kwargs = {**kwargs, **args}
        retval = self.execute_command('rm ' + ' & rm '.join(path), blocking=blocking, execute=execute, output=False, **kwargs)
        if not execute:
            return retval
        
    def du(
        self,
        path : str,
        all : bool=False,
        bytes : bool=True,
        execute : bool=True,
        blocking : bool=True,
        default_args : dict | None=None,
        **kwargs
        ):
        """
        Get the size of a directory/path.

        Args:
            path: Path to examine.
            all: Return size of all files and subdirectories of path, including path itself, separately.
            bytes: Return size in bytes, otherwise in KB.
            **kwargs: Passed to execute_command.
        
        Returns:
            A dict with path(s) as keys and size as values, if `blocking=False` None and if `execute=False` the command.
        """
        default_args = default_args or {}
        default_args = {"all" : None, "bytes" : None, **default_args}
        if not all:
            default_args.pop("all", None)
        if not bytes:
            default_args.pop("bytes", None)
        retval = self.execute_command(f'du "{path}" | cat', blocking=blocking, execute=execute, default_args=default_args, **kwargs)
        if not blocking:
            return None
        if not execute:
            return retval
        if not isinstance(retval, list):
            raise RuntimeError(f'Unexpected return value: {retval}')
        return dict((size_path[1], int(size_path[0])) for line in retval if (size_path := line.split("\t")))

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
            output = sanitize_path([], self.execute_command(f'cls -1 "{path}"'))
        
        # Clear the progress bar if end of top-level
        if pbar and _top:
            main_logger.info(f"{CLEAR_LINE}{ESC_EOL}\n")

        # Check if the output is a list
        if not isinstance(output, list):
            TypeError("Expected list, got {}".format(type(output)))
        
        return output
    
    def lls(self, local_path: str = ".", **kwargs) -> list[str] | str | None:
        """List files in a local directory via the LFTP shell.

        Args:
            local_path: 

        Notes:
            Prefer native Python/OS listing. This is mainly useful for
            consistency and debugging through the same LFTP session.
        """
        if os.name != "posix":
            raise NotImplementedError("IOHandler.lls() is not supported in non-Unix-like OSs (Windows)")
        recursive = kwargs.pop("R", kwargs.pop("recursive", False))
        if not (recursive is False):
            if local_path == "":
                local_path = "."
            output = self.execute_command(f'!find "{local_path}" -type f -exec realpath --relative-to="{local_path}" {{}} \\; | cat', **kwargs)
        else: 
            output = self.execute_command(f'!ls "{local_path}" | cat', **kwargs)
        
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
        self._local_dir = os.path.abspath(os.path.join(self._local_dir, local_path)) if not local_path.startswith("/") else local_path
        self.execute_command(f"lcd {self._local_dir}", output=False)

    def lpwd(self):
        """
        Get the current local directory.

        Returns:
           The current local directory.
        """
        return self._local_dir

    def mirror(
            self, 
            remote : str,
            local : str,
            reverse : bool=False,
            output : bool=True,
            blocking : bool=True,
            execute : bool=True,
            default_args : dict | None=None,
            **kwargs
        ) -> list[str] | None:
        """Mirror a remote directory to a local destination (LFTP ``mirror``).

        Args:
            remote: Remote directory to download (if not reverse).
            local: Local destination directory (if not reverse).
            reverse: Upload from `local` to `destination`.
            **kwargs: Additional options forwarded to ``mirror``.

        Returns:
           List of newly downloaded files or following `ImplicitMount.execute_command`.
        """
        if "R" in kwargs:
            raise RuntimeError(f'Passing `R` to IOHandler.mirror is not supported, please use `reverse` instead.')
        if reverse:
            kwargs["R"] = None
            if remote is None:
                remote = self.pwd()
            if local is None:
                raise ValueError('When reverse mirroring (uploading) local source must be specified.')
        else:
            if local is None:
                local = self.lpwd()
            if reverse is None:
                raise ValueError('When mirroring (downloading) remote source must be specified.')
        if output:
            # Capture the state of the directory before the operation
            if reverse:
                pre_existing_files = [] if not self.exists(remote) else self.ls(remote, recursive=True)
            else:
                pre_existing_files = [] if not os.path.exists(local) else self.lls(local, recursive=True)
            # Ensure that the pre_existing_files list is unique
            pre_existing_files = set(pre_existing_files)

        # Execute the mirror command
        default_args = default_args or {}
        default_args = {'P': 5, 'use-cache': None, **default_args}
        exec_output = self.execute_command(
            f'mirror "{local}" "{remote}"' if reverse else f'mirror "{remote}" "{local}"', 
            output=False, 
            blocking=blocking, 
            execute=execute,
            default_args=default_args, 
            **kwargs
        )
        if not execute:
            return exec_output
        
        if output:
            # Capture the state of the directory after the operation
            if reverse:
                post_download_files = [] if not self.exists(remote) else self.ls(remote, recursive=True)
            else:
                post_download_files = [] if not os.path.exists(local) else self.lls(local, recursive=True)
            # Ensure that the post_download_files list is unique
            post_download_files = set(post_download_files)
            # Calculate the set difference to get the newly downloaded files
            new_files = post_download_files - pre_existing_files
            return list(new_files)

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
        self.user_confirmation = user_confirmation
        self.do_clean = clean
        self.lftp_settings = lftp_settings
        self.cache : dict[str, list[str]] = {}

    def __enter__(self) -> "IOHandler":
        """
        Opens the remote connection in a background shell.
        """
        self.mount(self.lftp_settings or {})

        # Print local directory:
        # if the local directory is not specified in the config, 
        # it is a temporary directory, so it is nice to know where it is located
        main_logger.debug(f"Local directory: {self._local_dir}")

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
            n : int=14,
            blocking : bool=True,
            **kwargs
        ):
        """
        Downloads one or more files or a directory from the remote directory to the given local destination.

        Args:
            remote_path: The remote path(s) to download.
            local_destination: The local destination directory(s) to download the file(s) to. If None, the file(s) will be downloaded to the current local directory.
            n: Parallel connections to use if relevant (default=14).
            blocking: If True, the function will block until the download is complete.
            **kwargs: Extra keyword arguments are passed to the IOHandler.multi_download, IOHandler.pget or IOHandler.mirror functions depending on the type of the remote path(s).

        Returns:
           The local path(s) of the downloaded file(s) or directory.
        """
        if isinstance(remote_path, str):
            match self.pathtype(remote_path):
                case RemoteType.MISSING:
                    raise RuntimeError("Missing download target: " + remote_path)
                case RemoteType.DIRECTORY:
                    if not (isinstance(local_destination, str) or local_destination is None):
                        raise ValueError("Downloading directories should have a single destination!")
                    return self.mirror(remote_path, local_destination, blocking=blocking, P=n, **kwargs)
                case RemoteType.FILE:
                    if not (local_destination is None or isinstance(local_destination, str)):
                        raise ValueError("Downloading single files should have a single destination!")
                    if local_destination is not None:
                        local_destination = f'{local_destination}/{remote_path.split("/")[-1]}'
                    return self.pget(remote_path, local_destination, blocking=blocking, n=n, **kwargs)
                case _:
                    raise RuntimeError('Remote download path exists, but is not a file or directory?')
        n = min(len(remote_path), n)
        if local_destination is not None:
            if not isinstance(local_destination, (list, tuple)):
                if not isinstance(local_destination, str):
                    raise ValueError(f"Downloading multiple files should have a single destination dir or list of these, not {local_destination}")
                local_destination = [f'{local_destination}/{rp.split("/")[-1]}' for rp in remote_path]
            else:
                if len(local_destination) != len(remote_path):
                    raise ValueError(f'Destination and remote paths (source) have different lengths: {len(local_destination) != {len(remote_path)}}')
                local_destination = [f'{lp}/{rp.split("/")[-1]}' for lp, rp in zip(local_destination, remote_path)]
        return self.get(remote_path, local_destination, blocking=blocking, P=n, **kwargs)

    def upload(
            self, 
            local_path : str | list[str] | tuple[str, ...],
            remote_destination : str | list[str] | tuple[str, ...] | None=None,
            n : int=14,
            blocking : bool=True,
            **kwargs
        ):
        """
        Downloads one or more files or a directory from the remote directory to the given local destination.

        Args:
            local_path: The local file(s) or directory to upload.
            remote_destination: Remote destination directory(s) of uploaded files. If None will upload to current remote directory.
            n: Parallel connections to use if relevant (default=14).
            blocking: If True, the function will block until the download is complete.
            **kwargs: Extra keyword arguments are passed to the IOHandler.multi_download, IOHandler.pget or IOHandler.mirror functions depending on the type of the remote path(s).

        Returns:
           The local path(s) of the downloaded file(s) or directory.
        """
        if isinstance(local_path, str):
            if not os.path.exists(local_path):
                raise RuntimeError("Missing upload target: " + local_path)
            if os.path.isdir(local_path):
                if not (isinstance(remote_destination, str) or remote_destination is None):
                    raise ValueError("Uploading directories should have a single destination!")
                return self.mirror(remote_destination, local_path, reverse=True, blocking=blocking, P=n, **kwargs)
            if os.path.isfile(local_path):
                if not (remote_destination is None or isinstance(remote_destination, str)):
                    raise ValueError("Uploading single files should have a single destination!")
                if remote_destination is not None:
                    remote_destination = f'{remote_destination}/{local_path.split("/")[-1]}'
                return self.put(local_path, remote_destination, blocking=blocking, **kwargs)
            raise RuntimeError('Local upload path exists, but is not a file or directory?')
        n = min(len(local_path), n)
        if remote_destination is not None:
            if not isinstance(remote_destination, (list, tuple)):
                if not isinstance(remote_destination, str):
                    raise ValueError(f"Uploading multiple files should have a single destination dir, or list of these, not {remote_destination}")
                remote_destination = [f'{remote_destination}/{lp.split("/")[-1]}' for lp in local_path]
            else:
                if len(remote_destination) != len(local_path):
                    raise ValueError(f'Destination and local paths (source) have different lengths: {len(remote_destination) != {len(local_path)}}')
                remote_destination = [f'{rp}/{lp.split("/")[-1]}' for rp, lp in zip(remote_destination, local_path)]
        return self.put(local_path, remote_destination, blocking=blocking, P=n, **kwargs)

    def sync(
            self, 
            local_destination : str | None=None,
            direction : str="down",
            allow_root : bool=False,
            progress : bool=False,
            batch_size : int=128,
            replace_local : bool=False,
            refresh_cache : bool=False,
            **kwargs
        ):
        """
        Synchronized the current remote directory to the given local destination.

        Args:
            local_destination: The local destination to synchronize the current remote directory to, 
                defaults to "<CURRENT_LOCAL_DIRECTORY_PATH>/<CURRENT_REMOTE_DIRECTORY_NAME>".
            direction: Synchronization directory; one of non-case-sensitive ["down", "up", "both"] (default="down"). 
                "down": Download contents of current remote directory to local destination.
                "up": Upload contents of local destination to current remote directory.
                "both": First synchronize "down", then synchronize "up".
            progress: Show a progress bar.
            batch_size: Number of files passed to each download call.
            replace_local: By default existing files are skipped, if this is enabled, existing files are deleted and refetched.
            refresh_cache: Recompute file index of remote directory, can be extremely slow. Disabled by default.
            **kwargs: Passed to IOHandler.download.
        
        Returns:
           A list of paths to the local paths that have been synchronized, not including existing files if ``replace_local=False``.
        """
        if not isinstance(local_destination, str) and local_destination is not None:
            raise TypeError("Expected str or None, got {}".format(type(local_destination)))
        if local_destination is None:
            cur_remote_dir = self.pwd().split("/")[-1]
            if cur_remote_dir == "":
                if not allow_root:
                    raise RuntimeError(f'Attempted to synchronize root of remote, but `{allow_root=}`!')
                local_destination = self.lpwd()
            else:
                local_destination = os.path.join(self.lpwd(), cur_remote_dir)
        local_destination = os.path.normpath(os.path.abspath(os.path.expandvars(os.path.expanduser(local_destination))))
        if os.path.dirname(local_destination) == local_destination and not allow_root:
            raise RuntimeError(f'Attempted to synchronize root of local, but {allow_root=}!')
        if not os.path.exists(local_destination):
            try:
                os.makedirs(local_destination)
            except FileExistsError:
                pass
        if self.pwd() not in self.cache:
            self.cache_file_index(override=refresh_cache)
        down_files = []
        if direction in ["down", "both"]:
            down_files = self.cache[self.pwd()]
            ldest = [os.path.join(local_destination, *f.split("/")) for f in down_files]
            ldest, down_files = zip(*sorted([(l, f) for l, f in zip(ldest, down_files) if replace_local or not os.path.exists(l)]))
            down_files : list[str]
            ldir = [os.path.dirname(dst) for dst in ldest]
            for bi in trange(-(-len(down_files)//batch_size), desc=f"Synchronizing down: ({self.pwd()}) ==> ({local_destination}) ...", unit="file", unit_scale=batch_size, disable=not progress, dynamic_ncols=True):
                bs, be = bi*batch_size, min((bi+1)*batch_size, len(down_files))
                if replace_local:
                    for ld in ldest[bs:be]:
                        if os.path.exists(ld):
                            os.remove(ld)
                lres = self.download(down_files[bs:be], ldir[bs:be], **kwargs)
                if isinstance(lres, str):
                    lres = [lres]
                for r, e, f in zip(lres, ldest[bs:be], down_files[bs:be]):
                    if r != e:
                        raise RuntimeError(f'Unexpected download location for {f}, expected {e}, but got {r}?!')
        up_files = []
        if direction in ["up", "both"]:
            up_files = self.lls(local_destination)
            if not isinstance(up_files, list):
                raise RuntimeError(f'Unexpected return value: {up_files}')
            skippers = set([os.path.join(f.split("/")) for f in down_files])
            up_files = [f for f in up_files if f not in skippers]
            rdest = [f'{self.pwd()}/{f}' for f in up_files]
            rdir = ["/".join(f.split("/")[:-1]) for f in rdest]
            for bi in trange(-(-len(down_files)//batch_size), desc=f"Synchronizing up: ({self.pwd()}) ==> ({local_destination}) ...", unit="file", unit_scale=batch_size, disable=not progress, dynamic_ncols=True):
                bs, be = bi*batch_size, min((bi+1)*batch_size, len(down_files))
                rres = self.upload(up_files[bs:be], rdir[bs:be], **kwargs)
                if isinstance(rres, str):
                    rres = [rres]
                for r, e, f in zip(rres, rdest[bs:be], up_files[bs:be]):
                    if r != e:
                        raise RuntimeError(f'Unexpected upload location for {f}, expected {e}, but got {r}?!')
        return list(set(down_files).union(set(up_files)))
    
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
        file_index_exists = self.exists(".folder_index.txt")
        if not file_index_exists:
            # Backwards compatibility check for folder index with old naming convention (non-hidden)
            bc_file_index_exists = self.exists("folder_index.txt")
            if bc_file_index_exists:
                bc_file_index = self.execute_command('glob -f du -sh *folder_index.txt | sort -hr | cut -f 2 | head -n 1')
                if not (isinstance(bc_file_index, list) and len(bc_file_index) == 1 and isinstance(bc_file_index := bc_file_index[0], str)):
                    main_logger.warning('Deprecated folder index detected, but failed to rename!')
                else:
                    self.execute_command(f'mv "{bc_file_index}" .folder_index.txt')
                    file_index_exists = True
                    main_logger.debug(f'Detected deprecated folder index {bc_file_index} and renamed to .folder_index.txt in {self.pwd()}')
            elif self.verbose:
                main_logger.debug(f"Folder index does not exist in {self.pwd()}")
        # If override is True, delete the file index if it exists
        if override and file_index_exists:
            self.execute_command("rm .folder_index.txt")
            # Now the file index does not exist (duh)
            file_index_exists = False
        # If the file index does not exist, create it
        if not file_index_exists:
            main_logger.debug("Creating folder index...")
            # Traverse the remote directory and write the file index to a file
            files = self.ls(recursive=True, use_cache=False, pbar=True)
            local_index_path = os.path.join(self.lpwd(), ".folder_index.txt")
            with open(local_index_path, "w") as f:
                for file in files:
                    f.write(file + "\n")
            # Store the file index on the remote if 'store' is True, otherwise delete it
            if store:
                # Self has an implicit reference to the local working directory, however the scripts does not necessarily have the same working directory
                self.upload(local_index_path)
                os.remove(local_index_path)
        
        # Download the file index if 'store' is True or it already exists on the remote, otherwise read it from the local directory
        if store or file_index_exists:
            file_index_path = self.download(".folder_index.txt")
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
                if len(line) < 3 or "folder_index.txt" == line[:17]:
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
            confirmation = input(f"Are you sure you want to delete all files in the current directory {self.lpwd()}? (y/n)")
            if confirmation.lower() != "y":
                main_logger.debug("Aborted")
                return

        main_logger.debug("Cleaning up...")
        for path in os.listdir(self.lpwd()):
            try:
                delete_file_or_dir(path)
            except Exception as e:
                main_logger.debug(f"Error while deleting {path}: {e}")
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
        self.temp_dir = self.io_handler.lpwd()
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

