import time

from pyremotedata.config import remove_config
from pyremotedata.implicit_mount import *  # noqa: F403


def perf_roundtrip():
    mount = ImplicitMount()  # noqa: F405
    
    # Connect and cache result on server
    mount.mount()
    mount.ls()
    mount.execute_command("cls")

    start = time.time()
    print(mount.execute_command("recls"))
    end = time.time()

    print(f"Time taken: {end-start} seconds")
    mount.unmount()

if __name__ == "__main__":
    perf_roundtrip()
    remove_config()