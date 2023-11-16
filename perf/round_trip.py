from pyremotedata.implicit_mount import *
from pyremotedata.config import remove_config
import time

def perf_roundtrip():

    mount = ImplicitMount()
    mount.mount()
    
    mount.ls()

    mount.execute_command("cls")

    start = time.time()
    print(mount.execute_command("recls"))
    end = time.time()

    print("Time taken: {} seconds".format(end-start))

    mount.unmount()


perf_roundtrip()
remove_config()