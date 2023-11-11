from pyremotedata.implicit_mount import *
import time

def perf_roundtrip():

    mount = ImplicitMount()
    mount.mount()
    
    mount.ls()

    mount.cd("testing")

    start = time.time()
    print(mount.execute_command("debug"))
    end = time.time()

    print("Time taken: {} seconds".format(end-start))

    mount.unmount()

perf_roundtrip()