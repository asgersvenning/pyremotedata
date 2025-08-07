from pyremotedata.implicit_mount import IOHandler
from tqdm import tqdm as TQDM

with IOHandler() as io:
    for _ in TQDM(range(1000), desc="Creating errors...", unit="err"):
        try:
            print(io.cd("timelapse"))
        except:
            pass
        io.ls()
    print(io.ls())