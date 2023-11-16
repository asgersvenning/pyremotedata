# `pyRemoteData`
`pyRemoteData` is a module developed for scientific computation using the remote storage platform [ERDA](https://erda.au.dk/) (Electronic Research Data Archive) provided by Aarhus University IT, as part of my PhD at the Department of Ecoscience at Aarhus University.

It can be used with **any** passwordless SSH-enabled storage facility that supports SFTP and LFTP. But is only tested on a minimal SFTP server found at [atmoz/sftp](https://hub.docker.com/r/atmoz/sftp) and on the live AU ERDA service which runs on MiG (Minimum intrusion Grid - [SourceForge](https://sourceforge.net/projects/migrid/)/[GitHub](https://github.com/ucphhpc/migrid-sync)) developed by [SCIENCE HPC Centre at Copenhagen University](https://science.ku.dk/english/research/research-e-infrastructure/science-hpc-centre/).

If your facility requires a password, it should be very easy to modify the code to support this, in fact it is already implemented, but not exposed to the user.
Merely change line 76 in src/remote_data/implicit_mount.py to fetch the password from the environment variable of your choice, or simply hardcode it. However, do this at your own risk, as I have not assessed the security implications.

## Capabilities
In order to facility high-throughput computation in a cross-platform setting, `pyRemoteData` handles data transfer with multithreading and asynchronous data streaming using thread-safe buffers.

## Use-cases
If your storage facility supports SFTP and LFTP, and you need high-bandwidth data streaming for analysis, data migration or other purposes such as model-training, then this module may be of use to you.
Experience with SFTP or LFTP is not necessary, but you must be able to setup the required SSH configurations.

## Setup
A more user-friendly setup process, which facilitates both automated as well as interactive setup is currently in development. (**TODO**: Finish and describe the setup process)

### Installation
The package is available on PyPI, and can be installed using pip:
```bash
pip install pyremotedata
```

### Interactive
Simply follow the popup instructions that appear once you load the package for the first time.

### Automated
The automatic configuration setup relies on setting the correct environment variables **BEFORE LOADING THE PACKAGE**:

* `PYREMOTEDATA_REMOTE_USERNAME` : Should be set to your username on your remote service.
* `PYREMOTEDATA_REMOTE_URI` : Should be set to the URI of the endpoint for your remote service (e.g. for ERDA it is "io.erda.au.dk").
* `PYREMOTEDATA_REMOTE_DIRECTORY` : If you would like to set a default working directory, that is not the root of your remote storage, then set this to that (e.g. "/MY_PROJECT/DATASETS") otherwise simply set this to "/".
* `PYREMOTEDATA_AUTO` : Should be **set to "yes"** to disable interactive mode. If this is not set, or set to anything other than "yes" (not case-sensitive), while any of the prior environment variables are unset an error will be thrown.

### Example
If you want to test against a mock server simply follow the instructions in tests/README.

If you have a remote storage facility that supports SFTP and LFTP, then you can use the following example to test the functionality of the module:
```python
# Set the environment variables (only necessary in a non-interactive setting)
# If you are simply running this as a Python script, 
# you can omit these lines and you will be prompted to set them interactively
import os
os.environ["PYREMOTEDATA_REMOTE_USERNAME"] = "username"
os.environ["PYREMOTEDATA_REMOTE_URI"] = "storage.example.com"
os.environ["PYREMOTEDATA_REMOTE_DIRECTORY"] = "/MY_PROJECT/DATASETS"
os.environ["PYREMOTEDATA_AUTO"] = "yes"

from pyremotedata.implicit_mount import IOHandler

handler = IOHandler()

with handler as io:
    print(io.ls())

# The configuration is persistent, but can be removed using the following:
from pyremotedata.config import remove_config
remove_config()
```

## Issues
This module is certainly not maximally efficient, and you may run into network- or OS-specific issues. Any and all feedback and contributions is highly appreciated.