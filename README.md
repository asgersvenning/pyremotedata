# `pyRemoteData`
`pyRemoteData` is a module developed for scientific computation using the remote storage platform ERDA (Electronic Research Data Archive) provided by DeIC (Danish e-Infrastructure Consortium) as part of my PhD at the Department of Ecoscience at Aarhus University.

## Capabilities
In order to facility high-throughput computation in a cross-platform setting, `pyRemoteData` handles data transfer with multithreading and asynchronous data streaming using thread-safe buffers.

## Use-cases
If your storage facility supports SFTP and LFTP, and you need high-bandwidth data streaming for analysis, data migration or other purposes such as model-training, then this module may be of use to you.
Experience with SFTP or LFTP is not necessary, but you must be able to setup the required SSH configurations.

## Setup
A more user-friendly setup process, which facilitates both automated as well as interactive setup is currently in development. (**TODO**: Finish and describe the setup process)

### Interactive
Simply follow the popup instructions that appear once you load the package for the first time.

### Automated
The automatic configuration setup relies on setting the correct environment variables **BEFORE LOADING THE PACKAGE**:

* `PYREMOTEDATA_REMOTE_USERNAME` : Should be set to your username on your remote service.
* `PYREMOTEDATA_REMOTE_URI` : Should be set to the URI of the endpoint for your remote service (e.g. for ERDA it is "io.erda.au.dk").
* `PYREMOTEDATA_REMOTE_DIRECTORY` : If you would like to set a default working directory, that is not the root of your remote storage, then set this to that (e.g. "/MY_PROJECT/DATASETS") otherwise simply set this to "/".
* `PYREMOTEDATA_AUTO` : Should be **set to "yes"** to disable interactive mode. If this is not set, or set to anything other than "yes" (not case-sensitive though), while any of the prior environment variables are unset an error will be thrown.

## Issues
This module is certainly not maximally efficient, and you may run into network- or OS-specific issues. Any and all feedback and contributions is highly appreciated.