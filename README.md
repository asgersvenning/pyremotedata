# `pyRemoteData`
`pyRemoteData` is a module developed for scientific computation using the remote storage platform ERDA (Electronic Research Data Archive) provided by DeIC (Danish e-Infrastructure Consortium) as part of my PhD at the Department of Ecoscience at Aarhus University.

## Capabilities
In order to facility high-throughput computation in a cross-platform setting, `pyRemoteData` handles data transfer with multithreading and asynchronous data streaming using thread-safe buffers.

## Use-cases
If your storage facility supports SFTP and LFTP, and you need high-bandwidth data streaming for analysis, data migration or other purposes such as model-training, then this module may be of use to you.
Experience with SFTP or LFTP is not necessary, but you must be able to setup the required SSH configurations.

## Setup
In order to use `pyRemoteData` you must have a `pyremotedata_config.yaml` in the directory of the script you are executing (TODO: Create a more flexible setup for configurations)

## Issues
This module is certainly not maximally efficient, and you may run into network- or OS-specific issues. Any and all feedback and contributions is highly appreciated.