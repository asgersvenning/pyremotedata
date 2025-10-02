Usage Guide
==========

This guide covers the main features and use cases of PyRemoteData.

.. contents:: On this page
   :local:
   :depth: 2

Basic File Operations
--------------------

Listing Files
~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
       files = io.ls()
       print(f"Files in current directory: {files}")
       
       # List files in specific directory
       files = io.ls("/path/to/directory")
       print(f"Files in /path/to/directory: {files}")

Changing working directory
~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
       io.cd("/remote/directory")
       print(f"Working directory: {io.pwd()}")

Downloading Files
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
       # Download single file
       local_path = io.download("/remote/file.txt", "/local/file.txt")
       
       # Download directory
       local_path = io.download("/remote/directory", "/local/directory")

Synchronizing directories
~~~~~~~~~~~~~~~~~~~~~~~~~


.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
        # Navigate to the directory
        io.cd("my_directory")

        # Synchronize directory to local storage
        io.sync("<local_parent_directory>", progress=True) 

Uploading Files
~~~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
       # Upload single file
       io.put("/local/file.txt", "/remote/file.txt")
       
       # Upload directory (use mirror for directories)
       io.mirror("/local/directory", "/remote/directory")

Advanced Operations
------------------

Batch Operations
~~~~~~~~~~~~~~~

Perform operations on multiple files:

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   with IOHandler() as io:
        files = io.ls("/remote/dataset")
        
        # Download multiple files at once
        txt_files = [f"/remote/dataset/{file}" for file in files if file.endswith('.txt')]
        local_paths = io.download(txt_files, "/local/dataset")



Performance Optimization
----------------------

Why RemotePathIterator?
~~~~~~~~~~~~~~~~~~~~~~

`RemotePathIterator` streams many files efficiently by batching and prefetching downloads in a background thread while your main thread consumes files. This is ideal when:

- You need steady throughput from a high-latency/high-bandwidth SFTP server
- You process files one-by-one (e.g., parsing, feature extraction)
- You want automatic local cleanup to avoid filling disks

Basic Pattern
~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler, RemotePathIterator
   
   with IOHandler() as io:
       # Build an index of files (persisted remotely unless store=False)
       iterator = RemotePathIterator(
           io_handler=io,
           batch_size=64,          # files per batch
           batch_parallel=10,      # parallel transfers per batch
           max_queued_batches=3,   # prefetch up to 3 batches
           n_local_files=64*3*2,   # keep enough local files to avoid deletion while consuming
           clear_local=True,       # automatically delete after consumption
           # kwargs forwarded to io.get_file_index()
           # store=True (default) creates a folder_index.txt on remote for faster reuse
           # pattern=r"\.jpg$" to filter
       )
       
       # Optional: change order or subset before iterating
       # iterator.shuffle()
       # iterator.subset(list_of_indices)
       
       for local_path, remote_path in iterator:
           # Process the file
           process_file(local_path, remote_path)

Controlling Throughput and Memory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **batch_size**: Larger batches reduce command overhead; increase until memory or server limits are hit
- **batch_parallel**: More parallel transfers increase network utilization; tune for server fairness and stability
- **max_queued_batches**: Prefetch depth; higher values smooth throughput but use more local storage
- **n_local_files**: Must exceed batch_size * max_queued_batches. Use 2x that as a safe default
- **clear_local**: Enable to automatically remove consumed files and control disk usage

Dataset Splits and Reuse
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler, RemotePathIterator
   
   with IOHandler() as io:
       # RemotePathIterator loops over all files in the 
       # current working directory and its subdirectories
       io.cd("/remote/training_data")
       it = RemotePathIterator(io, batch_size=64, batch_parallel=8, max_queued_batches=2)
       
       # Create non-overlapping splits for sequential use (not parallel)
       train_it, val_it = it.split(proportion=[0.8, 0.2])
       
       for lp, rp in train_it:
           train_step(lp, rp)
       
       for lp, rp in val_it:
           validate_step(lp, rp)

Indexing Strategies
~~~~~~~~~~~~~~~~~~

`RemotePathIterator` uses `io.get_file_index()` underneath. You can speed up repeated runs by persisting the index on the remote folder (default).

.. code-block:: python

   with IOHandler() as io:
       # Persisted index (default: store=True); override=True rebuilds it
       it = RemotePathIterator(io, batch_size=64, store=True, override=False)
       
       # Read-only remote? Disable store (slower)
       it_ro = RemotePathIterator(io, batch_size=64, store=False)


Best Practices
-------------

* **Use context managers**: Always use `with` statements to ensure proper cleanup
* **Handle large files**: Use streaming for files larger than available memory
* **Batch operations**: Group related operations to minimize connection overhead