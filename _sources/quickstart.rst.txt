Quick Start Guide
=================

This guide will help you get started with PyRemoteData in just a few minutes.

Installation
------------

.. code-block:: bash

   pip install pyremotedata

Basic Usage
-----------

Interactive Setup
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyremotedata.implicit_mount import IOHandler
   
   # This will prompt you for connection details
   handler = IOHandler()
   
   with handler as io:
       files = io.ls()  # List files in remote directory
       print(f"Found {len(files)} files")

Automated Setup
~~~~~~~~~~~~~~~

For non-interactive environments, set environment variables before importing:

.. code-block:: python
   
   # Preferably set these globally, e.g. in .bashrc on Linux
   import os
   os.environ["PYREMOTEDATA_REMOTE_USERNAME"] = "your_username"
   os.environ["PYREMOTEDATA_REMOTE_URI"] = "storage.example.com"
   os.environ["PYREMOTEDATA_REMOTE_DIRECTORY"] = "/your/project/path"
   os.environ["PYREMOTEDATA_AUTO"] = "yes"
   
   from pyremotedata.implicit_mount import IOHandler
   
   handler = IOHandler()
   
   with handler as io:
       files = io.ls()
       print(f"Available files: {files}")

File Operations
---------------

Download a file:

.. code-block:: python

   with IOHandler() as io:
       local_path = io.download("/remote/file.txt")
       print(f"Downloaded to: {local_path}")

Upload a file:

.. code-block:: python

   with IOHandler() as io:
       remote_path = io.upload("/local/file.txt")
       print("File uploaded successfully")

Next Steps
----------

* Explore the :doc:`usage` guide for advanced features
* Check the :doc:`api/implicit_mount` for complete API reference
