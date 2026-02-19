Installation
============

`PyRemoteData` can be installed using pip from PyPI or from source.

Requirements
------------

* Python 3.10 or higher
* SSH client (for SFTP connections)
* LFTP (for high-performance file transfers)

Installing from PyPI
--------------------

The easiest way to install `PyRemoteData` is using pip:

.. code-block:: bash

   pip install pyremotedata

Installing from Source
----------------------

To install from the latest development version:

.. code-block:: bash

   git clone https://github.com/asgersvenning/pyremotedata.git
   cd pyremotedata
   pip install -e .

Verifying Installation
----------------------

After installation, you can verify that `PyRemoteData` is working correctly:

.. code-block:: python

   import pyremotedata
   print(pyremotedata.__version__)

Get started
-----------

Follow the :doc:`quickstart` guide to get up and running.
