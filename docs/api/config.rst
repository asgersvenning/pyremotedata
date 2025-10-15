Configuration Module
====================

The `pyremotedata.config` module provides configuration loading and access for PyRemoteData. Most users only need a small subset of the API.

What most users need
--------------------

Public API
~~~~~~~~~~

.. autofunction:: pyremotedata.config.get_config

.. autofunction:: pyremotedata.config.get_implicit_mount_config

.. autofunction:: pyremotedata.config.remove_config

Typical usage
~~~~~~~~~~~~~

Environment-first (non-interactive) setup:

.. code-block:: python

   import os
   from pyremotedata.config import get_config

   os.environ["PYREMOTEDATA_REMOTE_USERNAME"] = "username"
   os.environ["PYREMOTEDATA_REMOTE_URI"] = "io.erda.au.dk"
   os.environ["PYREMOTEDATA_LOCAL_DIRECTORY"] = "/tmp/pyremotedata"
   os.environ["PYREMOTEDATA_REMOTE_DIRECTORY"] = "/project/data"
   os.environ["PYREMOTEDATA_AUTO"] = "yes"  # disable prompts

   cfg = get_config(validate=True)
   print(cfg["implicit_mount"])  # dict with keys: user, remote, local_dir, default_remote_dir, lftp

Access the implicit mount configuration directly:

.. code-block:: python

   from pyremotedata.config import get_implicit_mount_config

   implicit_cfg = get_implicit_mount_config(validate=True)
   print(implicit_cfg["user"], implicit_cfg["remote"])  # username and server

Regenerate configuration (cleanup):

.. code-block:: python

   from pyremotedata.config import remove_config
   remove_config()  # deletes the stored YAML; it will be recreated on next get_config()

Environment variables
---------------------

The module recognizes these environment variables:

- ``PYREMOTEDATA_REMOTE_USERNAME``
- ``PYREMOTEDATA_REMOTE_URI``
- ``PYREMOTEDATA_LOCAL_DIRECTORY``
- ``PYREMOTEDATA_REMOTE_DIRECTORY``
- ``PYREMOTEDATA_AUTO`` (set to "yes" to disable interactive prompts)

Advanced (optional)
-------------------

These functions are primarily for advanced or programmatic setups and are not required for typical usage:

.. autofunction:: pyremotedata.config.create_default_config

.. rubric:: Notes

- When `validate=True`, `get_config` compares the active environment to the stored config. If they differ, the config is removed and recreated to prevent stale settings.
- The configuration file is stored next to the package installation by default.
