Usage with ERDA
===============
Using `PyRemoteData` with ERDA \(Electronic Research Data Archive at Aarhus University\) can be done in one of two ways.

Persistent SSH Setup
--------------------

This setup requires SSH and registering a authorized public key in ERDA under *Setup→SFTP→Public Keys*.

.. NOTE::
    The password option under *Setup→SFTP→Password* must be empty!

Then add the following to your SSH config (`~/.ssh/config`):

.. code-block:: text

    Host io.erda.au.dk erda
      HostName io.erda.au.dk
      User <EMAIL/USER>
      VerifyHostKeyDNS ask
      Port 2222


.. TIP::
    If you are unsure of what your ERDA "username" is, check *Setup→SFTP→Login Details→Username*.

Lastly, make sure to accept the fingerprint for the ERDA server, which you can do by running `sftp erda` in the terminal after the previous steps.
When prompted, compare the presented fingerprint with the expected one, which can again be found under *Setup→SFTP→Login Details*.

.. CAUTION::
    You should never share your private SSH keys, as such this option is best for stable environments such as workstations or production, and NOT ideal for testing.

Sharelink Access
----------------
.. DANGER::
    Using sharelinks to access ERDA is only semi-safe, as long as the sharelink is secret, however `PyRemoteData` does not work with read-only sharelinks.

If you want to access or deposit data from a non-trusted environment, create a read-write sharelink to a staging ERDA folder --- make sure to have a backup that isn't accessible via the sharelink!

Then simply copy the sharelink and use it like so:

.. code-block:: python

    from pyremotedata.implicit_mount import IOHandler

    SHARELINK_ID = "<MY_SHARELINK>" # E.g.: aFkda81mqa

    with IOHandler(user=SHARELINK_ID, password=SHARELINK_ID) as io:
        ...


This will allow you to skip all SSH and `PyRemoteData` configuration setup, which can be cumbersome in many environments, at the cost of some safety.
To accommodate the less safe nature of this method I suggest that you adopt as many of the following principles as possible:

* Never share the Sharelink ID, do **NOT** put it on GitHub or similar.
* Do **NOT** make write Sharelinks to folders which store permanent data.
* Delete Sharelinks when you are done using the "Share Links" app in ERDA.
* Use a staging folder to bridge semi-trusted to trusted barrier: 
    1. Create a read/write sharelink to a new staging folder on ERDA.
    2. Transfer resources which are needed by semi-trusted user(s) using a trusted user to the staging folder.
    3. Semi-trusted user(s) download resources and uploads results to the staging folder.
    4. Trusted user verifies and transfers results from the staging folder to trusted storage.

When using `PyRemoteData` with the "Persistent SSH Setup" method you should consider your process as trusted, and act accordingly.

.. TIP::
    You can add the "**Share Links**" app to ERDA by: 

    * click the "**Home**" icon (left-hand menu)
    * click "**Add**"
    * select "**Share Links**"
    * click "**Save**"