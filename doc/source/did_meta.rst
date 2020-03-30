DID Metadata
===========

Rucio supports adding Metadata on the dids.

Example::

    $ add metadata Example
    $ get metadata Example
    $ list dids by metadata Example


Even though regular users use metadata out of the box using the CLI, advanced users and Rucio admins should be aware that in the backend there are multiple options on how to store and manage the did metadata per experiment needs.

The concepts of DID Metadata Plugins exists on Rucio. While deploying the Rucio server you can configure which existing did plugins to use or even develop your own.

The default plugin in use the one originally developed for the needs of ATLAS, stores the metadata on fixed columns on the DID table and is the most optimal for the specific metadata.

Another option available is the JSON metadata plugin which stores the metadata in JSON blobs in the relational databased used by the Rucio Server.

When you are trying to add or fetch a VALUE for a given KEY, Rucio which asks in order each configured metadata plugin if it supports this KEY.

How to develop a custom metadata solution
-------------------

The module you develop needs to extend the [DidMetaPlugin](/) Abstract class. Basically the methods needed are ::

    get_metadata(scope, name, session=None)
    set_metadata(scope, name, key, value, recursive, session=None)
    delete_metadata(scope, name, key, session=None)
    list_dids(scope, filters, type='collection', ignore_case=False, limit=None, offset=None, long=False, recursive=False, session=None)
    manages_key(key)


How to configure which metadata plugin to use
-------------------
Configuration options for Metadata are::

    [metadata]
    # plugins = [list_of_plugins,comma_separated]
    # Example (escape.rucio.did_meta_plugin needs to be installed separately on web server for this to work)
    plugins = [rucio.core.did_meta_plugins.did_column_meta.DidColumnMeta, escape.rucio.did_meta_plugin]

