DID Metadata
===========

Rucio supports adding Metadata on the dids.

Example:

$ add metadata Example
$ get metadata Example
$ list dids by metadata Example


Even though regular users use metadata out of the box using the CLI, advanced users and Rucio admins should be aware that in the backend there are multiple option on how to store and manage the did metadata per experiment needs.

There are two basic metadata storages supported:
 * Fixed metadata
 * Generic metadata

Those storages can be enabled/disabled by Rucio admins or they can be overwritten with experiment-specific metadata storages.

Fixed metadata
-------------------
Those metadata were first introduced by the ATLAS experiment and are "hardcoded" into Rucio. Most of them have to do with core information about a did such as the checksum and lifespan.
The performance of fixed metadata is optimal out of the box and can not be overwritten by experiment-specific modules. Access to those metadata from the CLI can be disabled though.
For the full list of "fixed" metadata you can have a look here [Link to hardcoded metadata list]

Configuration options for fixed metadata are:
[metadata]
fixed_metadata = Enabled | Disabled (Default: Enabled)

Generic metadata
-------------------
Currently Rucio supports generic metadata for DID based on a JSON blobs on relational databases solution.
Experiment can overwrite this with their own module. 

Configuration options for Generic Metadata are:
[metadata]
generic_metadata = Enabled | Disabled
generic_metadata_module = 'name of python module' (default: 'rucio.core.did_meta.json')
generic_metadata_fixed_keys = Enabled | Disabled (default: 'Disabled')

When generic_metadata_fixed_keys are enabled, Rucio admins need to predefine which keys are supported for dids.

$ rucio-admin add_key
$ rucio-admin del_key
$ rucio-admin list_keys

How to develop custom generic metadata solution
-------------------

The module you develop needs to support the following methods:

get_did_meta(scope, name, session=None)
set_did_meta(scope, name, key, value, recursive, session=None)
delete_did_meta(scope, name, key, session=None)
list_dids(scope, filters, type='collection', ignore_case=False, limit=None, offset=None, long=False, recursive=False, session=None)

Optional methods to support fixed keys:

add_key(key, key_type, value_type=None, value_regexp=None, session=None)
del_key(key, session=None)
list_keys(session=None)
validate_meta(meta, did_type, session=None)

Link to example: [pointer to ESCAPE metadata storage solution]