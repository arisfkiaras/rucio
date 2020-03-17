DID Metadata
===========

Rucio supports adding Metadata on the dids.
Example:


Even though regular users use metadata out of the box using the CLI, advanced users and Rucio admins should know that in the backend there are multiple option on how to store and manage the did metadata per experiment needs.

There are three different metadata storages supported:
 * Fixed metadata
 * Controlled metadata
 * Generic metadata

Those storages can be enabled/disabled by Rucio admins or they can be overwritten with experiment-specific metadata storages.

Fixed metadata
-------------------
Those metadata were first introduced by the ATLAS experiment and are "hardcoded" into Rucio. Most of them have to do with core information about a did such as the checksum.
The performance of fixed metadata is optimal out of the box and can not be overwritten by experiment-specific modules but can be disabled.
For the full list of "fixed" metadata you can have a look here [Link to hardcoded metadata list]

Controlled metadata
-------------------
When Controlled Metadata are enabled, Rucio admins control which metadata keys are supported and users can only add values for them.

[Commands adding the metadata keys]

Generic Metadata
-------------------
If Generic Metadata are enabled, users are allowed to add any key/value per combination they wish about a DID.

Rucio has
