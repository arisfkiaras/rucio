"""
 Copyright European Organization for Nuclear Research (CERN)

 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

 Authors:
 - Vincent Garonne, <vincent.garonne@cern.ch>, 2016-2017
 - Thomas Beermann, <thomas.beermann@cern.ch>, 2017
 - Hannes Hansen, <hannes.jakob.hansen@cern.ch>, 2018

 PY3K COMPATIBLE
"""
from rucio.db.sqla.session import read_session, transactional_session, stream_session
from rucio.common.constraints import AUTHORIZED_VALUE_TYPES
from rucio.common.exception import (Duplicate, RucioException,
                                    KeyNotFound, InvalidValueForKey, UnsupportedValueType,
                                    InvalidObject, UnsupportedKeyType)
from rucio.db.sqla.constants import DIDType, KeyType
from sqlalchemy.exc import IntegrityError


from . import hardcoded as hardcoded_handler
from . import meta as default_key_handler
import importlib
from rucio.db.sqla import models

try:
    from ConfigParser import NoOptionError, NoSectionError
except ImportError:
    from configparser import NoOptionError, NoSectionError

from rucio.common import config

FALLBACK_METADATA_HANDLER = "json"

if config.config_has_section('metadata'):
    try:
        METADATA_HANDLER = config.config_get('metadata', 'package')
    except (NoOptionError, NoSectionError) as error:
        # fall back to old system for now
        METADATA_HANDLER = FALLBACK_METADATA_HANDLER
else:
    METADATA_HANDLER = FALLBACK_METADATA_HANDLER


if METADATA_HANDLER.lower() == 'json':
    METADATA_HANDLER = 'rucio.core.did_meta.json'

try:
    generic_handler = importlib.import_module(METADATA_HANDLER)
except (ImportError) as error:
    # raise exception.PolicyPackageNotFound('Module ' + POLICY + ' not found')
    raise NotImplementedError


def get_did_meta_interface(scope, name, filter="HARDCODED", session=None):
    """
    Gets the metadata for given did.
    This method has been adapted to bring the metadata from diffrent metadata stores. (hardcoded or json for now)

    :param scope: The scope of the did.
    :param name: The name of the did.
    :param filter: (optional) Filter down to specific metadata storages [ALL|HARDCODED|JSON]
    """

    if filter == "ALL":
        all_meta = {}

        hardcoded_meta = hardcoded_handler.get_did_meta(scope, name, session=session)
        if hardcoded_meta:
            all_meta.update(hardcoded_meta)

        generic_meta = generic_handler.get_did_meta(scope, name, session=session)
        if generic_meta:
            all_meta.update(generic_meta)

        return all_meta

    elif filter == "HARDCODED":
        return hardcoded_handler.get_did_meta(scope, name, session=session)
    elif filter == "JSON":
        return generic_handler.get_did_meta(scope, name, session=session)


def set_did_meta_interface(scope, name, key, value, recursive=False, session=None):
    """
    Sets the metadata for a given did.

    To decide which metadata store to use, it is checking the configuration of the server and wether the key exists
    as hardcoded.

    :param scope: The scope of the did.
    :param name: The name of the did.
    :param key: Key of the metadata.
    :param value: Value of the metadata.
    :param did: (Optional) The data identifier info.
    :param recursive: (Optional) Option to propagate the metadata change to content.
    :param session: (Optional)  The database session in use.
    """

    if hardcoded_handler.is_hardcoded(key):
        handler = hardcoded_handler
    else:
        handler = generic_handler

    handler.set_did_meta(scope, name, key, value, recursive, session=session)


def delete_did_meta_interface(scope, name, key, session=None):
    """
    Deletes the metadata stored for the given key. Currently only works for JSON metadata store

    :param scope: The scope of the did.
    :param name: The name of the did.
    :param key: Key of the metadata.
    """

    if hardcoded_handler.is_hardcoded(key):
        # Hardcoded metadata do not support deletion at the moment.
        raise NotImplementedError
    else:
        generic_handler.delete_did_meta(scope, name, key, session=session)


def list_dids_interface(scope=None, filters=None, type=None, ignore_case=False, limit=None,
                        offset=None, long=False, recursive=False, session=None):
    """
    List dids according to metadata.
    Either all of the metadata in the query should belong in the hardcoded ones, or none at all.
    A mixture of hardcoded and generic metadata is not supported at the moment.

    :param scope: The scope of the did.
    :param name: The name of the did.
    :param key: Key of the metadata.
    """
    has_hardcoded = False
    has_json = False
    for key in filters:
        if hardcoded_handler.is_hardcoded(key):
            has_hardcoded = True
        else:
            has_json = True
        if has_hardcoded and has_json:
            break

    if has_hardcoded and has_json:
        # Mix case, difficult, slow and will probably blow up memory
        raise NotImplementedError
    elif has_hardcoded:
        handler = hardcoded_handler
    else:
        handler = generic_handler

    return handler.list_dids(scope=scope, filters=filters, type=type,
                             ignore_case=ignore_case, limit=limit,
                             offset=offset, long=long, recursive=recursive, session=session)


def add_key_interface(key, key_type='DATASET', value_type=None, value_regexp=None, session=None):
    """
    Adds a new allowed key.
    Only relevant if fixed key configuration is Enabled. 

    :param key: the name for the new key.
    :param key_type: the type of the key: all(container, dataset, file), collection(dataset or container), file, derived(compute from file for collection).
    :param value_type: the type of the value, if defined.
    :param value_regexp: the regular expression that values should match, if defined.
    :param session: The database session in use.
    """
    if hardcoded_handler.is_hardcoded(key):
        # Hardcoded metadata do not support locking at the moment.
        raise NotImplementedError

    # If generic_handlers supports fixed keys will pass the argument, else will manage using the DIDMeta
    if hasattr(generic_handler, 'add_key'):
        generic_handler.add_key(key=key, key_type=key_type, value_type=value_type, value_regexp=value_regexp, session=session)
    else:
        default_key_handler.add_key(key=key, key_type=key_type, value_type=value_type, value_regexp=value_regexp, session=session)


@transactional_session
def del_key_interface(key, session=None):
    """
    Deletes a key.

    :param key: the name for the key.
    :param session: The database session in use.
    """
    if hardcoded_handler.is_hardcoded(key):
        # Hardcoded metadata do not support locking at the moment.
        raise NotImplementedError

    # If generic_handlers supports fixed keys will pass the argument, else will manage using the DIDMeta
    if hasattr(generic_handler, 'del_key'):
        generic_handler.del_key(key=key, session=session)
    else:
        default_key_handler.del_key(key=key, session=session)


@read_session
def list_keys_interface(session=None):
    """
    Lists all keys.

    :param session: The database session in use.

    :returns: A list containing all keys.
    """
    if hardcoded_handler.is_hardcoded(key):
        # Hardcoded metadata do not support locking at the moment.
        raise NotImplementedError

    # If generic_handlers supports fixed keys will pass the argument, else will manage using the DIDMeta
    if hasattr(generic_handler, 'list_keys'):
        return generic_handler.list_keys(session=session)
    else:
        return default_key_handler.list_keys(session=session)


@read_session
def validate_meta_interface(meta, did_type, session=None):
    """
    Validates metadata for a did.

    :param meta: the dictionary of metadata.
    :param meta: the type of the did, e.g, DATASET, CONTAINER, FILE.
    :param session: The database session in use.

    :returns: True
    """
    # If generic_handlers supports fixed keys will pass the argument, else will manage using the DIDMeta
    if hasattr(generic_handler, 'validate_meta'):
        return generic_handler.validate_meta(meta, did_type, session=session)
    else:
        return default_key_handler.validate_meta(meta, did_type, session=session)
