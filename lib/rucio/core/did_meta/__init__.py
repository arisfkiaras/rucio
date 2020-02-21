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

from . import hardcoded as hardcoded_handler
import importlib

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
    GENERIC_HANDLER_NAME = 'rucio.core.did_meta.json'  # importlib.import_module('rucio.core.did_meta.json')
elif METADATA_HANDLER.lower() == 'sql_table':
    GENERIC_HANDLER_NAME = 'sql_table'
    raise NotImplementedError
else:
    # 
    raise NotImplementedError


try:
    generic_handler = importlib.import_module(GENERIC_HANDLER_NAME)
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
    # metadata_store = "config_value"

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
    Sets the metadata for given did.
    This method has been adapted to bring the metadata from diffrent metadata stores. (hardcoded or json for now)
    To decide which metadata store to use, it is checking the configuration of the server and wether the key exists
    as hardcoded.

    :param scope: The scope of the did.
    :param name: The name of the did.
    :param key: Key of the metadata.
    :param value: Value of the metadata.
    :param did: (Optional) The data identifier info.
    :param recursive: (Optional) Option to propagate the metadata change to content.
    :param session: (Optional)  The database session in use.
    :param filter: (Optional) Filter down to specific metadata storages [ALL|HARDCODED|JSON]
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

    if is_hardcoded(key):
        pass
    else:
        _delete_did_meta_json(scope, name, key, session=session)


def list_dids_interface(scope=None, filters=None, type=None, ignore_case=False, limit=None,
                        offset=None, long=False, recursive=False, session=None):
    has_hardcoded = False
    has_json = False
    for key in filters:
        if hardcoded_handler.is_hardcoded(key):
            has_hardcoded = True
        else:
            print(key)
            has_json = True
        if has_hardcoded and has_json:
            break

    if has_hardcoded and has_json:
        # Mix case, difficult
        pass
    elif has_hardcoded:
        handler = hardcoded_handler
    else:
        handler = generic_handler

    return handler.list_dids(scope=scope, filters=filters, type=type,
                             ignore_case=ignore_case, limit=limit,
                             offset=offset, long=long, recursive=recursive, session=session)
