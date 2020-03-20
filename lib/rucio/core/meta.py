'''
  Copyright European Organization for Nuclear Research (CERN)

  Licensed under the Apache License, Version 2.0 (the "License");
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

  Authors:
  - Vincent Garonne, <vincent.garonne@cern.ch>, 2012-2015
  - Mario Lassnig, <mario.lassnig@cern.ch>, 2013
  - Hannes Hansen, <hannes.jakob.hansen@cern.ch>, 2018

  PY3K COMPATIBLE
'''

from __future__ import print_function
from re import match

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from rucio.common.constraints import AUTHORIZED_VALUE_TYPES
from rucio.common.exception import (Duplicate, RucioException,
                                    KeyNotFound, InvalidValueForKey, UnsupportedValueType,
                                    InvalidObject, UnsupportedKeyType)
from rucio.db.sqla import models
from rucio.db.sqla.constants import DIDType, KeyType
from rucio.db.sqla.session import read_session, transactional_session

from rucio.core.did_meta import add_key_interface, del_key_interface, list_keys_interface, validate_meta_interface
from rucio.core.did_meta.meta import add_value as add_value_moved, list_values as list_values_moved

@transactional_session
def add_key(key, key_type, value_type=None, value_regexp=None, session=None):
    """
    Adds a new allowed key.

    :param key: the name for the new key.
    :param key_type: the type of the key: all(container, dataset, file), collection(dataset or container), file, derived(compute from file for collection).
    :param value_type: the type of the value, if defined.
    :param value_regexp: the regular expression that values should match, if defined.
    :param session: The database session in use.
    """
    add_key_interface(key=key, key_type=key_type, value_type=value_type, value_regexp=value_regexp, session=session)

@transactional_session
def del_key(key, session=None):
    """
    Deletes a key.

    :param key: the name for the key.
    :param session: The database session in use.
    """
    del_key_interface(key=key, session=session)

@read_session
def list_keys(session=None):
    """
    Lists all keys.

    :param session: The database session in use.

    :returns: A list containing all keys.
    """
    list_keys_interface(session=session)


@transactional_session
def add_value(key, value, session=None):
    """
    Adds a new value to a key.

    :param key: the name for the key.
    :param value: the value.
    :param session: The database session in use.
    """
    add_value_moved(key=key, value=value, session=session)

@read_session
def list_values(key, session=None):
    """
    Lists all values for a key.

    :param key: the name for the key.
    :param session: The database session in use.

    :returns: A list containing all values.
    """
    list_values_moved(key=key, session=session)


@read_session
def validate_meta(meta, did_type, session=None):
    """
    Validates metadata for a did.

    :param meta: the dictionary of metadata.
    :param meta: the type of the did, e.g, DATASET, CONTAINER, FILE.
    :param session: The database session in use.

    :returns: True
    """
    validate_meta_interface(meta=meta, did_type=did_type, session=session)
