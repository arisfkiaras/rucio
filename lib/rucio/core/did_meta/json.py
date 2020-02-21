import json
import logging
import random
import sys

from datetime import datetime, timedelta
from hashlib import md5
from re import match
from six import string_types, iteritems

from sqlalchemy import and_, or_, exists, String, cast, type_coerce, JSON, tuple_#, in_
from sqlalchemy.exc import DatabaseError, IntegrityError, CompileError, InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import not_, func
from sqlalchemy.sql.expression import bindparam, case, text, Insert, select, true

import rucio.core.rule
import rucio.core.replica  # import add_replicas

from rucio.common import exception
from rucio.common.utils import str_to_date, is_archive, chunks
from rucio.core import account_counter, rse_counter, config as config_core
from rucio.core.message import add_message
from rucio.core.monitor import record_timer_block, record_counter
from rucio.core.naming_convention import validate_name
from rucio.db.sqla import models
from rucio.db.sqla.constants import DIDType, DIDReEvaluation, DIDAvailability, RuleState
from rucio.db.sqla.enum import EnumSymbol
from rucio.db.sqla.session import read_session, transactional_session, stream_session

SUPPORTED_OPERATIONS = [
    "SET",
    "GET",
    "LIST",
    "DELETE",
    "QUERY",
    "COMPARATORS",
    "FILTER_QUERY",
    "LONG"
]

def json_implemented():
        """
        Checks if the database on the current server installation can support json fields.
        Check if did meta json table exists.

        :param session: (Optional) The active session of the database.

        :returns: True, if json is supported, False otherwise.
        """

        if session.bind.dialect.name == 'oracle':
            oracle_version = int(session.connection().connection.version.split('.')[0])
            if oracle_version < 12:
                return False
        #TODO: check for the table here
        return True

def supports(session=None):
    if json_imlemented():
        return JSON_METADATA_HANDLER.SUPPORTED_OPERATIONS
    else:
        return []

def get_did_meta(scope, name, session=None):
    """
    Get data identifier metadata (JSON)

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    """
    if not json_implemented():
        raise NotImplementedError

    try:
        row = session.query(models.DidMeta).filter_by(scope=scope, name=name).one()
        meta = getattr(row, 'meta')
        return json.loads(meta) if session.bind.dialect.name in ['oracle', 'sqlite'] else meta
    except NoResultFound:
        # raise exception.DataIdentifierNotFound("No generic metadata found for '%(scope)s:%(name)s'" % locals())
        return None

def set_did_meta(scope, name, key, value, recursive, session=None):
    """
    Add or update the given metadata to the given did

    :param scope: the scope of the did
    :param name: the name of the did
    :param meta: the metadata to be added or updated
    """
    if not json_meta_implemented():
        raise NotImplementedError

    try:
        row_did = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).one()
        row_did_meta = session.query(models.DidMeta).filter_by(scope=scope, name=name).scalar()
        if row_did_meta is None:
            # Add metadata column to new table (if not already present)
            row_did_meta = models.DidMeta(scope=scope, name=name)
            row_did_meta.save(session=session, flush=True)

        existing_meta = getattr(row_did_meta, 'meta')

        # Oracle returns a string instead of a dict
        if session.bind.dialect.name in ['oracle', 'sqlite'] and existing_meta is not None:
            existing_meta = json.loads(existing_meta)

        if existing_meta is None:
            existing_meta = {}

        for k, v in iteritems(meta):
            existing_meta[k] = v

        row_did_meta.meta = None
        session.flush()

        # Oracle insert takes a string as input
        if session.bind.dialect.name in ['oracle', 'sqlite']:
            existing_meta = json.dumps(existing_meta)

        row_did_meta.meta = existing_meta
    except NoResultFound:
        raise exception.DataIdentifierNotFound("Data identifier '%(scope)s:%(name)s' not found" % locals())

def list_dids(scope, filters, type='collection', ignore_case=False, limit=None,
              offset=None, long=False, recursive=False, session=None):
    # Currently for sqlite only add, get and delete is implemented.
    if session.bind.dialect.name == 'sqlite':
        raise NotImplementedError
    if session.bind.dialect.name == 'oracle':
        oracle_version = int(session.connection().connection.version.split('.')[0])
        if oracle_version < 12:
            raise NotImplementedError

    query = session.query(models.DidMeta)
    if scope is not None:
        query = query.filter(models.DidMeta.scope == scope)

    for k, v in iteritems(select):
        if session.bind.dialect.name == 'oracle':
            query = query.filter(text("json_exists(meta,'$.%s?(@==''%s'')')" % (k, v)))
        else:
            query = query.filter(cast(models.DidMeta.meta[k], String) == type_coerce(v, JSON))
    dids = list()
    for row in query.yield_per(10):
        dids.append({'scope': row.scope, 'name': row.name})
    return dids