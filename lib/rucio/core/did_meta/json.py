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

def set_did_meta(scope, name, key, value, session=None):
        """
        Add or update the given metadata to the given did

        :param scope: the scope of the did
        :param name: the name of the did
        :param meta: the metadata to be added or updated
        """