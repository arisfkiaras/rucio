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

try:
    from ConfigParser import NoOptionError, NoSectionError
except ImportError:
    from configparser import NoOptionError, NoSectionError

from rucio.common import config


class JSON_METADATA_HANDLER(object):

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

    def __init__(self):
        pass

    @staticmethod
    def supports(session=None):
        return JSON_METADATA_HANDLER.SUPPORTED_OPERATIONS

    def get_did_meta(self, scope, name, session=None):
        """
        Get data identifier metadata (JSON)

        :param scope: The scope name.
        :param name: The data identifier name.
        :param session: The database session in use.
        """
        if not _json_implemented():
            raise NotImplementedError

        try:
            row = session.query(models.DidMeta).filter_by(scope=scope, name=name).one()
            meta = getattr(row, 'meta')
            return json.loads(meta) if session.bind.dialect.name in ['oracle', 'sqlite'] else meta
        except NoResultFound:
            # raise exception.DataIdentifierNotFound("No generic metadata found for '%(scope)s:%(name)s'" % locals())
            return None

    def get_did_meta_value(self):
        pass

    @staticmethod
    @transactional_session
    def set_did_meta(scope, name, key, value, session=None):
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

    @transactional_session
    def delete_did_meta(self):
        """
        Delete a key from the metadata column

        :param scope: the scope of did
        :param name: the name of the did
        :param key: the key to be deleted
        """
        if not json_meta_implemented(session=session):
            raise NotImplementedError

        try:
            row = session.query(models.DidMeta).filter_by(scope=scope, name=name).one()
            existing_meta = getattr(row, 'meta')
            # Oracle returns a string instead of a dict
            if session.bind.dialect.name in ['oracle', 'sqlite'] and existing_meta is not None:
                existing_meta = json.loads(existing_meta)

            if key not in existing_meta:
                raise exception.KeyNotFound(key)

            existing_meta.pop(key, None)

            row.meta = None
            session.flush()

            # Oracle insert takes a string as input
            if session.bind.dialect.name in ['oracle', 'sqlite']:
                existing_meta = json.dumps(existing_meta)

            row.meta = existing_meta
        except NoResultFound:
            raise exception.DataIdentifierNotFound("Key not found for data identifier '%(scope)s:%(name)s'" % locals())

    def list_dids(self, scope=None, filters=None, type=None, ignore_case=False, limit=None,
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

    def filter_query(self):
        pass

    def _json_implemented(session=None):
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

def validate_package():
    """
    Checks to see that package loaded has the minimum methods needed
    """
    pass


FALLBACK_METADATA_HANDLER = JSON_METADATA_HANDLER

if config.config_has_section('metadata'):
    try:
        METADATA_HANDLER = config.config_get('metadata', 'package')

    except (NoOptionError, NoSectionError) as error:
        # fall back to old system for now
        METADATA_HANDLER = FALLBACK_METADATA_HANDLER
else:
    METADATA_HANDLER = FALLBACK_METADATA_HANDLER


HARDCODED_KEYS = [
    "lifetime",
    "guid",
    "events",
    "adler32",
    "bytes",
    "events",

    # Fields on the did table
    "length",
    "md5",
    # "expired_at",
    # "purge_replicas",
    "deleted_at",
    "project",
    "datatype",
    "run_number",
    "stream_name",
    "prod_step",
    "version",
    "campaign",
    "task_id",
    "panda_id",
    "lumiblocknr",
    "provenance",
    "phys_group",
    "transient",
    "accessed_at",
    "closed_at",
    "eol_at",
    "is_archive",
    "constituent",
    "access_cnt",

    # Keys used while listing dids
    "created_before",
    "created_after",
    "guid",
    "length.gt",
    "length.lt",
    "length.gte",
    "length.lte",
    "length",
    "name"
]


def is_hardcoded(key):
    """
    Returns if metadata key is hardcoded.
    'Hardcoded' keys use dedicated columns in the did table and should provide the maximum search performance.

    :param key: The key to look for in the list of hardcoded keys.

    :returns: True, if hardcoded, False otherwise.
    """

    if key in HARDCODED_KEYS:# or hasattr(models.DataIdentifier, key):
        return True

    return False


def get_did_meta_interface(scope, name, filter="ALL", session=None):
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

        hardcoded_meta = _get_did_meta_hardcoded(scope, name, session=session)
        if hardcoded_meta:
            all_meta.update(hardcoded_meta)

        generic_meta = _get_generic_did_meta(scope, name, session=session)
        if generic_meta:
            all_meta.update(generic_meta)

        return all_meta
    elif filter == "HARDCODED":
        return _get_did_meta_hardcoded(scope, name, session=session)
    elif filter == "JSON":
        return _get_generic_did_meta(scope, name, session=session)


def set_did_meta_interface(scope, name, key, value, did=None,
                           recursive=False, session=None):
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

    if is_hardcoded(key):
        _set_did_meta_hardcoded(scope, name, key, value, did, recursive, session=session)
    else:
        _set_generic_did_meta(scope, name, key, value, session=session)
    # else:
    #     raise NotImplementedError


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
        if is_hardcoded(key):
            has_hardcoded = True
        else:
            has_json = True
        if has_hardcoded and has_json:
            break

    if has_hardcoded and has_json:
        # Mix case, difficult
        pass
    elif has_hardcoded:
        return _list_dids_by_hardcoded_meta(scope=scope, filters=filters, type=type,
                                          ignore_case=ignore_case, limit=limit,
                                          offset=offset, long=long, recursive=recursive, session=session)
    elif has_json:
        return _list_dids_by_generic_meta(scope=scope, filters=filters, type=type,
                                          ignore_case=ignore_case, limit=limit,
                                          offset=offset, long=long, recursive=recursive, session=session)


def filter_query_by_meta_interface(scope, filters, query):
    """
    Filters query according to metadata filters. The filters can be about 'hardcoded' metadata,
    generic metadata (json) or a mix of both.

    :param scope: The scope of the did.
    :param filters: The metadata filters.
    :param query: The sqlalchemy session.query to apply the filters to.

    :return query: The sqlalchemy session.query having the filters applied.
    """
    has_hardcoded = False
    has_json = False
    for key in filters:
        if is_hardcoded(key):
            has_hardcoded = True
        else:
            has_json = True
        if has_hardcoded and has_json:
            break

    if has_hardcoded and has_json:
        # Mix case, difficult
        pass
    elif has_hardcoded:
        query = _filter_by_did_meta_hardcoded(filters, query)
    elif has_json:
        query = _filter_by_did_meta_json(filters, query)

    return query


@transactional_session
def _set_did_meta_hardcoded(scope, name, key, value, did=None,
                            recursive=False, session=None):
    """
    Add metadata to data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param key: the key.
    :param value: the value.
    :param did: The data identifier info.
    :param recursive: Option to propagate the metadata change to content.
    :param session: The database session in use.
    """
    try:
        rowcount = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).\
            with_hint(models.DataIdentifier, "INDEX(DIDS DIDS_PK)", 'oracle').one()
    except NoResultFound:
        raise exception.DataIdentifierNotFound("Data identifier '%s:%s' not found" % (scope, name))

    if key == 'lifetime':
        try:
            expired_at = None
            if value is not None:
                expired_at = datetime.utcnow() + timedelta(seconds=float(value))
            rowcount = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).update({'expired_at': expired_at}, synchronize_session='fetch')
        except TypeError as error:
            raise exception.InvalidValueForKey(error)
    elif key in ['guid', 'events']:
        rowcount = session.query(models.DataIdentifier).filter_by(scope=scope, name=name, did_type=DIDType.FILE).update({key: value}, synchronize_session=False)

        session.query(models.DataIdentifierAssociation).filter_by(child_scope=scope, child_name=name, child_type=DIDType.FILE).update({key: value}, synchronize_session=False)
        if key == 'events':
            for parent_scope, parent_name in session.query(models.DataIdentifierAssociation.scope, models.DataIdentifierAssociation.name).filter_by(child_scope=scope, child_name=name):
                events = session.query(func.sum(models.DataIdentifierAssociation.events)).filter_by(scope=parent_scope, name=parent_name).one()[0]
                session.query(models.DataIdentifier).filter_by(scope=parent_scope, name=parent_name).update({'events': events}, synchronize_session=False)

    elif key == 'adler32':
        rowcount = session.query(models.DataIdentifier).filter_by(scope=scope, name=name, did_type=DIDType.FILE).update({key: value}, synchronize_session=False)
        session.query(models.DataIdentifierAssociation).filter_by(child_scope=scope, child_name=name, child_type=DIDType.FILE).update({key: value}, synchronize_session=False)
        session.query(models.Request).filter_by(scope=scope, name=name).update({key: value}, synchronize_session=False)
        session.query(models.RSEFileAssociation).filter_by(scope=scope, name=name).update({key: value}, synchronize_session=False)

    elif key == 'bytes':
        rowcount = session.query(models.DataIdentifier).filter_by(scope=scope, name=name, did_type=DIDType.FILE).update({key: value}, synchronize_session=False)
        session.query(models.DataIdentifierAssociation).filter_by(child_scope=scope, child_name=name, child_type=DIDType.FILE).update({key: value}, synchronize_session=False)
        session.query(models.Request).filter_by(scope=scope, name=name).update({key: value}, synchronize_session=False)

        for account, bytes, rse_id, rule_id in session.query(models.ReplicaLock.account, models.ReplicaLock.bytes, models.ReplicaLock.rse_id, models.ReplicaLock.rule_id).filter_by(scope=scope, name=name):
            session.query(models.ReplicaLock).filter_by(scope=scope, name=name, rule_id=rule_id, rse_id=rse_id).update({key: value}, synchronize_session=False)
            account_counter.decrease(rse_id=rse_id, account=account, files=1, bytes=bytes, session=session)
            account_counter.increase(rse_id=rse_id, account=account, files=1, bytes=value, session=session)

        for bytes, rse_id in session.query(models.RSEFileAssociation.bytes, models.RSEFileAssociation.rse_id).filter_by(scope=scope, name=name):
            session.query(models.RSEFileAssociation).filter_by(scope=scope, name=name, rse_id=rse_id).update({key: value}, synchronize_session=False)
            rse_counter.decrease(rse_id=rse_id, files=1, bytes=bytes, session=session)
            rse_counter.increase(rse_id=rse_id, files=1, bytes=value, session=session)

        for parent_scope, parent_name in session.query(models.DataIdentifierAssociation.scope, models.DataIdentifierAssociation.name).filter_by(child_scope=scope, child_name=name):

            values = {}
            values['length'], values['bytes'], values['events'] = session.query(func.count(models.DataIdentifierAssociation.scope),
                                                                                func.sum(models.DataIdentifierAssociation.bytes),
                                                                                func.sum(models.DataIdentifierAssociation.events)).filter_by(scope=parent_scope, name=parent_name).one()
            session.query(models.DataIdentifier).filter_by(scope=parent_scope, name=parent_name).update(values, synchronize_session=False)
            session.query(models.DatasetLock).filter_by(scope=parent_scope, name=parent_name).update({'length': values['length'], 'bytes': values['bytes']}, synchronize_session=False)
    else:
        try:
            rowcount = session.query(models.DataIdentifier).\
                with_hint(models.DataIdentifier, "INDEX(DIDS DIDS_PK)", 'oracle').\
                filter_by(scope=scope, name=name).\
                update({key: value}, synchronize_session='fetch')
        except CompileError as error:
            raise exception.InvalidMetadata(error)
        except InvalidRequestError as error:
            raise exception.InvalidMetadata("Key %s is not accepted" % key)

        # propagate metadata updates to child content
        if recursive:
            content_query = session.query(models.DataIdentifierAssociation.child_scope,
                                          models.DataIdentifierAssociation.child_name).\
                with_hint(models.DataIdentifierAssociation,
                          "INDEX(CONTENTS CONTENTS_PK)", 'oracle').\
                filter_by(scope=scope, name=name)

            for child_scope, child_name in content_query:
                try:
                    session.query(models.DataIdentifier).\
                        with_hint(models.DataIdentifier, "INDEX(DIDS DIDS_PK)", 'oracle').\
                        filter_by(scope=child_scope, name=child_name).\
                        update({key: value}, synchronize_session='fetch')
                except CompileError as error:
                    raise exception.InvalidMetadata(error)
                except InvalidRequestError as error:
                    raise exception.InvalidMetadata("Key %s is not accepted" % key)

    if not rowcount:
        # check for did presence
        raise exception.UnsupportedOperation('%(key)s for %(scope)s:%(name)s cannot be updated' % locals())


def _get_did_meta_hardcoded(scope, name, session=None):
    """
    Get data identifier metadata

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    """
    try:
        row = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).\
            with_hint(models.DataIdentifier, "INDEX(DIDS DIDS_PK)", 'oracle').one()
        d = {}
        for column in row.__table__.columns:
            d[column.name] = getattr(row, column.name)
        return d
    except NoResultFound:
        raise exception.DataIdentifierNotFound("Data identifier '%(scope)s:%(name)s' not found" % locals())


def _filter_by_did_meta_hardcoded(filters, query):
    """
    Filters query according to metadata filters. The filters can be about 'hardcoded' metadata,
    generic metadata (json) or a mix of both.

    :param filters: The metadata filters.
    :param query: The sqlalchemy session.query to apply the filters to.

    :return query: The sqlalchemy session.query having the filters applied.
    """
    for (k, v) in filters.items():
        # if isinstance(v, string_types) and ('*' in v or '%' in v):
        #     if v in ('*', '%', u'*', u'%'):
        #         continue
        #     if session.bind.dialect.name == 'postgresql':
        #         query = query.filter(getattr(models.DataIdentifier, k).
        #                              like(v.replace('*', '%').replace('_', '\_'),  # NOQA: W605
        #                                   escape='\\'))
        #     else:
        #         query = query.filter(getattr(models.DataIdentifier, k).
        #                              like(v.replace('*', '%').replace('_', '\_'), escape='\\'))  # NOQA: W605
        if k == 'created_before':
            created_before = str_to_date(v)
            query = query.filter(models.DataIdentifier.created_at <= created_before)
        elif k == 'created_after':
            created_after = str_to_date(v)
            query = query.filter(models.DataIdentifier.created_at >= created_after)
        elif k == 'guid':
            query = query.filter_by(guid=v).\
                with_hint(models.ReplicaLock, "INDEX(DIDS_GUIDS_IDX)", 'oracle')
        elif k == 'length.gt':
            query = query.filter(models.DataIdentifier.length > v)
        elif k == 'length.lt':
            query = query.filter(models.DataIdentifier.length < v)
        elif k == 'length.gte':
            query = query.filter(models.DataIdentifier.length >= v)
        elif k == 'length.lte':
            query = query.filter(models.DataIdentifier.length <= v)
        elif k == 'length':
            query = query.filter(models.DataIdentifier.length == v)
        else:
            query = query.filter(getattr(models.DataIdentifier, k) == v)

    return query


def _filter_by_did_meta_json(filters, query, scope=None, session=None):
    """
    Filters query according to metadata filters. The filters can be about 'hardcoded' metadata,
    generic metadata (json) or a mix of both.

    :param filters: The metadata filters.
    :param query: The sqlalchemy session.query to apply the filters to.

    :return query: The sqlalchemy session.query having the filters applied.
    """
    if not json_meta_implemented(session=session) or session.bind.dialect.name == 'sqlite':
        raise NotImplementedError

    query2 = session.query(models.DidMeta)
    if scope is not None:
        query2 = query2.filter(models.DidMeta.scope == scope)

    for k, v in iteritems(select):
        if session.bind.dialect.name == 'oracle':
            query2 = query2.filter(text("json_exists(meta,'$.%s?(@==''%s'')')" % (k, v)))
        else:
            query2 = query2.filter(cast(models.DidMeta.meta[k], String) == type_coerce(v, JSON))

    dids = []
    for row in query2.yield_per(10):
        dids.append(row.name)
    # If we are not scope specific, this might make did with duplicate names from different scopes to appear
    # query = query.filter(models.DataIdentifier.name.in_(dids))
    query = query.filter(tuple_(models.DataIdentifier.scope, models.DataIdentifier.name).in_(dids))

    return query


def list_dids_by_hardcoded_meta(scope, filters, type='collection', ignore_case=False, limit=None,
              offset=None, long=False, recursive=False, session=None):
    """
    Search data identifiers

    :param scope: the scope name.
    :param filters: dictionary of attributes by which the results should be filtered.
    :param type: the type of the did: all(container, dataset, file), collection(dataset or container), dataset, container, file.
    :param ignore_case: ignore case distinctions.
    :param limit: limit number.
    :param offset: offset number.
    :param long: Long format option to display more information for each DID.
    :param session: The database session in use.
    :param recursive: Recursively list DIDs content.
    """
    types = ['all', 'collection', 'container', 'dataset', 'file']
    if type not in types:
        raise exception.UnsupportedOperation("Valid type are: %(types)s" % locals())

    query = session.query(models.DataIdentifier.scope,
                          models.DataIdentifier.name,
                          models.DataIdentifier.did_type,
                          models.DataIdentifier.bytes,
                          models.DataIdentifier.length).\
        filter(models.DataIdentifier.scope == scope)

    # Exclude suppressed dids
    query = query.filter(models.DataIdentifier.suppressed != true())

    if type == 'all':
        query = query.filter(or_(models.DataIdentifier.did_type == DIDType.CONTAINER,
                                 models.DataIdentifier.did_type == DIDType.DATASET,
                                 models.DataIdentifier.did_type == DIDType.FILE))
    elif type.lower() == 'collection':
        query = query.filter(or_(models.DataIdentifier.did_type == DIDType.CONTAINER,
                                 models.DataIdentifier.did_type == DIDType.DATASET))
    elif type.lower() == 'container':
        query = query.filter(models.DataIdentifier.did_type == DIDType.CONTAINER)
    elif type.lower() == 'dataset':
        query = query.filter(models.DataIdentifier.did_type == DIDType.DATASET)
    elif type.lower() == 'file':
        query = query.filter(models.DataIdentifier.did_type == DIDType.FILE)

    for (k, v) in filters.items():

        if k not in ['created_before', 'created_after', 'length.gt', 'length.lt', 'length.lte', 'length.gte', 'length'] \
           and not hasattr(models.DataIdentifier, k):
            raise exception.KeyNotFound(k)

        if isinstance(v, string_types) and ('*' in v or '%' in v):
            if v in ('*', '%', u'*', u'%'):
                continue
            if session.bind.dialect.name == 'postgresql':
                query = query.filter(getattr(models.DataIdentifier, k).
                                     like(v.replace('*', '%').replace('_', '\_'),  # NOQA: W605
                                          escape='\\'))
            else:
                query = query.filter(getattr(models.DataIdentifier, k).
                                     like(v.replace('*', '%').replace('_', '\_'), escape='\\'))  # NOQA: W605
        elif k == 'created_before':
            created_before = str_to_date(v)
            query = query.filter(models.DataIdentifier.created_at <= created_before)
        elif k == 'created_after':
            created_after = str_to_date(v)
            query = query.filter(models.DataIdentifier.created_at >= created_after)
        elif k == 'guid':
            query = query.filter_by(guid=v).\
                with_hint(models.ReplicaLock, "INDEX(DIDS_GUIDS_IDX)", 'oracle')
        elif k == 'length.gt':
            query = query.filter(models.DataIdentifier.length > v)
        elif k == 'length.lt':
            query = query.filter(models.DataIdentifier.length < v)
        elif k == 'length.gte':
            query = query.filter(models.DataIdentifier.length >= v)
        elif k == 'length.lte':
            query = query.filter(models.DataIdentifier.length <= v)
        elif k == 'length':
            query = query.filter(models.DataIdentifier.length == v)
        else:
            query = query.filter(getattr(models.DataIdentifier, k) == v)

    if 'name' in filters:
        if '*' in filters['name']:
            query = query.\
                with_hint(models.DataIdentifier, "NO_INDEX(dids(SCOPE,NAME))", 'oracle')
        else:
            query = query.\
                with_hint(models.DataIdentifier, "INDEX(DIDS DIDS_PK)", 'oracle')

    if limit:
        query = query.limit(limit)

    if recursive:
        # Get attachted DIDs and save in list because query has to be finished before starting a new one in the recursion
        collections_content = []
        parent_scope = scope
        for scope, name, did_type, bytes, length in query.yield_per(100):
            if (did_type == DIDType.CONTAINER or did_type == DIDType.DATASET):
                collections_content += [did for did in list_content(scope=scope, name=name)]

        # List DIDs again to use filter
        for did in collections_content:
            filters['name'] = did['name']
            for result in list_dids(scope=did['scope'], filters=filters, recursive=True, type=type, limit=limit, offset=offset, long=long, session=session):
                yield result

    if long:
        for scope, name, did_type, bytes, length in query.yield_per(5):
            yield {'scope': scope,
                   'name': name,
                   'did_type': str(did_type),
                   'bytes': bytes,
                   'length': length}
    else:
        for scope, name, did_type, bytes, length in query.yield_per(5):
            yield name


def _get_generic_did_meta(scope, name, session=None):
    if "GET" in METADATA_HANDLER.supports():
        return METADATA_HANDLER.get_did_meta(scope, name, session=session)
    else:
        raise NotImplementedError


def _set_generic_did_meta(scope, name, key, value, session=None):
    if "SET" in METADATA_HANDLER.supports(session=session):
        return METADATA_HANDLER.set_did_meta(scope, name, key, value, session=session)
    else:
        raise NotImplementedError


def _delete_generic_did_meta(scope, name, key, session=None):
    if "DELETE" in METADATA_HANDLER.supports():
        return METADATA_HANDLER.delete_did_meta(scope, name, key, session=session)
    else:
        raise NotImplementedError


def _list_dids_by_generic_meta(scope=None, filters=None, type=None, ignore_case=False, limit=None,
                               offset=None, long=False, recursive=False, session=None):
    if "LIST" in METADATA_HANDLER.supports():
        return METADATA_HANDLER.list_dids(scope=scope, filters=filters, type=type,
                                          ignore_case=ignore_case, limit=limit,
                                          offset=offset, long=long, recursive=recursive, session=session)
    else:
        raise NotImplementedError
