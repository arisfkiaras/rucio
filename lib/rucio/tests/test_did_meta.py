# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Asket Agarwal, <asket.agarwal96@gmail.com>
# - Hannes Hansen, <hannes.jakob.hansen@cern.ch>, 2019
# - Aristeidis Fkiaras, <aristeidis.fkiaras@cern.ch>, 2019
#
# PY3K COMPATIBLE

from nose.tools import assert_equal, assert_is_instance, assert_in, assert_raises, assert_true

from rucio.client.didclient import DIDClient
from rucio.common.utils import generate_uuid as uuid
from rucio.common.exception import DataIdentifierNotFound, KeyNotFound, UnsupportedKeyType
from rucio.db.sqla.session import get_session
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.did import (list_dids, add_did, delete_dids, get_did_atime, touch_dids, attach_dids, detach_dids,
                            get_did, get_did_access_cnt, add_did_to_followed,
                            get_users_following_did, remove_did_from_followed)
from rucio.common.utils import generate_uuid
from rucio.core.did_meta import (list_dids_interface, get_did_meta_interface, set_did_meta_interface,
                                add_key_interface, del_key_interface, list_keys_interface, validate_meta_interface)
from rucio.db.sqla.constants import DIDType, KeyType
from rucio.db.sqla import models

session = get_session()


class TestDidMetaHardcoded():

    def test_add_did_meta(self):
        tmp_scope = InternalScope('mock')
        root = InternalAccount('root')
        did_name = 'mock_did_%s' % generate_uuid()
        add_did(scope=tmp_scope, name=did_name, type='DATASET', account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=did_name, key='project', value='data12_8TeV', session=session)
        assert_equal(get_did_meta_interface(scope=tmp_scope, name=did_name, session=session)['project'], 'data12_8TeV')

    def test_get_did_meta(self):
        tmp_scope = InternalScope('mock')
        root = InternalAccount('root')
        did_name = 'mock_did_%s' % generate_uuid()
        dataset_meta = {'project': 'data12_8TeV'}
        add_did(scope=tmp_scope, name=did_name, type='DATASET', meta=dataset_meta, account=root, session=session)

        assert_equal(get_did_meta_interface(scope=tmp_scope, name=did_name, session=session)['project'], 'data12_8TeV')

    def test_list_did_meta(self):
        dsns = []
        tmp_scope = InternalScope('mock')
        tmp_dsn1 = 'dsn_%s' % generate_uuid()
        root = InternalAccount('root')

        dsns.append(tmp_dsn1)

        dataset_meta = {'project': 'data12_8TeV',
                        'run_number': 400000,
                        'stream_name': 'physics_CosmicCalo',
                        'prod_step': 'merge',
                        'datatype': 'NTUP_TRIG',
                        'version': 'f392_m920',
                        }

        add_did(scope=tmp_scope, name=tmp_dsn1, type="DATASET", account=root, meta=dataset_meta, session=session)

        tmp_dsn2 = 'dsn_%s' % generate_uuid()
        dsns.append(tmp_dsn2)
        dataset_meta['run_number'] = 400001
        add_did(scope=tmp_scope, name=tmp_dsn2, type="DATASET", account=root, meta=dataset_meta, session=session)

        tmp_dsn3 = 'dsn_%s' % generate_uuid()
        dsns.append(tmp_dsn3)
        dataset_meta['stream_name'] = 'physics_Egamma'
        dataset_meta['datatype'] = 'NTUP_SMWZ'
        add_did(scope=tmp_scope, name=tmp_dsn3, type="DATASET", account=root, meta=dataset_meta, session=session)

        dids = list_dids(tmp_scope, {'project': 'data12_8TeV', 'version': 'f392_m920'}, session=session)
        results = []
        for d in dids:
            results.append(d)
        for dsn in dsns:
            assert_in(dsn, results)
        dsns.remove(tmp_dsn1)

        dids = list_dids(tmp_scope, {'project': 'data12_8TeV', 'run_number': 400001}, session=session)
        results = []
        for d in dids:
            results.append(d)
        for dsn in dsns:
            assert_in(dsn, results)
        dsns.remove(tmp_dsn2)

        dids = list_dids(tmp_scope, {'project': 'data12_8TeV', 'stream_name': 'physics_Egamma', 'datatype': 'NTUP_SMWZ'}, session=session)
        results = []
        for d in dids:
            results.append(d)
        for dsn in dsns:
            assert_in(dsn, results)

        # with assert_raises(KeyNotFound):
        #     list_dids(tmp_scope, {'NotReallyAKey': 'NotReallyAValue'})

class TestDidMetaJSON():

    def test_add_did_meta(self):
        tmp_scope = InternalScope('mock')
        root = InternalAccount('root')
        did_name = 'mock_did_%s' % generate_uuid()
        meta_key = 'my_key_%s'  % generate_uuid()
        meta_value = 'my_value_%s'  % generate_uuid()
        add_did(scope=tmp_scope, name=did_name, type='DATASET', account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=did_name, key=meta_key, value=meta_value, session=session)

        # session.commit()

        assert_equal(get_did_meta_interface(scope=tmp_scope, name=did_name, filter='JSON', session=session)[meta_key], meta_value)

    def test_get_did_meta(self):
        tmp_scope = InternalScope('mock')
        root = InternalAccount('root')
        did_name = 'mock_did_%s' % generate_uuid()
        meta_key = 'my_key_%s'  % generate_uuid()
        meta_value = 'my_value_%s'  % generate_uuid()
        add_did(scope=tmp_scope, name=did_name, type='DATASET', account=root)
        set_did_meta_interface(scope=tmp_scope, name=did_name, key=meta_key, value=meta_value, session=session)
        assert_equal(get_did_meta_interface(scope=tmp_scope, name=did_name, filter='JSON', session=session)[meta_key], meta_value)

    def test_list_did_meta(self):
        tmp_scope = InternalScope('mock')
        root = InternalAccount('root')

        meta_key1 = 'my_key_%s'  % generate_uuid()
        meta_key2 = 'my_key_%s'  % generate_uuid()
        meta_value1 = 'my_value_%s'  % generate_uuid()
        meta_value2 = 'my_value_%s'  % generate_uuid()
        meta_value3 = 'my_value_%s'  % generate_uuid()

        tmp_dsn1 = 'dsn_%s' % generate_uuid()
        add_did(scope=tmp_scope, name=tmp_dsn1, type="DATASET", account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=tmp_dsn1, key=meta_key1, value=meta_value1, session=session)

        tmp_dsn2 = 'dsn_%s' % generate_uuid()
        add_did(scope=tmp_scope, name=tmp_dsn2, type="DATASET", account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=tmp_dsn2, key=meta_key1, value=meta_value2, session=session)

        tmp_dsn3 = 'dsn_%s' % generate_uuid()
        add_did(scope=tmp_scope, name=tmp_dsn3, type="DATASET", account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=tmp_dsn3, key=meta_key2, value=meta_value1, session=session)

        tmp_dsn4 = 'dsn_%s' % generate_uuid()
        add_did(scope=tmp_scope, name=tmp_dsn4, type="DATASET", account=root, session=session)
        set_did_meta_interface(scope=tmp_scope, name=tmp_dsn4, key=meta_key1, value=meta_value1, session=session)
        set_did_meta_interface(scope=tmp_scope, name=tmp_dsn4, key=meta_key2, value=meta_value2, session=session)


        dids = list_dids_interface(tmp_scope, {meta_key1: meta_value1}, session=session)
        results = []
        for d in dids:
            results.append(d)
        # results_clean = ['%s:%s' % (r['scope'], r['name']) for r in results]
        # print(results_clean)
        # assert_equal([{'scope': tmp_scope, 'name': u'%s' % tmp_dsn1}, {'scope': tmp_scope, 'name': u'%s' % tmp_dsn4}], results)
        assert_equal(sorted([{'scope': tmp_scope, 'name': tmp_dsn1}, {'scope': tmp_scope, 'name': tmp_dsn4}]), sorted(results))
        assert_equal(len(results), 2)

        dids = list_dids_interface(tmp_scope, {meta_key1: meta_value2}, session=session)
        results = []
        for d in dids:
            results.append(d)
        assert_equal([{'scope': (tmp_scope), 'name':str(tmp_dsn2)}], results)
        assert_equal(len(results), 1)

        dids = list_dids_interface(tmp_scope, {meta_key2: meta_value1}, session=session)
        results = []
        for d in dids:
            results.append(d)
        assert_equal([{'scope': (tmp_scope), 'name':tmp_dsn3}], results)
        assert_equal(len(results), 1)

        dids = list_dids_interface(tmp_scope, {meta_key1: meta_value1, meta_key2: meta_value2}, session=session)
        results = []
        for d in dids:
            results.append(d)
        assert_equal([{'scope': (tmp_scope), 'name':tmp_dsn4}], results)
        assert_equal(len(results), 1)

        # with assert_raises(KeyNotFound):
        #     list_dids(tmp_scope, {'NotReallyAKey': 'NotReallyAValue'})
        session.commit()

class TestDidMetaClient():

    def setup(self):
        self.did_client = DIDClient()
        self.tmp_scope = 'mock'
        self.tmp_name = 'name_%s' % uuid()
        self.did_client.add_did(scope=self.tmp_scope, name=self.tmp_name, type="DATASET")
        self.implemented = True
        session = get_session()
        if session.bind.dialect.name == 'oracle':
            oracle_version = int(session.connection().connection.version.split('.')[0])
            if oracle_version < 12:
                self.implemented = False
        elif session.bind.dialect.name == 'sqlite':
            self.implemented = False

    def test_add_did_meta(self):
        """ META (CLIENTS) : Adds a fully set json column to a did, updates if some keys present """
        if self.implemented:
            data1 = {"key1": "value_" + str(uuid()), "key2": "value_" + str(uuid()), "key3": "value_" + str(uuid())}
            self.did_client.add_did_meta(scope=self.tmp_scope, name=self.tmp_name, meta=data1)

            metadata = self.did_client.get_did_meta(scope=self.tmp_scope, name=self.tmp_name)
            assert_equal(len(metadata), 3)
            assert_equal(metadata, data1)

            data2 = {"key4": "value_" + str(uuid()), "key5": "value_" + str(uuid())}
            self.did_client.add_did_meta(scope=self.tmp_scope, name=self.tmp_name, meta=data2)

            metadata = self.did_client.get_did_meta(scope=self.tmp_scope, name=self.tmp_name)
            assert_equal(len(metadata), 5)
            assert_equal(metadata, dict(list(data1.items()) + list(data2.items())))

            with assert_raises(DataIdentifierNotFound):
                self.did_client.add_did_meta(scope=self.tmp_scope, name='Nimportnawak', meta=data1)

            data3 = {"key2": "value2", "key6": "value6"}
            self.did_client.add_did_meta(scope=self.tmp_scope, name=self.tmp_name, meta=data3)
            metadata = self.did_client.get_did_meta(scope=self.tmp_scope, name=self.tmp_name)
            assert_equal(len(metadata), 6)
            assert_equal(metadata["key2"], "value2")

    def test_delete_generic_metadata(self):
        """ META (CLIENTS) : Deletes metadata key """
        if self.implemented:
            data = {"key1": "value_" + str(uuid()), "key2": "value_" + str(uuid()), "key3": "value_" + str(uuid())}
            self.did_client.add_did_meta(scope=self.tmp_scope, name=self.tmp_name, meta=data)

            key = "key2"
            self.did_client.delete_did_meta(scope=self.tmp_scope, name=self.tmp_name, key=key)
            metadata = self.did_client.get_did_meta(scope=self.tmp_scope, name=self.tmp_name)
            assert_equal(len(metadata), 2)

            with assert_raises(KeyNotFound):
                self.did_client.delete_did_meta(scope=self.tmp_scope, name=self.tmp_name, key="key9")

    def test_get_generic_metadata(self):
        """ META (CLIENTS) : Gets all metadata for the given did """
        if self.implemented:
            data = {"key1": "value_" + str(uuid()), "key2": "value_" + str(uuid()), "key3": "value_" + str(uuid())}
            self.did_client.add_did_meta(scope=self.tmp_scope, name=self.tmp_name, meta=data)

            metadata = self.did_client.get_did_meta(scope=self.tmp_scope, name=self.tmp_name)
            assert_equal(metadata, data)

    def test_list_dids_by_generic_meta(self):
        """ META (CLIENTS) : Get all dids matching the values of the provided metadata keys """
        if self.implemented:
            tmp_scope = 'mock'
            tmp_dids = []

            did1 = 'name_%s' % uuid()
            tmp_dids.append(did1)
            self.did_client.add_did(scope=tmp_scope, name=did1, type="DATASET")
            data = {"key1": "value1"}
            self.did_client.add_did_meta(scope=tmp_scope, name=did1, meta=data)

            did2 = 'name_%s' % uuid()
            tmp_dids.append(did2)
            self.did_client.add_did(scope=tmp_scope, name=did2, type="DATASET")
            data = {"key1": "value1", "key2": "value2"}
            self.did_client.add_did_meta(scope=tmp_scope, name=did2, meta=data)

            did3 = 'name_%s' % uuid()
            tmp_dids.append(did3)
            self.did_client.add_did(scope=tmp_scope, name=did3, type="DATASET")
            data = {"key1": "value1", "key2": "value2", "key3": "value3"}
            self.did_client.add_did_meta(scope=tmp_scope, name=did3, meta=data)

            did4 = 'name_%s' % uuid()
            tmp_dids.append(did4)
            self.did_client.add_did(scope=tmp_scope, name=did4, type="DATASET")
            data = {"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"}
            self.did_client.add_did_meta(scope=tmp_scope, name=did4, meta=data)

            dids = self.did_client.list_dids_by_meta(scope=tmp_scope, select={"key1": "value1"})
            for did in tmp_dids:
                assert_in({'scope': 'mock', 'name': did}, dids)
            tmp_dids.remove(did1)

            dids = self.did_client.list_dids_by_meta(scope=tmp_scope, select={"key2": "value2"})
            for did in tmp_dids:
                assert_in({'scope': 'mock', 'name': did}, dids)
            tmp_dids.remove(did2)

            dids = self.did_client.list_dids_by_meta(scope=tmp_scope, select={"key3": "value3"})
            for did in tmp_dids:
                assert_in({'scope': 'mock', 'name': did}, dids)
            tmp_dids.remove(did3)

            dids = self.did_client.list_dids_by_meta(scope=tmp_scope, select={"key4": "value4"})
            for did in tmp_dids:
                assert_in({'scope': 'mock', 'name': did}, dids)
            tmp_dids.remove(did4)

            select_query = {"key1": "value1", "key2": "value2"}
            dids = self.did_client.list_dids_by_meta(scope=tmp_scope, select=select_query)
            assert_is_instance(dids, list)
            # Since there is no cleanup and key is not uuid, (len(dids), 3) would fail on second run.
            # assert_equal(len(dids), 3)
            assert_in({'scope': 'mock', 'name': did2}, dids)

class TestDidKey():
    
    def test_add_key(self):
        """ META (CORE): Add a new key """
        types = [{'type': DIDType.FILE, 'expected': KeyType.FILE},
                 {'type': DIDType.CONTAINER, 'expected': KeyType.CONTAINER},
                 {'type': DIDType.DATASET, 'expected': KeyType.DATASET},
                 {'type': KeyType.ALL, 'expected': KeyType.ALL},
                 {'type': KeyType.DERIVED, 'expected': KeyType.DERIVED},
                 {'type': KeyType.FILE, 'expected': KeyType.FILE},
                 {'type': KeyType.COLLECTION, 'expected': KeyType.COLLECTION},
                 {'type': KeyType.CONTAINER, 'expected': KeyType.CONTAINER},
                 {'type': KeyType.DATASET, 'expected': KeyType.DATASET},
                 {'type': 'FILE', 'expected': KeyType.FILE},
                 {'type': 'ALL', 'expected': KeyType.ALL},
                 {'type': 'COLLECTION', 'expected': KeyType.COLLECTION},
                 {'type': 'DATASET', 'expected': KeyType.DATASET},
                 {'type': 'D', 'expected': KeyType.DATASET},
                 {'type': 'FILE', 'expected': KeyType.FILE},
                 {'type': 'F', 'expected': KeyType.FILE},
                 {'type': 'DERIVED', 'expected': KeyType.DERIVED},
                 {'type': 'C', 'expected': KeyType.CONTAINER}]

        for key_type in types:
            key_name = 'datatype%s' % str(uuid())
            add_key_interface(key_name, key_type['type'])
            stored_key_type = session.query(models.DIDKey).filter_by(key=key_name).one()['key_type']
            assert_true(stored_key_type, key_type['expected'])

        with assert_raises(UnsupportedKeyType):
            add_key_interface('datatype_generic', DIDType.ARCHIVE)

        with assert_raises(UnsupportedKeyType):
            add_key_interface('datatype_generic', 'A')
