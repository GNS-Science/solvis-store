#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest
from moto import mock_dynamodb
from solvis_store import model
from solvis_store.query import get_fault_name_rupture_ids, get_fault_name_ruptures


@mock_dynamodb
class TestRuptureIds(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetParentFaultRuptures.create_table(wait=True)
        model.RuptureSetParentFaultRuptures(
            rupture_set_id='RUPSET_ZZ',
            fault_id=0,
            fault_name="STOUT",
            ruptures=[1, 2, 3],
            rupture_count=3,
        ).save()
        model.RuptureSetParentFaultRuptures(
            rupture_set_id='RUPSET_ZZ',
            fault_id=2,
            fault_name="APA",
            ruptures=[2, 3, 4],
            rupture_count=3,
        ).save()
        model.RuptureSetParentFaultRuptures(
            rupture_set_id='RUPSET_ZZ',
            fault_id=3,
            fault_name='IPA',
            ruptures=[44, 45],
            rupture_count=2,
        ).save()
        model.RuptureSetParentFaultRuptures(
            rupture_set_id='RUPSET_ZZ',
            fault_id=4,
            fault_name='DRAUGHT',
            ruptures=[1, 2, 3, 7, 8, 9],
            rupture_count=6,
        ).save()
        return super(TestRuptureIds, self).setUp()

    def test_get_fault_name_rupture_ids_STOUT(self):
        rids = list(get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT']))
        self.assertEqual(len(rids), 3)
        assert sorted(rids) == [1, 2, 3]

    def test_get_fault_name_rupture_ids_APA(self):
        rids = list(get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['APA']))
        self.assertEqual(len(rids), 3)
        assert sorted(rids) == [2, 3, 4]

    def test_get_fault_name_rupture_ids_APA_STOUT_intersection(self):
        rids = list(get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT', 'APA'], union=False))
        self.assertEqual(len(rids), 2)
        assert sorted(rids) == [2, 3]

    def test_get_fault_name_rupture_ids_APA_STOUT_union(self):
        rids = list(get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT', 'APA'], union=True))
        self.assertEqual(len(rids), 4)
        assert sorted(rids) == [1, 2, 3, 4]

    def test_get_fault_name_rupture_ids_APA_STOUT_IPA_intersection(self):
        rids = list(get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT', 'APA', 'IPA']))
        self.assertEqual(len(rids), 0)
        assert sorted(rids) == []

    def test_get_fault_name_rupture_ids_APA_STOUT_IPA_union(self):
        rids = list(
            get_fault_name_rupture_ids(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT', 'APA', 'IPA'], union=True)
        )
        print(rids)
        self.assertEqual(len(rids), 6)
        assert sorted(rids) == [1, 2, 3, 4, 44, 45]

    def test_get_fault_name_ruptures_DRAUGHT(self):
        rsds = list(get_fault_name_ruptures(rupture_set_id='RUPSET_ZZ', fault_names=['DRAUGHT']))
        self.assertEqual(len(rsds), 6)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3, 7, 8, 9]
        for rsd in rsds:
            assert 'DRAUGHT' == rsd.fault_name

    def test_get_fault_name_ruptures_APA_STOUT_IPA_union(self):
        rids = list(
            get_fault_name_ruptures(rupture_set_id='RUPSET_ZZ', fault_names=['STOUT', 'APA', 'IPA'], union=True)
        )
        print(rids)
        self.assertEqual(len(rids), 8)
        assert sorted(set([rid.rupt_id for rid in rids])) == [1, 2, 3, 4, 44, 45]

    def tearDown(self):
        # with app.app_context():
        model.RuptureSetParentFaultRuptures.delete_table()
        return super(TestRuptureIds, self).tearDown()
