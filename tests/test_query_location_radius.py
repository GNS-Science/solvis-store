#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest
import random
from moto import mock_dynamodb
from solvis_store import model
from solvis_store.query import get_location_radius_rupture_ids, get_location_radius_ruptures


@mock_dynamodb
class TestRuptureIds(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetLocationDistances.create_table(wait=True)
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            distances=[float(random.randint(1000, 10000)) for n in range(3)],
            rupture_count=3,
        ).save()
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='MRO:10000',
            radius=10000,
            location='MRO',
            ruptures=[2, 3, 4],
            distances=[float(random.randint(1000, 10000)) for n in range(3)],
            rupture_count=3,
        ).save()
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='IVC:10000',
            radius=10000,
            location='IVC',
            ruptures=[44, 45],
            distances=[float(random.randint(1000, 10000)) for n in range(2)],
            rupture_count=2,
        ).save()
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='ZSD:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3, 7, 8, 9],
            distances=[float(random.randint(1000, 10000)) for n in range(6)],
            rupture_count=6,
        ).save()
        return super(TestRuptureIds, self).setUp()

    def test_get_location_radius_rupture_ids_WLG(self):
        rids = list(get_location_radius_rupture_ids(rupture_set_id='test_ruptset_id', locations=('WLG',), radius=10000))
        self.assertEqual(len(rids), 3)
        assert sorted(rids) == [1, 2, 3]

    def test_get_location_radius_rupture_ids_MRO(self):
        rids = list(get_location_radius_rupture_ids(rupture_set_id='test_ruptset_id', locations=('MRO',), radius=10000))
        self.assertEqual(len(rids), 3)
        assert sorted(rids) == [2, 3, 4]

    def test_get_location_radius_rupture_ids_MRO_WLG_intersection(self):
        rids = list(
            get_location_radius_rupture_ids(rupture_set_id='test_ruptset_id', locations=('MRO', 'WLG'), radius=10000)
        )
        self.assertEqual(len(rids), 2)
        assert sorted(rids) == [2, 3]

    def test_get_location_radius_rupture_ids_MRO_WLG_union(self):
        rids = list(
            get_location_radius_rupture_ids(
                rupture_set_id='test_ruptset_id', locations=('MRO', 'WLG'), radius=10000, union=True
            )
        )
        self.assertEqual(len(rids), 4)
        assert sorted(rids) == [1, 2, 3, 4]

    def test_get_location_radius_rupture_ids_MRO_WLG_IVC_intersection(self):
        rids = list(
            get_location_radius_rupture_ids(
                rupture_set_id='test_ruptset_id', locations=('MRO', 'WLG', 'IVC'), radius=10000
            )
        )
        self.assertEqual(len(rids), 0)
        assert sorted(rids) == []

    def test_get_location_radius_rupture_ids_MRO_WLG_IVC_union(self):
        rids = list(
            get_location_radius_rupture_ids(
                rupture_set_id='test_ruptset_id', locations=('MRO', 'WLG', 'IVC'), radius=10000, union=True
            )
        )
        print(rids)
        self.assertEqual(len(rids), 6)
        assert sorted(rids) == [1, 2, 3, 4, 44, 45]

    def test_get_location_radius_ruptures_ZSD(self):
        rsds = list(get_location_radius_ruptures(rupture_set_id='test_ruptset_id', locations=('ZSD',), radius=10000))
        self.assertEqual(len(rsds), 6)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3, 7, 8, 9]
        for rsd in rsds:
            assert 1000 <= rsd.distance <= 10000
            assert 'ZSD' == rsd.location_id

    def test_get_location_radius_ruptures_MRO_WLG_IVC_union(self):
        rids = list(
            get_location_radius_ruptures(
                rupture_set_id='test_ruptset_id', locations=('MRO', 'WLG', 'IVC'), radius=10000, union=True
            )
        )
        print(rids)
        self.assertEqual(len(rids), 8)
        assert sorted(set([rid.rupt_id for rid in rids])) == [1, 2, 3, 4, 44, 45]

    def tearDown(self):
        # with app.app_context():
        model.RuptureSetLocationDistances.delete_table()
        return super(TestRuptureIds, self).tearDown()
