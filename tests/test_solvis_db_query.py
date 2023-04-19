#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest
import random
from moto import mock_dynamodb
from solvis_store import model
from solvis_store.solvis_db_query import get_rupture_ids


@mock_dynamodb
class TestWithoutDistances(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetLocationDistances.create_table(wait=True)
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            rupture_count=3,
        ).save()
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='MRO:10000',
            radius=10000,
            location='MRO',
            ruptures=[2, 3, 4],
            rupture_count=3,
        ).save()
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='IVC:10000',
            radius=10000,
            location='IVC',
            ruptures=[44, 45],
            rupture_count=2,
        ).save()
        return super(TestWithoutDistances, self).setUp()

    def test_get_rupture_ids_WLG(self):
        rsds = list(get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['WLG'], radius=10000))
        self.assertEqual(len(rsds), 3)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3]

    def test_get_rupture_ids_MRO(self):
        rsds = list(get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['MRO'], radius=10000))
        self.assertEqual(len(rsds), 3)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [2, 3, 4]

    def test_get_rupture_ids_MRO_WLG_intersection(self):
        rsds = list(get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['MRO', 'WLG'], radius=10000))
        self.assertEqual(len(rsds), 2)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [2, 3]

    def test_get_rupture_ids_MRO_WLG_union(self):
        rsds = list(
            get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['MRO', 'WLG'], radius=10000, union=True)
        )
        self.assertEqual(len(rsds), 4)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3, 4]

    def test_get_rupture_ids_MRO_WLG_IVC_intersection(self):
        rsds = list(get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['MRO', 'WLG', 'IVC'], radius=10000))
        self.assertEqual(len(rsds), 0)
        assert sorted([rsd.rupt_id for rsd in rsds]) == []

    def test_get_rupture_ids_MRO_WLG_IVC_union(self):
        rsds = list(
            get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['MRO', 'WLG', 'IVC'], radius=10000, union=True)
        )
        self.assertEqual(len(rsds), 6)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3, 4, 44, 45]

    def tearDown(self):
        # with app.app_context():
        model.RuptureSetLocationDistances.delete_table()
        return super(TestWithoutDistances, self).tearDown()


@mock_dynamodb
class TestWithDistances(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetLocationDistances.create_table(wait=True)
        model.RuptureSetLocationDistances(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3, 7, 8, 9],
            distances=[float(random.randint(1000, 10000)) for n in range(6)],
            rupture_count=3,
        ).save()
        return super(TestWithDistances, self).setUp()

    def test_get_rupture_ids_WLG(self):
        rsds = list(get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['WLG'], radius=10000))
        self.assertEqual(len(rsds), 6)
        assert sorted([rsd.rupt_id for rsd in rsds]) == [1, 2, 3, 7, 8, 9]
        for rsd in rsds:
            assert 1000 <= rsd.distance <= 10000

    def tearDown(self):
        # with app.app_context():
        model.RuptureSetLocationDistances.delete_table()
        return super(TestWithDistances, self).tearDown()
