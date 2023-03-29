#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest
import random
from moto import mock_dynamodb
from solvis_store import model
from solvis_store.solvis_db_query import get_rupture_ids, matched_rupture_sections_gdf, get_all_solution_ruptures


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetLocationRadiusRuptures.create_table(wait=True)
        dataframe = model.RuptureSetLocationRadiusRuptures(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            rupture_count=3,
        )
        dataframe.save()

    def test_get_rupture_ids(self):
        ids = get_rupture_ids(rupture_set_id='test_ruptset_id', locations=['WLG'], radius=10000)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, set([1, 2, 3]))


    def test_save_with_distances(self):
        dataframe = model.RuptureSetLocationRadiusRuptures(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            distances=[random.randint(1000, 10000) for n in range(3) ],
            rupture_count=3,
        )
        dataframe.save()

        # sr = get_all_solution_ruptures('test_solution_id')
        # print(sr)
        # self.assertEqual(sr.shape, (1, 10))


    def tearDown(self):
        # with app.app_context():
        model.RuptureSetLocationRadiusRuptures.delete_table()
        return super(PynamoTest, self).tearDown()


@mock_dynamodb
class PynamoTestMatchedSections(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.SolutionRupture.create_table(wait=True)
        model.RuptureSetFaultSection.create_table(wait=True)
        dataframe = model.SolutionRupture(
            solution_id='test_solution_id',
            rupture_index_rk='1',
            rupture_index=1,
            rupture_set_id='test_ruptset_id',
            magnitude=6.66,
            avg_rake=99,
            area_m2=10e7,
            length_m=1e5,
            annual_rate=0.05,  # Annual Rate
            fault_sections=[0, 1, 2, 3, 4, 5],
        )
        dataframe.save()

        for sect in range(5):
            dataframe = model.RuptureSetFaultSection(
                rupture_set_id='test_ruptset_id',
                section_index_rk=str(sect),
                section_index=sect,
                fault_name=f"YABBY_{sect}",
                dip_degree=90,
                rake=180,
                low_depth=10e3,
                up_depth=0,
                dip_dir=270,
                aseismic_slip_factor=1.0,
                coupling_coeff=1.0,
                slip_rate=4 * sect,
                parent_id=0,
                parent_name="YABBY",
                slip_rate_std_dev=0.87,
                geometry="POINT (0.0 0.0)",
            )
            dataframe.save()

    def test_get_rupture_ids_all(self):
        sr = get_all_solution_ruptures('test_solution_id')
        print(sr)
        self.assertEqual(sr.shape, (1, 10))

    def test_matched_rupture_sections_gdf(self):
        gdf = matched_rupture_sections_gdf(
            solution_id="test_solution_id",
            rupture_set_id='test_ruptset_id',
            locations="",
            radius=None,
            min_rate=0,
            max_rate=1e-1,
            min_mag=1,
            max_mag=8,
            union=False,
        )
        print(gdf)
        assert not gdf.empty
        self.assertEqual(gdf.shape, (5, 22))



    def test_location_matched_query(self):
        model.RuptureSetLocationRadiusRuptures.create_table(wait=True)
        dataframe = model.RuptureSetLocationRadiusRuptures(
            rupture_set_id='test_ruptset_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            distances=[random.randint(1000, 10000) for n in range(3) ],
            rupture_count=3,
        )
        dataframe.save()

        gdf = matched_rupture_sections_gdf(
            solution_id="test_solution_id",
            rupture_set_id='test_ruptset_id',
            locations="WLG",
            radius=10000,
            min_rate=0,
            max_rate=1e-1,
            min_mag=1,
            max_mag=8,
            union=False,
        )
        print(gdf)
        assert not gdf.empty
        self.assertEqual(gdf.shape, (5, 22))

        print(gdf)
        assert 0



    def tearDown(self):
        model.SolutionRupture.delete_table()
        model.RuptureSetFaultSection.delete_table()
        return super(PynamoTestMatchedSections, self).tearDown()
