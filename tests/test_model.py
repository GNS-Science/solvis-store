#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest

from moto import mock_dynamodb
from solvis_store import model


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.RuptureSetLocationDistances.create_table(wait=True)
        print("Migrate created table: RuptureSetLocationDistances")

    def test_table_exists(self):
        # with app.app_context():
        self.assertEqual(model.RuptureSetLocationDistances.exists(), True)

    def tearDown(self):
        # with app.app_context():
        model.RuptureSetLocationDistances.delete_table()
        return super(PynamoTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
