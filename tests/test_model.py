#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest

from moto import mock_dynamodb
from solvis_store import model


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.migrate()

    def test_tables_exists(self):
        # with app.app_context():
        for m in model.table_classes:
            self.assertEqual(m.exists(), True)

    def tearDown(self):
        # with app.app_context():
        model.drop_all()
        return super(PynamoTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
