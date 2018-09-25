from unittest import TestCase
from myparser.workflow_parser import WorkflowParser


class TestScopeResolver(TestCase):
    def setUp(self):
        self.wp = WorkflowParser()

    def test_delta_interval_normalize_1d(self):
        self.assertEqual('1D', self.wp.normalized_delta_interval('1.00:00:00'))

    def test_delta_interval_normalize_2d(self):
        self.assertEqual('2D', self.wp.normalized_delta_interval('2.00:00:00'))

    def test_delta_interval_normalize_5d(self):
        self.assertEqual('5D', self.wp.normalized_delta_interval('5.00:00:00'))

    def test_delta_interval_normalize_1h(self):
        self.assertEqual('1H', self.wp.normalized_delta_interval('01:00:00'))

    def test_delta_interval_normalize_3h(self):
        self.assertEqual('3H', self.wp.normalized_delta_interval('03:00:00'))

    def test_delta_interval_normalize_1D_P1D(self):
        self.assertEqual('1D', self.wp.normalized_delta_interval('P1D'))

    def test_delta_interval_normalize_2D_P2D(self):
        self.assertEqual('2D', self.wp.normalized_delta_interval('P2D'))

