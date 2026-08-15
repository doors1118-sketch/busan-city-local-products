import datetime
import unittest

import alert_check


class PreviousCollectionDateTests(unittest.TestCase):
    def test_monday_checks_sunday_collection(self):
        monday = datetime.datetime(2026, 8, 17, 9, 0)
        self.assertEqual(alert_check._previous_collection_date(monday), "20260816")

    def test_sunday_checks_saturday_collection(self):
        sunday = datetime.datetime(2026, 8, 16, 9, 0)
        self.assertEqual(alert_check._previous_collection_date(sunday), "20260815")

    def test_regular_weekday_checks_previous_day(self):
        tuesday = datetime.datetime(2026, 8, 18, 9, 0)
        self.assertEqual(alert_check._previous_collection_date(tuesday), "20260817")


if __name__ == "__main__":
    unittest.main()
