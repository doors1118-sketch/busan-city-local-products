from __future__ import annotations

import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse
from pathlib import Path

from company_reconcile import CompanyLookupError, make_verified_company_lookup_client


class CompanyLookupContractTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(
            Path(__file__).with_name("test_company_lookup_contract_fixture.json").read_text(encoding="ascii")
        )

    def test_lookup_uses_captured_contract_and_normalized_business_number(self):
        payload = self.fixture["success_response"]

        class Response:
            def read(self, _size=-1):
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", self.fixture["request"]["serviceKey"])
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()) as open_url:
            item = client.lookup("123-45-67890")
        query = parse_qs(urlparse(open_url.call_args.args[0].full_url).query)
        self.assertEqual(query, {key: [value] for key, value in self.fixture["request"].items()})
        self.assertEqual(item["bizno"], "1234567890")

    def test_lookup_accepts_only_explicit_not_found_as_authoritative_empty(self):
        payload = self.fixture["not_found_response"]

        class Response:
            def read(self, _size=-1):
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", "service-key")
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()):
            self.assertEqual(client.lookup("1234567890"), [])

    def test_lookup_fails_closed_when_captured_success_code_drifts(self):
        payload = self.fixture["success_response"]

        class Response:
            def read(self, _size=-1):
                payload["response"]["header"]["resultCode"] = "99"
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", "service-key")
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()):
            with self.assertRaisesRegex(CompanyLookupError, "resultCode=99"):
                client.lookup("1234567890")

    def test_lookup_fails_closed_on_empty_success_payload(self):
        class Response:
            def read(self, _size=-1):
                return b'{"response":{"header":{"resultCode":"00"},"body":{"items":[],"totalCount":0,"numOfRows":1,"pageNo":1}}}'

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", "service-key")
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()):
            with self.assertRaisesRegex(CompanyLookupError, "authoritative"):
                client.lookup("1234567890")
