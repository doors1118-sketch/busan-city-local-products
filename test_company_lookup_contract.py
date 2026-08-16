from __future__ import annotations

import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from company_reconcile import CompanyLookupError, make_verified_company_lookup_client


class CompanyLookupContractTests(unittest.TestCase):
    def test_lookup_uses_normalized_business_number_and_validates_success_response(self):
        class Response:
            def read(self, _size=-1):
                return json.dumps(
                    {
                        "response": {
                            "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."},
                            "body": {
                                "items": {"item": [{"bizno": "1234567890", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "202608160900"}]},
                                "totalCount": 1,
                                "numOfRows": 1,
                                "pageNo": 1,
                            },
                        }
                    }
                ).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", "service-key")
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()) as open_url:
            item = client.lookup("123-45-67890")
        query = parse_qs(urlparse(open_url.call_args.args[0].full_url).query)
        self.assertEqual(query, {
            "serviceKey": ["service-key"], "inqryDiv": ["2"], "bizno": ["1234567890"],
            "numOfRows": ["1"], "pageNo": ["1"], "type": ["json"],
        })
        self.assertEqual(item["bizno"], "1234567890")

    def test_lookup_accepts_only_explicit_not_found_as_authoritative_empty(self):
        class Response:
            def read(self, _size=-1):
                return b'{"response":{"header":{"resultCode":"03"},"body":{}}}'

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        client = make_verified_company_lookup_client("https://supplier.example/api", "service-key")
        with patch("company_reconcile.urllib.request.urlopen", return_value=Response()):
            self.assertEqual(client.lookup("1234567890"), [])

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
