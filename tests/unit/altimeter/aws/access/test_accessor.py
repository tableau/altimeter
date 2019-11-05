import datetime
import os
from unittest import TestCase

from altimeter.aws.access.accessor import (
    AccessStep,
    MultiHopAccessor,
    SessionCache,
    SessionCacheValue,
)

import boto3
from moto import mock_sts


class TestSessionCacheValue(TestCase):
    def test_is_expired_when_expired(self):
        cache_value = SessionCacheValue(
            None, datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        )
        self.assertTrue(cache_value.is_expired())

    def test_is_expired_when_not_expired(self):
        cache_value = SessionCacheValue(
            None, datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
        )
        self.assertFalse(cache_value.is_expired())


class TestSessionCache(TestCase):
    def test_build_key(self):
        key = SessionCache._build_key(
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        self.assertEqual(key, "1234:test_role:test_role_session:test_region")

    def test_get_miss(self):
        cache = SessionCache()
        cached_session = cache.get(
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        self.assertIsNone(cached_session)

    def test_put_get_not_expired(self):
        cache = SessionCache()
        session = boto3.Session()
        cache.put(
            session=session,
            expiration=datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        cached_session = cache.get(
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        self.assertIsNotNone(cached_session)

    def test_put_get_expired(self):
        cache = SessionCache()
        session = boto3.Session()
        cache.put(
            session=session,
            expiration=datetime.datetime.utcnow() - datetime.timedelta(minutes=5),
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        cached_session = cache.get(
            account_id="1234",
            role_name="test_role",
            role_session_name="test_role_session",
            region="test_region",
        )
        self.assertIsNone(cached_session)


class TestAccessStep(TestCase):
    def test_str_with_account(self):
        access_step = AccessStep(role_name="test_role_name", account_id="1234")
        self.assertEqual(str(access_step), "test_role_name@1234")

    def test_str_without_account(self):
        access_step = AccessStep(role_name="test_role_name")
        self.assertEqual(str(access_step), "test_role_name@target")

    def test_to_dict(self):
        access_step = AccessStep(role_name="test_role_name", account_id="1234", external_id="abcd")
        expected_dict = {"role_name": "test_role_name", "account_id": "1234", "external_id": "abcd"}
        self.assertEqual(expected_dict, access_step.to_dict())

    def test_from_dict(self):
        d = {"role_name": "test_role_name", "account_id": "1234", "external_id": "abcd"}
        access_step = AccessStep.from_dict(d)
        self.assertEqual(access_step.to_dict(), d)

    def test_from_dict_without_role_name(self):
        d = {"account_id": "1234", "external_id": "abcd"}
        with self.assertRaises(ValueError):
            AccessStep.from_dict(d)

    def test_from_dict_with_external_id_env_var(self):
        d = {"role_name": "test_role_name", "account_id": "1234", "external_id_env_var": "EXT_ID"}
        os.environ["EXT_ID"] = "abcd"
        try:
            access_step = AccessStep.from_dict(d)
            expected_dict = {
                "role_name": "test_role_name",
                "account_id": "1234",
                "external_id": "abcd",
            }
            self.assertEqual(access_step.to_dict(), expected_dict)
        finally:
            del os.environ["EXT_ID"]

    def test_from_dict_with_external_id_env_var_missing_var(self):
        d = {"role_name": "test_role_name", "account_id": "1234", "external_id_env_var": "EXT_ID"}
        with self.assertRaises(ValueError):
            AccessStep.from_dict(d)


@mock_sts
class TestMultiHopAccessor(TestCase):
    def test_get_session(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="1234", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="5678"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        session = mha.get_session("4566")
        self.assertIsInstance(session, boto3.Session)
        expected_cache_sorted_keys = [
            "1234:test_role_name1:test_role_session_name:None",
            "4566:test_role_name3:test_role_session_name:None",
            "5678:test_role_name2:test_role_session_name:None",
        ]
        self.assertEqual(sorted(mha.session_cache._cache.keys()), expected_cache_sorted_keys)

    def test_without_access_steps(self):
        with self.assertRaises(ValueError):
            MultiHopAccessor(role_session_name="test_role_session_name", access_steps=[])

    def test_with_access_steps_non_final_missing_account_id(self):
        access_steps = [
            AccessStep(role_name="test_role_name1"),
            AccessStep(role_name="test_role_name2"),
        ]
        with self.assertRaises(ValueError):
            MultiHopAccessor(role_session_name="test_role_session_name", access_steps=access_steps)

    def test_with_access_steps_final_with_account_id(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="1234"),
            AccessStep(role_name="test_role_name2", account_id="5678"),
        ]
        with self.assertRaises(ValueError):
            MultiHopAccessor(role_session_name="test_role_session_name", access_steps=access_steps)

    def test_cache_usage(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="1234", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="5678"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        mha.get_session("4567")
        mha.get_session("4567")
        mha.get_session("8901")
        mha.get_session("8901")
        expected_cache_sorted_keys = [
            "1234:test_role_name1:test_role_session_name:None",
            "4567:test_role_name3:test_role_session_name:None",
            "5678:test_role_name2:test_role_session_name:None",
            "8901:test_role_name3:test_role_session_name:None",
        ]
        self.assertEqual(sorted(mha.session_cache._cache.keys()), expected_cache_sorted_keys)

    def test_str(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="1234", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="5678"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        expected_str = "accessor:test_role_session_name:test_role_name1@1234,test_role_name2@5678,test_role_name3@target"
        self.assertEqual(str(mha), expected_str)

    def test_to_dict(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="1234", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="5678"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        expected_dict = {
            "role_session_name": "test_role_session_name",
            "access_steps": [
                {"role_name": "test_role_name1", "external_id": "abcd", "account_id": "1234"},
                {"role_name": "test_role_name2", "external_id": None, "account_id": "5678"},
                {"role_name": "test_role_name3", "external_id": None, "account_id": None},
            ],
        }
        self.assertEqual(mha.to_dict(), expected_dict)

    def test_from_dict(self):
        mha_dict = {
            "role_session_name": "test_role_session_name",
            "access_steps": [
                {"role_name": "test_role_name1", "external_id": "abcd", "account_id": "1234"},
                {"role_name": "test_role_name2", "external_id": None, "account_id": "5678"},
                {"role_name": "test_role_name3", "external_id": None, "account_id": None},
            ],
        }
        mha = MultiHopAccessor.from_dict(mha_dict)
        self.assertEqual(mha_dict, mha.to_dict())

    def test_from_dict_missing_access_steps(self):
        mha_dict = {"role_session_name": "test_role_session_name"}
        with self.assertRaises(ValueError):
            MultiHopAccessor.from_dict(mha_dict)

    def test_from_dict_missing_role_session_name(self):
        mha_dict = {
            "access_steps": [
                {"role_name": "test_role_name1", "external_id": "abcd", "account_id": "1234"},
                {"role_name": "test_role_name2", "external_id": None, "account_id": "5678"},
                {"role_name": "test_role_name3", "external_id": None, "account_id": None},
            ]
        }
        with self.assertRaises(ValueError):
            MultiHopAccessor.from_dict(mha_dict)
