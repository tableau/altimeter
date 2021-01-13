import time
from unittest import TestCase

from altimeter.aws.auth.cache import (
    AWSCredentials,
    AWSCredentialsCache,
)

from moto import mock_sts


class TestAWSCredentials(TestCase):
    def test_is_expired_when_expired(self):
        five_min_ago_epoch = int(time.time()) - 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=five_min_ago_epoch,
        )
        self.assertTrue(aws_credentials.is_expired())

    def test_is_expired_when_not_expired(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=in_five_min_epoch,
        )
        self.assertFalse(aws_credentials.is_expired())

    def test_get_session_with_region(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=in_five_min_epoch,
        )
        session = aws_credentials.get_session(region_name="us-east-4")
        self.assertEqual(session.region_name, "us-east-4")

    def test_get_session_without_region(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=in_five_min_epoch,
        )
        session = aws_credentials.get_session()
        self.assertIsNone(session.region_name)

    def test_from_dict(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="testaki",
            secret_access_key="testsak",
            session_token="testst",
            expiration=in_five_min_epoch,
        )
        data = {
            "access_key_id": "testaki",
            "secret_access_key": "testsak",
            "session_token": "testst",
            "expiration": in_five_min_epoch,
        }
        aws_credentials_from_dict = AWSCredentials(**data)
        self.assertEqual(aws_credentials, aws_credentials_from_dict)


class TestBuildAWSCredentialsCacheKey(TestCase):
    def test(self):
        key = AWSCredentialsCache.build_cache_key(
            account_id="1234", role_name="test_role", role_session_name="test_role_session"
        )
        expected_str = "1234:test_role:test_role_session"
        self.assertEqual(str(key), expected_str)


class TestAWSCredentialsCache(TestCase):
    @mock_sts
    def test_put(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache = AWSCredentialsCache()
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        keys_values = cache.cache.items()
        self.assertEqual(len(keys_values), 1)
        for key, value in keys_values:
            self.assertEqual(
                key,
                AWSCredentialsCache.build_cache_key(
                    account_id="123456789012", role_name="test_rn", role_session_name="test_rsn"
                ),
            )
            self.assertEqual(
                value,
                AWSCredentials(
                    access_key_id="test_aki",
                    secret_access_key="test_sak",
                    session_token="test_st",
                    expiration=in_five_min_epoch,
                ),
            )

    @mock_sts
    def test_put_wrong_account_id(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache = AWSCredentialsCache()
        with self.assertRaises(ValueError):
            cache.put(
                credentials=aws_credentials,
                account_id="987654321098",
                role_name="test_rn",
                role_session_name="test_rsn",
            )

    @mock_sts
    def test_get_miss(self):
        cache = AWSCredentialsCache()
        session = cache.get(
            account_id="123456789012", role_name="test_rn", role_session_name="test_rsn"
        )
        self.assertIsNone(session)

    @mock_sts
    def test_get_without_region(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache = AWSCredentialsCache()
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        session = cache.get(
            account_id="123456789012", role_name="test_rn", role_session_name="test_rsn"
        )
        self.assertIsNone(session.region_name)
        creds = session.get_credentials()
        self.assertEqual(creds.access_key, "test_aki")
        self.assertEqual(
            creds.secret_key, "test_sak",
        )
        self.assertEqual(creds.token, "test_st")

    @mock_sts
    def test_get_with_region(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache = AWSCredentialsCache()
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        session = cache.get(
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
            region_name="us-east-4",
        )
        self.assertEqual(session.region_name, "us-east-4")
        creds = session.get_credentials()
        self.assertEqual(creds.access_key, "test_aki")
        self.assertEqual(
            creds.secret_key, "test_sak",
        )
        self.assertEqual(creds.token, "test_st")

    @mock_sts
    def test_get_expired_returns_none(self):
        five_min_ago_epoch = int(time.time()) - 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=five_min_ago_epoch,
        )
        cache = AWSCredentialsCache()
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        session = cache.get(
            account_id="123456789012", role_name="test_rn", role_session_name="test_rsn"
        )
        self.assertIsNone(session)

    @mock_sts
    def test_to_dict(self):
        cache = AWSCredentialsCache()
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        aws_credentials = AWSCredentials(
            access_key_id="test_aki2",
            secret_access_key="test_sak2",
            session_token="test_st2",
            expiration=in_five_min_epoch,
        )
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn2",
            role_session_name="test_rsn2",
        )
        self.assertDictEqual(
            cache.dict(),
            {
                "cache": {
                    "123456789012:test_rn:test_rsn": {
                        "access_key_id": "test_aki",
                        "secret_access_key": "test_sak",
                        "session_token": "test_st",
                        "expiration": in_five_min_epoch,
                    },
                    "123456789012:test_rn2:test_rsn2": {
                        "access_key_id": "test_aki2",
                        "secret_access_key": "test_sak2",
                        "session_token": "test_st2",
                        "expiration": in_five_min_epoch,
                    },
                }
            },
        )

    @mock_sts
    def test_from_dict(self):
        cache = AWSCredentialsCache()
        in_five_min_epoch = int(time.time()) + 5 * 60
        aws_credentials = AWSCredentials(
            access_key_id="test_aki",
            secret_access_key="test_sak",
            session_token="test_st",
            expiration=in_five_min_epoch,
        )
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn",
            role_session_name="test_rsn",
        )
        aws_credentials = AWSCredentials(
            access_key_id="test_aki2",
            secret_access_key="test_sak2",
            session_token="test_st2",
            expiration=in_five_min_epoch,
        )
        cache.put(
            credentials=aws_credentials,
            account_id="123456789012",
            role_name="test_rn2",
            role_session_name="test_rsn2",
        )

        data = {
            "cache": {
                "123456789012:test_rn:test_rsn": {
                    "access_key_id": "test_aki",
                    "secret_access_key": "test_sak",
                    "session_token": "test_st",
                    "expiration": in_five_min_epoch,
                },
                "123456789012:test_rn2:test_rsn2": {
                    "access_key_id": "test_aki2",
                    "secret_access_key": "test_sak2",
                    "session_token": "test_st2",
                    "expiration": in_five_min_epoch,
                },
            }
        }
        from_data_cache = AWSCredentialsCache(**data)
        self.assertEqual(from_data_cache, cache)

    @mock_sts
    def test_from_dict_to_dict(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        data = {
            "cache": {
                "123456789012:test_rn:test_rsn": {
                    "access_key_id": "test_aki",
                    "secret_access_key": "test_sak",
                    "session_token": "test_st",
                    "expiration": in_five_min_epoch,
                },
                "123456789012:test_rn2:test_rsn2": {
                    "access_key_id": "test_aki2",
                    "secret_access_key": "test_sak2",
                    "session_token": "test_st2",
                    "expiration": in_five_min_epoch,
                },
            }
        }
        self.assertDictEqual(data, AWSCredentialsCache(**data).dict())
