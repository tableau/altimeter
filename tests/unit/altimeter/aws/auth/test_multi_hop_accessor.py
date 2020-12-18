from unittest import TestCase

from altimeter.aws.auth.multi_hop_accessor import AccessStep, MultiHopAccessor
from altimeter.aws.auth.cache import AWSCredentialsCache

from moto import mock_sts


class TestAccessStep(TestCase):
    def test_to_str_all_attrs(self):
        access_step = AccessStep(
            role_name="test_rn", account_id="123456789012", external_id="test_ext_id"
        )
        self.assertEqual(str(access_step), "test_rn@123456789012")

    def test_to_str_no_account_id(self):
        access_step = AccessStep(role_name="test_rn", external_id="test_ext_id")
        self.assertEqual(str(access_step), "test_rn@target")

    def test_from_dict_all_attrs(self):
        data = {"role_name": "test_rn", "account_id": "123456789012", "external_id": "test_ext_id"}
        access_step = AccessStep(**data)
        self.assertEqual(
            access_step,
            AccessStep(role_name="test_rn", account_id="123456789012", external_id="test_ext_id"),
        )

    def test_from_dict_role_name_only(self):
        data = {"role_name": "test_rn"}
        access_step = AccessStep(**data)
        self.assertEqual(
            access_step, AccessStep(role_name="test_rn"),
        )


class TestMultiHopAccessor(TestCase):
    def test_init_successful(self):
        MultiHopAccessor(
            role_session_name="foo",
            access_steps=[
                AccessStep(role_name="foo", account_id="123456789012"),
                AccessStep(role_name="boo"),
            ],
        )

    def test_init_no_access_steps(self):
        with self.assertRaises(ValueError):
            MultiHopAccessor(role_session_name="foo", access_steps=[])

    def test_init_nonfinal_access_step_missing_account_id(self):
        with self.assertRaises(ValueError):
            MultiHopAccessor(
                role_session_name="foo",
                access_steps=[AccessStep(role_name="foo"), AccessStep(role_name="boo")],
            )

    def test_init_final_access_step_having_account_id(self):
        with self.assertRaises(ValueError):
            MultiHopAccessor(
                role_session_name="foo",
                access_steps=[
                    AccessStep(role_name="foo", account_id="123456789012"),
                    AccessStep(role_name="boo", account_id="123456789012"),
                ],
            )

    @mock_sts
    def test_get_session_without_cache(self):
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="123456789012", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="123456789012"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        session = mha.get_session("123456789012")
        self.assertIsNone(session.region_name)

    @mock_sts
    def test_get_session_with_cache(self):
        cache = AWSCredentialsCache()
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="123456789012", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="123456789012"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        session = mha.get_session("123456789012", credentials_cache=cache)
        self.assertIsNone(session.region_name)
        self.assertEqual(
            sorted(cache.dict()["cache"].keys()),
            [
                "123456789012:test_role_name1:test_role_session_name",
                "123456789012:test_role_name2:test_role_session_name",
                "123456789012:test_role_name3:test_role_session_name",
            ],
        )

    @mock_sts
    def test_get_session_with_primed_cache(self):
        cache = AWSCredentialsCache()
        access_steps = [
            AccessStep(role_name="test_role_name1", account_id="123456789012", external_id="abcd"),
            AccessStep(role_name="test_role_name2", account_id="123456789012"),
            AccessStep(role_name="test_role_name3"),
        ]
        mha = MultiHopAccessor(
            role_session_name="test_role_session_name", access_steps=access_steps
        )
        session = mha.get_session("123456789012", credentials_cache=cache)
        frozen_creds = session.get_credentials().get_frozen_credentials()
        # cache is now primed, the next call should use cached creds. record them so we can
        # compare.
        new_session = mha.get_session("123456789012", credentials_cache=cache)
        self.assertEqual(frozen_creds, new_session.get_credentials().get_frozen_credentials())

    def test_to_str(self):
        mha = MultiHopAccessor(
            role_session_name="foo",
            access_steps=[
                AccessStep(role_name="foo", account_id="123456789012"),
                AccessStep(role_name="boo"),
            ],
        )
        self.assertEqual(str(mha), "accessor:foo:foo@123456789012,boo@target")

    def test_to_dict(self):
        mha = MultiHopAccessor(
            role_session_name="foo",
            access_steps=[
                AccessStep(role_name="foo", account_id="123456789012"),
                AccessStep(role_name="boo"),
            ],
        )
        self.assertDictEqual(
            mha.dict(),
            {
                "role_session_name": "foo",
                "access_steps": [
                    {"role_name": "foo", "account_id": "123456789012", "external_id": None},
                    {"role_name": "boo", "account_id": None, "external_id": None},
                ],
            },
        )

    def test_from_dict(self):
        mha = MultiHopAccessor(
            role_session_name="foo",
            access_steps=[
                AccessStep(role_name="foo", account_id="123456789012"),
                AccessStep(role_name="boo"),
            ],
        )
        data = {
            "role_session_name": "foo",
            "access_steps": [
                {"role_name": "foo", "account_id": "123456789012", "external_id": None},
                {"role_name": "boo", "account_id": None, "external_id": None},
            ],
        }
        from_dict_mha = MultiHopAccessor(**data)
        self.assertEqual(mha, from_dict_mha)

    def test_from_dict_to_dict(self):
        data = {
            "role_session_name": "foo",
            "access_steps": [
                {"role_name": "foo", "account_id": "123456789012", "external_id": None},
                {"role_name": "boo", "account_id": None, "external_id": None},
            ],
        }
        self.assertDictEqual(data, MultiHopAccessor(**data).dict())

    def test_from_dict_missing_role_session_name(self):
        data = {
            "access_steps": [
                {"role_name": "foo", "account_id": "123456789012", "external_id": None},
                {"role_name": "boo", "account_id": None, "external_id": None},
            ],
        }
        with self.assertRaises(ValueError):
            MultiHopAccessor(**data)
