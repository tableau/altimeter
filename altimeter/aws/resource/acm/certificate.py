"""Resource for ACM Certificates"""
from typing import Any, Type, List, Dict

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.core.graph.field.dict_field import DictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField, EmbeddedScalarField
from altimeter.core.graph.schema import Schema
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.acm import ACMResourceSpec


class ACMCertificateResourceSpec(ACMResourceSpec):
    """Resource for ACM Certificates"""

    type_name = "certificate"
    schema = Schema(
        ScalarField("DomainName", optional=True),
        ListField("SubjectAlternativeNames", EmbeddedScalarField(), optional=True),
        ScalarField("Serial", optional=True),
        ScalarField("Subject", optional=True),
        ScalarField("Issuer", optional=True),
        ScalarField("CreatedAt", optional=True),
        ScalarField("IssuedAt", optional=True),
        ScalarField("ImportedAt", optional=True),
        ScalarField("Status", optional=True),
        ScalarField("RevokedAt", optional=True),
        ScalarField("RevocationReason", optional=True),
        ScalarField("NotBefore", optional=True),
        ScalarField("NotAfter", optional=True),
        ScalarField("KeyAlgorithm", optional=True),
        ScalarField("SignatureAlgorithm", optional=True),
        ListField("InUseBy", EmbeddedScalarField(), optional=True),
        ScalarField("FailureReason", optional=True),
        ScalarField("Type", optional=True),
        ListField(
            "DomainValidationOptions",
            EmbeddedDictField(
                ScalarField("DomainName", optional=True),
                ListField("ValidationEmails", EmbeddedScalarField(), optional=True),
                ScalarField("ValidationDomain", optional=True),
                ScalarField("ValidationStatus", optional=True),
                DictField(
                    "ResourceRecord",
                    EmbeddedDictField(
                        ScalarField("Name", optional=True),
                        ScalarField("Type", optional=True),
                        ScalarField("Value", optional=True),
                    ),
                    optional=True,
                ),
                ScalarField("ValidationMethod", optional=True),
            ),
            optional=True,
        ),
        ListField("KeyUsages", EmbeddedDictField(ScalarField("Name")), optional=True),
        ListField(
            "ExtendedKeyUsages",
            EmbeddedDictField(ScalarField("Name"), ScalarField("OID", optional=True)),
            optional=True,
        ),
        ScalarField("RenewalEligibility", optional=True),
    )

    @classmethod
    def list_from_aws(
        cls: Type["ACMCertificateResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        cert_arns: List[str] = []
        paginator = client.get_paginator("list_certificates")

        for resp in paginator.paginate(
            Includes={
                "keyTypes": [
                    "RSA_1024",
                    "RSA_2048",
                    "RSA_3072",
                    "RSA_4096",
                    "EC_prime256v1",
                    "EC_secp384r1",
                    "EC_secp521r1",
                ]
            }
        ):
            cert_arns.extend(
                [
                    cert_dict["CertificateArn"]
                    for cert_dict in resp.get("CertificateSummaryList", [])
                ]
            )
        certs: Dict[str, Dict[str, Any]] = {}

        for cert_arn in cert_arns:
            try:
                cert_data = get_cert_data(client=client, cert_arn=cert_arn)
                certs[cert_arn] = cert_data
            except ClientError as c_e:
                error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                if error_code not in ("ResourceNotFoundException",):
                    raise c_e
        return ListFromAWSResult(resources=certs)


def get_cert_data(client: BaseClient, cert_arn: str) -> Dict:
    """Retrieve detailed properties of a specific ACM cert"""
    cert_resp = client.describe_certificate(CertificateArn=cert_arn)
    return cert_resp["Certificate"]
