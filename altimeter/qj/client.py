"""Client for the QJ API"""

from datetime import datetime
import json
from typing import List, Optional

import requests

from altimeter.qj import schemas
from altimeter.qj.settings import API_KEY_HEADER_NAME


class QJAPIClientError(Exception):
    """General QJ API Client exception"""


class QJAPIClient:
    """Client for the QJ API"""

    def __init__(
        self, host: str, port: int = 443, scheme: str = "https", api_key: Optional[str] = None
    ) -> None:
        base_url = f"{scheme}://{host}:{port}"
        self._base_url = base_url
        self._base_url_v1 = f"{base_url}/v1"
        self._auth_header = {API_KEY_HEADER_NAME: api_key}

    def get_auth(self) -> str:
        """Get the currently used api auth token. This can be used to validate that the client's
        key is valid"""
        url = f"{self._base_url}/auth"
        try:
            response = requests.get(url, headers=self._auth_header)
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        return response.text

    def get_jobs(self, active_only: bool = True) -> List[schemas.Job]:
        """Get all jobs, by default only active jobs"""
        url = f"{self._base_url_v1}/jobs"
        try:
            response = requests.get(url, params={"active_only": active_only})
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return [schemas.Job(**job_json) for job_json in raw_json]

    def get_job(self, job_name: str) -> Optional[schemas.Job]:
        """Get the active version of a Job by name"""
        url = f"{self._base_url_v1}/jobs/{job_name}"
        try:
            response = requests.get(url)
            if response.status_code == 404:
                return None
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return schemas.Job(**raw_json)

    def create_job(self, job_in: schemas.JobCreate) -> schemas.Job:
        """Create a Job"""
        url = f"{self._base_url_v1}/jobs"
        try:
            response = requests.post(url, json=json.loads(job_in.json()), headers=self._auth_header)
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return schemas.Job(**raw_json)

    def update_job(
        self, job_name: str, job_created: datetime, job_in: schemas.JobUpdate
    ) -> schemas.Job:
        """Update a Job"""
        url = f"{self._base_url_v1}/jobs/{job_name}/versions/{job_created.isoformat()}"
        try:
            response = requests.patch(
                url, json=json.loads(job_in.json()), headers=self._auth_header
            )
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return schemas.Job(**raw_json)

    def _job_job_create_identical(self, job: schemas.Job, job_create: schemas.JobCreate) -> bool:
        """Return True if a given JobCreate would create an identical job to a given Job"""
        create_fields = schemas.JobCreate.__fields__
        for create_field in create_fields:
            if getattr(job, create_field) != getattr(job_create, create_field):
                return False
        return True

    def _job_job_create_nonupdatable_fields_identical(
        self, job: schemas.Job, job_create: schemas.JobCreate
    ) -> bool:
        """Return True if a given JobCreate and Job have identical non-updatable fields"""
        create_keys = set(schemas.JobCreate.__fields__)
        update_keys = set(schemas.JobUpdate.__fields__)
        non_updatable_fields = create_keys - update_keys
        for non_updatable_field in non_updatable_fields:
            if getattr(job, non_updatable_field) != getattr(job_create, non_updatable_field):
                return False
        return True

    def create_or_update_active_job(self, job_in: schemas.JobCreate) -> schemas.Job:
        """This method ensures that an active Job as described by JobCreate exists.
        If the Job does not exist it will be created.  If a Job which can be updated
        exists (e.g. query is the same but perhaps category is different) that job will
        be updated.  The created or updated job will be activated. Note that for
        finding a job candidate to update, only active jobs are considered - an inactive
        job will never be updated and used, instead a new version would be created."""
        job = self.get_job(job_name=job_in.name)
        if job:
            if self._job_job_create_identical(job, job_in):
                return job
            if self._job_job_create_nonupdatable_fields_identical(job, job_in):
                job_update = schemas.JobUpdate.from_job_create(job_in)
                return self.update_job(
                    job_name=job.name, job_created=job.created, job_in=job_update
                )
            job = self.create_job(job_in)
            job_update = schemas.JobUpdate(active=True)
            return self.update_job(job_name=job.name, job_created=job.created, job_in=job_update)
        job = self.create_job(job_in)
        job_update = schemas.JobUpdate(active=True)
        return self.update_job(job_name=job.name, job_created=job.created, job_in=job_update)

    def create_result_set(self, result_set: schemas.ResultSetCreate) -> schemas.ResultSet:
        """Create a ResultSet"""
        url = f"{self._base_url_v1}/result_sets/result_set"
        try:
            response = requests.post(
                url, json=json.loads(result_set.json()), headers=self._auth_header
            )
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return schemas.ResultSet(**raw_json)

    def delete_expired_result_sets(self) -> schemas.ResultSetsPruneResult:
        """Delete all expired ResultSets"""
        url = f"{self._base_url_v1}/result_sets/expired"
        try:
            response = requests.delete(url, headers=self._auth_header)
        except Exception as ex:
            raise QJAPIClientError(f"Error connecting to {url}: {str(ex)}")
        try:
            response.raise_for_status()
        except Exception as ex:
            raise_client_error(response, exception=ex)
        raw_json = response.json()
        return schemas.ResultSetsPruneResult(**raw_json)


def raise_client_error(response: requests.Response, exception: Exception) -> None:
    """Raise an QJAPIClientError based on a Response and Exception"""
    try:
        response_detail = response.json().get("Message", response.text)
    except Exception:
        response_detail = response.text
    raise QJAPIClientError(f"{str(exception)}: {response_detail}")
