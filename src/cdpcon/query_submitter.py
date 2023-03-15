#
#  Copyright (c) 2022, salesforce.com, inc.
#  All rights reserved.
#  SPDX-License-Identifier: BSD-3-Clause
#  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

from datetime import timedelta
import json
import logging
import requests
import socket
from timeit import default_timer as timer

from .constants import API_VERSION_V2, QUERY_HEADER_VALUE_TEXT_CSV
from .constants import QUERY_HEADER_KEY_AUTHORIZATION
from .constants import QUERY_HEADER_KEY_CONTENT_TYPE
from .constants import QUERY_HEADER_VALUE_APPLICATION_JSON
from .constants import QUERY_HEADER_KEY_ACCEPT_ENCODING
from .constants import QUERY_HEADER_VALUE_GZIP
from .exceptions import Error


def allowed_gai_family():
    return socket.AF_INET


requests.packages.urllib3.util.connection.allowed_gai_family = allowed_gai_family


class QuerySubmitter:
    """
    Helper methods to execute query against V2 API
    """

    logger = logging.getLogger()
    session = requests.session()

    def execute(connection, query, api_version=API_VERSION_V2, enable_arrow_stream=False):
        """
        This method submits the query to queryV2 API for execution
        :param connection: SalesforceCDPConnection
        :param query: The query to be executed
        :param api_version: v1 or v2 API
        :param enable_arrow_stream: Set as True to fetch the results as ArrowStream
        :return: Returns the response JSON
        """
        token, instance_url = connection.authentication_helper.get_token()
        return QuerySubmitter._get_query_results(query, instance_url, token, api_version, enable_arrow_stream)

    def get_next_batch(connection, next_batch_id, enable_arrow_stream=False):
        """
        This method fetches the next batch of results using the v2 APIs.
        :param connection:  SalesforceCDPConnection
        :param next_batch_id: batchId to fetch the results
        :param enable_arrow_stream: Set as True to fetch the results as ArrowStream
        :return:
        """
        token, instance_url = connection.authentication_helper.get_token()
        return QuerySubmitter._get_next_batch_results(next_batch_id, instance_url, token, enable_arrow_stream)

    def _get_query_results(query, instance_url, token, api_version='V2', enable_arrow_stream=False):
        url = f'https://{instance_url}/api/{api_version}/query'
        json_payload = QuerySubmitter._get_payload(query)
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        QuerySubmitter.logger.debug("Submitting query for execution")
        start_time = timer()
        sql_response = QuerySubmitter.session.post(url=url, data=json_payload, headers=headers, verify=True)
        QuerySubmitter.logger.debug("Query Submitted in %s", str(timedelta(seconds=timer() - start_time)))
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing query in server : %s' % error_message)
                raise Error('Failed executing query in server')
        response_json = sql_response.json()
        return response_json

    def _get_next_batch_results(next_batch_id, instance_url, token, enable_arrow_stream=False):
        url = f'https://{instance_url}/api/v2/query/{next_batch_id}'
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        start_time = timer()
        sql_response = QuerySubmitter.session.get(url=url, headers=headers, verify=False)
        QuerySubmitter.logger.debug("Fetched next batch in %s", str(timedelta(seconds=timer() - start_time)))
        response_json = sql_response.json()
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing query in server : %s' % error_message)
                raise Error('Failed executing query in server')
        return response_json

    def _get_headers(token, enable_arrow_stream):
        headers = {QUERY_HEADER_KEY_AUTHORIZATION: f'Bearer {token}',
                   QUERY_HEADER_KEY_CONTENT_TYPE: QUERY_HEADER_VALUE_APPLICATION_JSON,
                   QUERY_HEADER_KEY_ACCEPT_ENCODING: QUERY_HEADER_VALUE_GZIP}
        if enable_arrow_stream:
            headers['enable-arrow-stream'] = 'true'
        return headers

    def _get_headers_csv(token, enable_arrow_stream):
        headers = {QUERY_HEADER_KEY_AUTHORIZATION: f'Bearer {token}',
                   QUERY_HEADER_KEY_CONTENT_TYPE: QUERY_HEADER_VALUE_TEXT_CSV,
                   QUERY_HEADER_KEY_ACCEPT_ENCODING: QUERY_HEADER_VALUE_GZIP}
        if enable_arrow_stream:
            headers['enable-arrow-stream'] = 'true'
        return headers

    def _get_payload(query):
        payload = {
            'sql': query
        }
        json_payload = json.dumps(payload)
        return json_payload

    def _post_ingest_stream(instance_url, token, payload, sources, object, api_version='V2', enable_arrow_stream=False):

        url = f'https://{instance_url}/api/v1/ingest/sources/{sources}/{object}'

        json_payload = json.dumps(payload)

        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        QuerySubmitter.logger.debug("Submitting _post_ingest_stream for execution")
        start_time = timer()
        sql_response = QuerySubmitter.session.post(url=url, data=json_payload, headers=headers, verify=False)
        QuerySubmitter.logger.debug("_post_ingest_stream Submitted in %s", str(timedelta(seconds=timer() - start_time)))

        if sql_response.status_code != 202:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing _post_ingest_stream in server : %s' % error_message)
                raise Error('Failed executing _post_ingest_stream in server')
        response_json = sql_response.json()
        return response_json

    def _post_ingest_bulk_job_start(instance_url, token, objinfo, api_version='V2', enable_arrow_stream=False):

        url = instance_url + '/api/v1/ingest/jobs'
        
        json_payload = json.dumps(objinfo)

        start_time = timer()        
        
        headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
                
        sql_response = QuerySubmitter.session.post(url=url, data=json_payload, headers=headers, verify=False)
        QuerySubmitter.logger.debug("Job started in %s", str(timedelta(seconds=timer() - start_time)))
        '''
        {'object': 'athlete_profiles', 'id': 'cd186a64-e5ad-4af3-8ba2-7c59ddaa5fcb', 'operation': 'upsert', 'sourceName': 'athlete', 'createdById': '005R0000000J2cCIAS', 'createdDate': '2022-10-16T01:12:17.589480Z', 'systemModstamp': '2022-10-16T02:01:08.793Z', 'state': 'JobComplete', 'contentType': 'CSV', 'apiVersion': 'v1', 'contentUrl': '/api/v1/ingest/jobs/cd186a64-e5ad-4af3-8ba2-7c59ddaa5fcb/batches', 'retries': 0, 'totalProcessingTime': 51}
        '''
        error_message = None
        if sql_response.status_code != 201:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing _post_ingest_bulk_job_start in server : %s' % error_message)
                raise Error('Failed executing _post_ingest_bulk_job_start in server')
        response_json = sql_response.json()
        return response_json


    def _post_ingest_bulk_job_upload_data(instance_url, token, jobid, data, api_version='V2', enable_arrow_stream=False):
        url = instance_url + '/api/v1/ingest/jobs/' + jobid + '/batches'
        start_time = timer()        
        
        data_payload = data.encode('ascii')           

        headers=headers = QuerySubmitter._get_headers_csv(token, enable_arrow_stream)
        sql_response = QuerySubmitter.session.put(url=url, data=data_payload, headers=headers, verify=True)
        QuerySubmitter.logger.debug("Job data poosted %s", str(timedelta(seconds=timer() - start_time)))

        error_message = None
        if sql_response.status_code != 202:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing post in server : %s' % error_message)
                raise Error('Failed executing post in server')                
        return sql_response.json()

    def _patch_ingest_bulk_job_update_status(instance_url, token, jobid, data, api_version='V2', enable_arrow_stream=False, state='UploadComplete'):
        url = instance_url + '/api/v1/ingest/jobs/' + jobid

        start_time = timer()        
        headers=headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        data = {
            "state" : state
        }
        data_payload = json.dumps(data)
        sql_response = QuerySubmitter.session.patch(url=url, data=data_payload, headers=headers, verify=True)
        QuerySubmitter.logger.debug("Job data posted %s", str(timedelta(seconds=timer() - start_time)))

        error_message = None
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing _patch_ingest_bulk_job_update_status in server : %s' % error_message)
                raise Error('Failed executing _patch_ingest_bulk_job_update_status in server')                
        return sql_response.json()

    def _get_ingest_bulk_jobs(instance_url, token, api_version='V2', enable_arrow_stream=False):
        url = instance_url + '/api/v1/ingest/jobs/'
        start_time = timer()        
        headers=headers = QuerySubmitter._get_headers(token, enable_arrow_stream)
        sql_response = QuerySubmitter.session.get(url=url, data="", headers=headers, verify=True)
        QuerySubmitter.logger.debug("_get_ingest_bulk_jobs data posted %s", str(timedelta(seconds=timer() - start_time)))
        error_message = None
        if sql_response.status_code != 200:
            try:
                error_json = sql_response.json()
                error_message = error_json['message']
            finally:
                if error_message is not None:
                    raise Error('Failed executing _get_ingest_bulk_jobs in server : %s' % error_message)
                raise Error('Failed executing _get_ingest_bulk_jobs in server')           
        return sql_response.json()

