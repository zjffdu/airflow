# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# pylint: disable=invalid-name


from __future__ import print_function
from urllib.parse import urlparse
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
import oss2


class OSSHook(BaseHook):

    def __init__(self,conn_id, remote_base_folder):
        self.conn = self.get_connection(conn_id)
        (bucket_name, self.base_folder) = OSSHook.parse_oss_url(remote_base_folder)
        self.log.info("Init OSSHook with oss bucket: {}, base folder: {}".format(bucket_name, self.base_folder))
        extra_config = self.conn.extra_dejson
        self.auth_type = extra_config.get('auth_type', None)
        if not self.auth_type:
            raise Exception("No auth_type specified in extra_config, either 'AK' or 'STS'")

        if self.auth_type == 'AK':
            oss_access_key_id = extra_config.get('access_key_id', None)
            oss_access_key_secret = extra_config.get('access_key_secret', None)
            oss_region =  extra_config.get('region', None)
            if not oss_access_key_id:
                raise Exception("No access_key_id is specified for connection: " + conn_id)
            if not oss_access_key_secret:
                raise Exception("No access_key_secret is specified for connection: " + conn_id)
            if not oss_region:
                raise Exception("No region is specified for connection: " + conn_id)

            auth = oss2.Auth(oss_access_key_id, oss_access_key_secret)
            self.bucket = oss2.Bucket(auth, 'http://oss-' + oss_region + '.aliyuncs.com', bucket_name)
        elif self.auth_type == 'STS':
            self._refresh_sts()
            auth = oss2.StsAuth(self._sts_access_key_id, self._sts_access_key_secret, self._sts_security_token)
            self.bucket = oss2.Bucket(auth, 'http://oss-' + self._region + '.aliyuncs.com', bucket_name)
        else:
            raise Exception("Unsupported auth_type: " + self.auth_type)

    @staticmethod
    def parse_oss_url(ossurl):
        parsed_url = urlparse(ossurl)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name instead of "%s"' % ossurl)
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip('/')
            return bucket_name, key

    def load_string(self, content, key, replace):
        try:
            self.log.info("Write oss key: " + key)
            self.bucket.put_object(self.base_folder + '/' + key, content)
        except Exception as e:
            if 'InvalidSecurityToken.Expired' in str(e):
                self._refresh_sts()
                self.bucket.put_object(self.base_folder + '/' + key, content)
            else:
                raise e

    def read_key(self, key):
        try:
            self.log.info("Read oss key: " + key)
            return self.bucket.get_object(self.base_folder + '/' + key).read().decode("utf-8")
        except Exception as e:
            if 'InvalidSecurityToken.Expired' in str(e):
                self._refresh_sts()
                return self.bucket.get_object(self.base_folder + '/' + key).read()
            else:
                raise e

    def key_exist(self, key):
        full_path = None
        try:
            full_path = self.base_folder + '/' + key
            self.log.info("Check oss key: " + full_path)
            return self.bucket.object_exists(full_path)
        except Exception as e:
            if 'InvalidSecurityToken.Expired' in str(e):
                self._refresh_sts()
                return self.bucket.object_exists(full_path)
            else:
                raise e

    def _refresh_sts(self):
        import requests
        resp = requests.get('http://100.100.100.200/latest/meta-data/Ram/security-credentials/AliyunECSInstanceForEMRRole').json()
        self._sts_access_key_id = resp['AccessKeyId']
        self._sts_access_key_secret = resp['AccessKeySecret']
        self._sts_security_token = resp['SecurityToken']
        self._region = requests.get('http://100.100.100.200//latest/meta-data/region-id').text
        self.log.info("STS token is refreshed")
