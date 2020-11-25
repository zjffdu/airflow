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
import subprocess
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.file import TemporaryDirectory
import oss2
import os

class OSSHook(BaseHook):

    def __init__(
            self,
            conn_id="oss_default",
            remote_base_folder = "oss://airflow-bucket/airflow/logs"):
        self.conn = self.get_connection(conn_id)
        (bucket, base_folder) = OSSHook.parse_oss_url(remote_base_folder)
        auth = oss2.Auth(os.environ['OSS_ACCESS_KEY_ID'], os.environ['OSS_ACCESS_KEY_SECRET'])
        self.bucket = oss2.Bucket(auth, 'http://oss-cn-hongkong.aliyuncs.com', bucket)
        self.base_folder = base_folder
        self.log.info("Init OSSHook with oss bucket: {}, base folder: {}".format(self.bucket, self.base_folder))

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
        result = self.bucket.put_object(self.base_folder + '/' + key, content)
        print('http status: {0}'.format(result.status))

    def ready_key(self, key):
        object_stream = self.bucket.get_object(self.base_folder + '/' + key)
        content = object_stream.read()
        print(content)
        return content

    def key_exist(self, key):
        return self.bucket.object_exists(key)

