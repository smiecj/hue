#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from librdbms.jdbc import query_and_fetch

from notebook.connectors.rdbms import RdbmsApi

import logging
LOG = logging.getLogger(__name__)

class JdbcApiMySQL(RdbmsApi):

  def fetch_result(self, notebook, snippet, rows, start_over):
    return {}

  def download(self, notebook, snippet, file_format='csv'):
    from beeswax import data_export
    from desktop.lib import export_csvxls
    from beeswax import conf
    from notebook.connectors.base import _get_snippet_name
    import json

    file_name = _get_snippet_name(notebook)
    max_rows = conf.DOWNLOAD_ROW_LIMIT.get()
    result_wrapper = JdbcMySQLWrapper(self, notebook, snippet, max_rows)

    generator = export_csvxls.create_generator(result_wrapper, file_format)
    resp = export_csvxls.make_response(generator, file_format, file_name)
    id = snippet['id']
    if id:
      resp.set_cookie(
        'download-%s' % id,
        json.dumps({
          'truncated': False,
          'row_counter': result_wrapper.num_cols
        }),
        max_age=data_export.DOWNLOAD_COOKIE_AGE
      )
    return resp

class JdbcMySQLWrapper:

  def __init__(self, api, notebook, snippet, max_rows=-1):
    self.api = api
    self.notebook = notebook
    self.snippet = snippet
    self.first_fetched = True

    # max_rows current not used
    self.max_rows = max_rows
    self.limit_rows = max_rows > -1

    self.num_cols = 0

  def __iter__(self):
    return self

  def next(self):
    if self.first_fetched:
      datas, description = query_and_fetch(self.api.db, self.snippet['statement'], self.max_rows)
      ret_headers = []
      ret_datas = []

      if datas is not None:
        ret_headers = [col[0] for col in description]
        ret_datas = [data for data in datas]
        self.num_cols = len(datas)

      self.first_fetched = False

      return ret_headers, ret_datas
    else:
      raise StopIteration
