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

from notebook.connectors.jdbc import JdbcApi
from notebook.connectors.jdbc import Assist
from notebook.connectors.base import AuthenticationRequired

import logging
LOG = logging.getLogger(__name__)

class JdbcApiClickhouse(JdbcApi):

  def _createAssist(self, db):
    return ClickhouseAssist(db)

  def fetch_result(self, notebook, snippet, rows, start_over):
    return {}

  def autocomplete(self, snippet, database=None, table=None, column=None, nested=None):
    if self.db is None:
      raise AuthenticationRequired()

    assist = self._createAssist(self.db)
    response = {'status': -1}

    if database is None:
      response['databases'] = assist.get_databases()
    elif table is None:
      tables = assist.get_tables_full(database)
      response['tables'] = [table['name'] for table in tables]
      response['tables_meta'] = tables
    else:
      columns = assist.get_columns_full(database, table)
      response['columns'] = [col['name'] for col in columns]
      response['extended_columns'] = columns

    response['status'] = 0
    return response

  def download(self, notebook, snippet, file_format='csv'):
    from beeswax import data_export
    from desktop.lib import export_csvxls
    from beeswax import conf
    from notebook.connectors.base import _get_snippet_name
    import json

    file_name = _get_snippet_name(notebook)
    max_rows = conf.DOWNLOAD_ROW_LIMIT.get()
    result_wrapper = JdbcDataWrapper(self, notebook, snippet, max_rows)

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

class ClickhouseAssist(Assist):

  def get_databases(self):
    dbs, description = query_and_fetch(self.db, 'SHOW DATABASES')
    return [db[0] and db[0].strip() for db in dbs]

  def get_tables_full(self, database, table_names=[]):
    tables, description = query_and_fetch(self.db, "SELECT name, '' FROM system.tables WHERE database='%s'" % database)
    return [{"comment": table[1] and table[1].strip(), "type": "Table", "name": table[0] and table[0].strip()} for table in tables]

  def get_columns_full(self, database, table):
    columns, description = query_and_fetch(self.db, "SELECT name, type, '' FROM system.columns WHERE database='%s' AND table = '%s'" % (database, table))
    return [{"comment": col[2] and col[2].strip(), "type": col[1], "name": col[0] and col[0].strip()} for col in columns]

  def get_sample_data(self, database, table, column=None):
    column = column or '*'
    return query_and_fetch(self.db, 'SELECT %s FROM %s.%s limit 100' % (column, database, table))

class JdbcDataWrapper:

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
      LOG.info("[debug] current sql: {}, ready to query data, max count: {}".format(self.snippet['statement'], self.max_rows))
      datas, description = query_and_fetch(self.api.db, self.snippet['statement'], self.max_rows)
      LOG.info("[debug] current sql: {}, query data finish: {}".format(self.snippet['statement'], len(datas)))
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
