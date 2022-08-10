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

'''
SQL Alchemy offers native connections to databases via dialects https://docs.sqlalchemy.org/en/latest/dialects/.

When the dialect of a paricular datavase is installed on the Hue API server, any of its URL connection strings should work.

e.g.
mysql://root:root@localhost:3306/hue

To offer more self service capabilities, parts of the URL can be parameterized.

Supported parameters are:

* USER
* PASSWORD

e.g.
mysql://${USER}:${PASSWORD}@localhost:3306/hue

Parameters are not saved at any time in the Hue database. The are currently not even cached in the Hue process. The clients serves these parameters
each time a query is sent.

Note: the SQL Alchemy engine could leverage create_session() and cache the engine object (without its credentials) like in the jdbc.py interpreter.
Note: this is currently supporting concurrent querying by one users as engine is a new object each time. Could use a thread global SQL Alchemy
session at some point.
Note: using the task server would not leverage any caching.
'''

import datetime
import json
import logging
import uuid
import sys
import re
import textwrap
import prestodb

from string import Template

from django.utils.translation import ugettext as _
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError

from desktop.lib import export_csvxls
from desktop.lib.i18n import force_unicode
from beeswax import data_export
from librdbms.server import dbms

from notebook.connectors.base import Api, QueryError, QueryExpired, _get_snippet_name, AuthenticationRequired
from notebook.models import escape_rows


ENGINES = {}
CONNECTIONS = {}
ENGINE_KEY = '%(username)s-%(connector_name)s'
URL_PATTERN = '(?P<driver_name>.+?://)(?P<host>[^:/ ]+):(?P<port>[0-9]*).*'

CONNECTION_CACHE = {}
LOG = logging.getLogger(__name__)


def query_error_handler(func):
  def decorator(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except OperationalError, e:
      message = str(e)
      if '1045' in message: # 'Access denied' # MySQL
        raise AuthenticationRequired(message=message)
      else:
        raise e
    except Exception, e:
      message = force_unicode(e)
      if 'Invalid query handle' in message or 'Invalid OperationHandle' in message:
        raise QueryExpired(e)
      else:
        LOG.exception('Query Error')
        raise QueryError(message)
  return decorator


class SqlAlchemyApi(Api):

  def __init__(self, user, interpreter, request):
    super(SqlAlchemyApi, self).__init__(user=user, interpreter=interpreter, request=request)
    #self.user = user
    self.options = interpreter['options']

  def _get_engine_key(self):
    return ENGINE_KEY % {
      'username': self.user.username,
      'connector_name': self.interpreter['name']
    }
  
  def _get_engine(self):
    engine_key = self._get_engine_key()

    if engine_key not in ENGINES:
      ENGINES[engine_key] = self._create_engine()

    return ENGINES[engine_key]

  def _create_engine_s(self):
    if '${' in self.options['url']: # URL parameters substitution
      vars = {'user': self.user.username}
      for _prop in self.options['session']['properties']:
        if _prop['name'] == 'user':
          vars['USER'] = _prop['value']
        if _prop['name'] == 'password':
          vars['PASSWORD'] = _prop['value']
      raw_url = Template(self.options['url'])
      url = raw_url.safe_substitute(**vars)
    else:
      url = self.options['url']
    return create_engine(url)

  def _create_engine(self):
    if '${' in self.options['url']:  # URL parameters substitution
      vars = {'USER': self.user.username}

      if '${PASSWORD}' in self.options['url']:
        auth_provided = False
        if 'session' in self.options:
          for _prop in self.options['session']['properties']:
            if _prop['name'] == 'user':
              vars['USER'] = _prop['value']
              auth_provided = True
            if _prop['name'] == 'password':
              vars['PASSWORD'] = _prop['value']
              auth_provided = True

        if not auth_provided:
          raise AuthenticationRequired(message='Missing username and/or password')

      raw_url = Template(self.options['url'])
      url = raw_url.safe_substitute(**vars)
      LOG.warn('****url={}****'.format(url))
    else:
      url = self.options['url']

    if url.startswith('awsathena+rest://'):
      url = url.replace(url[17:37], urllib_quote_plus(url[17:37]))
      url = url.replace(url[38:50], urllib_quote_plus(url[38:50]))
      s3_staging_dir = url.rsplit('s3_staging_dir=', 1)[1]
      url = url.replace(s3_staging_dir, urllib_quote_plus(s3_staging_dir))

    if self.options.get('has_impersonation'):
      m = re.search(URL_PATTERN, url)
      driver_name = m.group('driver_name')

      if not driver_name:
        raise QueryError('Driver name of %(url)s could not be found and impersonation is turned on' % {'url': url})

      url = url.replace(driver_name, '%(driver_name)s%(username)s@' % {
        'driver_name': driver_name,
        'username': self.user.username
      })

    if self.options.get('credentials_json'):
      self.options['credentials_info'] = json.loads(
          self.options.pop('credentials_json')
      )

    # Enables various SqlAlchemy args to be passed along for both Hive & Presto connectors
    # Refer to SqlAlchemy pyhive for more details
    if self.options.get('connect_args'):
      self.options['connect_args'] = json.loads(
          self.options.pop('connect_args')
      )

    options = self.options.copy()
    options.pop('session', None)
    options.pop('url', None)
    options.pop('has_ssh', None)
    options.pop('has_impersonation', None)
    options.pop('ssh_server_host', None)

    options['pool_pre_ping'] = not url.startswith('phoenix://')  # Should be moved to dialect when connectors always on
    
    LOG.warn('****url={}****'.format(url))
    #return create_engine(url, **options)
    return create_engine(url)
  
  def _get_session(self, notebook, snippet):
    for session in notebook['sessions']:
      if session['type'] == snippet['type']:
        return session

    return None

  def _create_connection_old(self, engine):
    connection = None
    try:
      connection = engine.connect()
    except Exception as e:
      engine_key = self._get_engine_key()
      ENGINES.pop(engine_key, None)

      raise AuthenticationRequired(message='Could not establish connection to datasource: %s' % e)

    return connection


  @query_error_handler
  def get_log_is_full_log(self, notebook, snippet):
    return True

  @query_error_handler
  def can_start_over(self, notebook, snippet):
    return False

  @query_error_handler
  def execute(self, notebook, snippet):
    import time
    start_time = time.time()
    guid = uuid.uuid4().hex

    '''
    session = self._get_session(notebook, snippet)
    if session is not None:
      self.options['session'] = session

    engine = self._get_engine()
    connection = self._create_connection(engine)
    current_statement = self._get_current_statement(notebook, snippet)
    '''
    connection = self._get_connection()
    cur = connection.cursor()
    end_time = time.time()
    LOG.warn('****create connection,cost time:{:.2f}s****'.format(end_time - start_time))
    #import json
    #LOG.warn('****snippet:{}'.format(json.dumps(snippet)))
    statement=snippet['statement']
    if snippet['type'] == "presto":
       statement=statement.replace("stg_stream", "kudu.stg_stream")
       statement=statement.rstrip(";")
    end_time = time.time()
    LOG.warn('****statement modify,cost time:{:.2f}s****'.format(end_time - start_time))
    #result = connection.execution_options(stream_results=True).execute(statement)
    #result = connection.execute(statement)
    cur.execute(statement)
    first_one = cur.fetchone()

    end_time = time.time()
    LOG.warn('****execute query,cost time:{:.2f}s****'.format(end_time - start_time))
    cache = {
      'connection': connection,
      'result': cur,
      'first_one':first_one,
      'meta': [{
          'name': col[0] if (type(col) is tuple or type(col) is dict) else col.name if hasattr(col, 'name') else col,
          'type': 'STRING_TYPE',
          'comment': ''
        } for col in cur.description]
    }
    CONNECTION_CACHE[guid] = cache

    response = {
      'sync': False,
      'has_result_set': True,
      'modified_row_count': 0,
      'guid': guid,
      'result': {
        'has_more': True,
        'data': [],
        'meta': cache['meta'],
        'type': 'table'
      }
    }
    LOG.info("[debug] clickhouse fetch response: {}".format(response))
    return response

  def _get_connection(self):
    engine_key = self._get_engine_key()

    if engine_key not in ENGINES:
      ENGINES[engine_key] = self._create_connection()

    return ENGINES[engine_key]

  def _create_connection(self):
    connection = None
    try:
      host = self.options['host']
      port = self.options['port']
      catalog = self.options['catalog']
      schema = self.options['schema']
      auth_username = self.options['auth_username']
      auth_password = self.options['auth_password']
      if self.options.get('has_impersonation'):
        user = self.user.username
      connection=prestodb.dbapi.connect(
          host=host,
          port=port,
          user=self.user.username,
          catalog=catalog,
          schema=schema,
	  auth=prestodb.auth.BasicAuthentication(auth_username, auth_password),
      )
    except Exception as e:
      engine_key = self._get_engine_key()
      ENGINES.pop(engine_key, None)

      raise AuthenticationRequired(message='Could not establish connection to datasource: %s' % e)

    return connection

  @query_error_handler
  def check_status(self, notebook, snippet):
    guid = snippet['result']['handle']['guid']
    connection = CONNECTION_CACHE.get(guid)

    if connection:
      return {'status': 'available'}
    else:
      return {'status': 'canceled'}

  def fetch_result_d(self, notebook, snippet, rows, start_over,is_download):
    guid = snippet['result']['handle']['guid']
    cache = CONNECTION_CACHE.get(guid)
    #import json
    #LOG.warn("******sqlalchemy fetch_result,notebook={},snippet={},rows={},start_over={}******".format(json.dumps(notebook),json.dumps(snippet),rows,start_over))
    if cache:
      if start_over or is_download:
	rows = rows - 1
        data = list(cache['result'].fetchmany(rows))
	first_one = cache['first_one']
        if first_one:
          data.insert(0,first_one)
      else:
        data = cache['result'].fetchmany(rows)
        #data = cache['result']
      meta = cache['meta']
      self._assign_types(data, meta)
    else:
      data = []
      meta = []

    response = {
      'has_more': data and len(data) >= rows,
      'data': data if data else [],
      'meta': meta if meta else [],
      'type': 'table'
    }
    LOG.info("[debug] clickhouse fetch_result_d result response: {}".format(response))
    return response

  @query_error_handler
  def fetch_result(self, notebook, snippet, rows, start_over):
    return self.fetch_result_d(notebook, snippet, rows, start_over,is_download=False)

  @query_error_handler
  def download(self, notebook, snippet, format):
    from beeswax import data_export 
    from beeswax import conf
    try:
      result_wrapper = ExecutionWrapper(self, notebook, snippet)
      file_name = _get_snippet_name(notebook)

      #max_rows = conf.DOWNLOAD_ROW_LIMIT.get()
      #content_generator = HS2DataAdapter(None, result_wrapper, max_rows=max_rows, start_over=True)

      return data_export.download(None, format, result_wrapper, id=snippet['id'], file_name=file_name)
    except Exception, e:
      title = 'The query result cannot be downloaded.'
      LOG.exception(title)

      if hasattr(e, 'message') and e.message:
        if 'generic failure: Unable to find a callback: 32775' in e.message:
          message = e.message + " " + _("Increase the sasl_max_buffer value in hue.ini")
        elif 'query result cache exceeded its limit' in e.message:
          message = e.message.replace("Restarting the fetch is not possible.", _("Please execute the query again."))
        else:
          message = e.message
      else:
        message = e
      raise PopupException(_(title), detail=message)

  @query_error_handler
  def export_data_as_hdfs_file(self, snippet, target_file, overwrite):
    from beeswax.data_export import upload
    from beeswax import conf 
    #db = self._get_db(snippet)
    #handle = self._get_handle(snippet)

    result_wrapper = ExecutionWrapper(self, None, snippet)
    #max_rows = DOWNLOAD_ROW_LIMIT.get()
    max_rows = conf.DOWNLOAD_ROW_LIMIT.get()

    upload(target_file, None, self.request.user, result_wrapper, self.request.fs, max_rows=max_rows)

    return '/filebrowser/view=%s' % target_file

  def _assign_types(self, results, meta):
    result = results and results[0]
    if result:
      for index, col in enumerate(result):
        if isinstance(col, int):
          meta[index]['type'] = 'INT_TYPE'
        elif isinstance(col, float):
          meta[index]['type'] = 'FLOAT_TYPE'
        elif isinstance(col, long):
          meta[index]['type'] = 'BIGINT_TYPE'
        elif isinstance(col, bool):
          meta[index]['type'] = 'BOOLEAN_TYPE'
        elif isinstance(col, datetime.date):
          meta[index]['type'] = 'TIMESTAMP_TYPE'
        else:
          meta[index]['type'] = 'STRING_TYPE'

  @query_error_handler
  def fetch_result_metadata(self):
    pass


  @query_error_handler
  def cancel(self, notebook, snippet):
    result = {'status': -1}
    try:
      guid = snippet['result']['handle']['guid']
      connection = CONNECTION_CACHE.get(guid)
      if connection:
        connection['connection'].close()
        del CONNECTION_CACHE[guid]
      result['status'] = 0
    finally:
      return result


  @query_error_handler
  def get_log(self, notebook, snippet, startFrom=None, size=None):
    return ''


  @query_error_handler
  def close_statement(self,  snippet):
    result = {'status': -1}

    try:
      guid = snippet['result']['handle']['guid']
      connection = CONNECTION_CACHE.get('guid')
      if connection:
        connection['connection'].close()
        del CONNECTION_CACHE[guid]
      result['status'] = 0
    finally:
      return result


  @query_error_handler
  def autocomplete(self, snippet, database=None, table=None, column=None, nested=None):
    engine = self._create_engine()
    inspector = inspect(engine)

    assist = Assist(inspector, engine)
    response = {'status': -1}

    if database is None:
      response['databases'] = assist.get_databases()
    elif table is None:
      tables_meta = []
      for t in assist.get_tables(database):
        tables_meta.append({'name': t, 'type': 'Table', 'comment': ''})
      response['tables_meta'] = tables_meta
    elif column is None:
      columns = assist.get_columns(database, table)
      response['columns'] = [col['name'] for col in columns]
      response['extended_columns'] = [
        {
          'autoincrement': col.get('autoincrement'),
          'comment': col.get('comment'),
          'default': col.get('default'),
          'name': col.get('name'),
          'nullable': col.get('nullable'),
          'type': str(col.get('type'))
        } for col in columns
      ]
    else:
      columns = assist.get_columns(database, table)
      response['name'] = next((col['name'] for col in columns if column == col['name']), '')
      response['type'] = next((col['type'] for col in columns if column == col['name']), '')

    response['status'] = 0
    return response


  @query_error_handler
  def get_sample_data(self, snippet, database=None, table=None, column=None, async=False, operation=None):
    engine = self._create_engine()
    inspector = inspect(engine)

    assist = Assist(inspector, engine)
    response = {'status': -1, 'result': {}}

    metadata, sample_data = assist.get_sample_data(database, table, column)
    has_result_set = sample_data is not None

    if sample_data:
      response['status'] = 0
      response['rows'] = escape_rows(sample_data)

    if table:
      columns = assist.get_columns(database, table)
      response['full_headers'] = [{
        'name': col.get('name'),
        'type': str(col.get('type')),
        'comment': ''
      } for col in columns]
    elif metadata:
      response['full_headers'] = [{
        'name': col[0] if type(col) is dict or type(col) is tuple else col,
        'type': 'STRING_TYPE',
        'comment': ''
      } for col in metadata]

    return response

  @query_error_handler
  def get_browse_query(self, snippet, database, table, partition_spec=None):
    return "SELECT * FROM `%s`.`%s` LIMIT 1000" % (database, table)


class Assist():

  def __init__(self, db, engine):
    self.db = db
    self.engine = engine

  def get_databases(self):
    return self.db.get_schema_names()

  def get_tables(self, database, table_names=[]):
    return self.db.get_table_names(database)

  def get_columns(self, database, table):
    return self.db.get_columns(table, database)

  def get_sample_data(self, database, table, column=None):
    column = '`%s`' % column if column else '*'
    statement = "SELECT %s FROM `%s`.`%s` LIMIT %d" % (column, database, table, 100)
    connection = self.engine.connect()
    try:
      result = connection.execution_options(stream_results=True).execute(statement)
      return result.cursor.description, result.fetchall()
    finally:
      connection.close()

class ExecutionWrapper():
  def __init__(self, api, notebook, snippet, callback=None):
    self.api = api
    self.notebook = notebook
    self.snippet = snippet
    self.callback = callback
    self.should_close = False

  def fetch(self, handle, start_over=None, rows=None):
    if start_over:
      if not self.snippet['result'].get('handle') or not self.snippet['result']['handle'].get('guid') or not self.api.can_start_over(self.notebook, self.snippet):
        start_over = False
        handle = self.api.execute(self.notebook, self.snippet)
        self.snippet['result']['handle'] = handle
        if self.callback and hasattr(self.callback, 'on_execute'):
          self.callback.on_execute(handle)
        self.should_close = True
        self._until_available()
    if self.snippet['result']['handle'].get('sync', False):
      result = self.snippet['result']['handle']['result']
    else:
      result = self.api.fetch_result_d(self.notebook, self.snippet, rows, start_over,is_download=True)
    LOG.info("[debug] clickhouse fetch result: {}".format(result))
    return ResultWrapper(result.get('meta'), result.get('data'), result.get('has_more'))

  def _until_available(self):
    if self.snippet['result']['handle'].get('sync', False):
      return # Request is already completed
    count = 0
    sleep_seconds = 1
    check_status_count = 0
    get_log_is_full_log = self.api.get_log_is_full_log(self.notebook, self.snippet)
    while True:
      response = self.api.check_status(self.notebook, self.snippet)
      if self.callback and hasattr(self.callback, 'on_status'):
        self.callback.on_status(response['status'])
      if self.callback and hasattr(self.callback, 'on_log'):
        log = self.api.get_log(self.notebook, self.snippet, startFrom=count)
        if get_log_is_full_log:
          log = log[count:]

        self.callback.on_log(log)
        count += len(log)

      if response['status'] not in ['waiting', 'running', 'submitted']:
        break
      check_status_count += 1
      if check_status_count > 5:
        sleep_seconds = 5
      elif check_status_count > 10:
        sleep_seconds = 10
      time.sleep(sleep_seconds)

  def close(self, handle):
    if self.should_close:
      self.should_close = False
      self.api.close_statement(self.notebook, self.snippet)

class ResultWrapper():
  def __init__(self, cols, rows, has_more):
    self._cols = cols
    self._rows = rows
    self.has_more = has_more

  def cols(self):
    return [column['name'] for column in self._cols]

  def full_cols(self):
    return self._cols

  def rows(self):
    return self._rows
