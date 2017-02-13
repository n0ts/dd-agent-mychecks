import argparse
import boto.redshift
import boto.utils
import datetime
import logging
import os
import psycopg2
import sys
import time
import yaml

from datadog import initialize
from datadog.util.config import get_config
from datadog import ThreadStats

from logging import getLogger, StreamHandler, DEBUG

sys.path.append('/opt/datadog-agent/agent')
import config


QUERY_TABLE_COUNT = """\
select count(DISTINCT tablename)
  from pg_table_def where schemaname = 'public'
"""

QUERY_NODE = """\
select node, sum(rows)
  from stv_slices m
  join stv_tbl_perm s on s.slice = m.slice
  group by node
"""

QUERY_TABLE_RECORD = """\
select name, sum(rows) as rows
  from stv_tbl_perm
  group by name
"""

QUERY_TABLE_STATUS = """\
select "table", size, tbl_rows, skew_rows
  from svv_table_info
"""

QUERY_LOG_TYPE = """\
select count(*)
  from svl_qlog
  where starttime >= '%s' and endtime <= '%s' and substring like '%s';
"""

class AwsRedshiftStatus:
    def __init__(self, config):
        parser = argparse.ArgumentParser()
        parser.add_argument('--from-cron', action='store_true')
        parser.add_argument('--debug', action='store_true')
        args = parser.parse_args()

        log_level = logging.INFO
        if args.from_cron:
            log_level = logging.WARN
        elif args.debug:
            log_level = logging.DEBUG

        logging.basicConfig(level=log_level)
        config.initialize_logging(self.__class__.__name__)
        self.log = config.log

        try:
            agent_config = get_config()
            if 'api_key' in agent_config:
                api_key = agent_config['api_key']
        except:
            api_key = os.environ['DD_API_KEY']

        initialize(api_key=api_key)

    def _load_conf(self, instance):
        name = instance.get('name')
        cluster_name = instance.get('cluster_name')
        cluster_address = instance.get('cluster_address')
        cluster_port = instance.get('cluster_port')
        if cluster_name is None and cluster_address is None and cluster_port is None:
            raise Exception('Bad configuration. You must specify a cluster_name or cluster_address and cluster_port')

        db_name = instance.get('db_name')
        if db_name is None:
            raise Exception('Bad configuration. You must specify a db_name')

        user_name = instance.get('user_name')
        if user_name is None:
            raise Exception('Bad configuration. You must specify a user_name')

        user_password = instance.get('user_password')
        if user_password is None:
            raise Exception('Bad sconfiguration. You must specify a user_password')

        aws_access_key_id = instance.get('aws_access_key_id')
        aws_secret_access_key = instance.get('aws_secret_access_key')
        aws_region = instance.get('aws_region')
        if aws_region is None:
            aws_region = boto.utils.get_instance_metadata()['placement']['availability-zone'][:-1]

        query = instance.get('query', False)

        tags = instance.get('tags', [])
        tags.append('name:%s' % name)
        if cluster_name is not None:
            tags.append('cluster_name:%s' % cluster_name)
        tags.append('aws_region:%s' % aws_region)

        return name, cluster_name, cluster_address, cluster_port, db_name, user_name, user_password, \
            aws_access_key_id, aws_secret_access_key, aws_region, query, tags

    def _db_query(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def check(self):
        try:
            yaml_file = os.environ.get('DATADOG_CONF', '/etc/dd-agent/conf.d/aws_redshift_status.yaml')
            yaml_data = yaml.load(file(yaml_file))
            init_config = yaml_data['init_config']
            interval = init_config.get('min_collection_interval', 300)

            stats = ThreadStats()
            stats.start(flush_interval=10, roll_up_interval=1, device=None,
                        flush_in_thread=False, flush_in_greenlet=False, disabled=False)

            start = time.time()
            for instance in yaml_data['instances']:
                self.log.debug('instance name is %s' % instance['name'])

                name, cluster_name, cluster_address, cluster_port, db_name, user_name, user_password, \
                    aws_access_key_id, aws_secret_access_key, aws_region, query, \
                    tags = self._load_conf(instance)

                if cluster_address is None and cluster_port is None:
                    redshift = boto.redshift.connect_to_region(aws_region,
                                                               aws_access_key_id=aws_access_key_id,
                                                               aws_secret_access_key=aws_secret_access_key)
                    clusters = redshift.describe_clusters(cluster_name)
                    if len(clusters) == 0:
                        raise Exception('Cluster is empty')

                    cluster = clusters['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]
                    endpoint = cluster['Endpoint']
                    cluster_address = endpoint['Address']
                    cluster_port = endpoint['Port']

                connect_timeout = init_config.get('connect_timeout', 5)
                conn = psycopg2.connect(
                    host=cluster_address,
                    port=cluster_port,
                    database=db_name,
                    user=user_name,
                    password=user_password,
                    connect_timeout=connect_timeout,
                )

                today = datetime.datetime.utcnow()
                starttime = (today - datetime.timedelta(seconds=interval)).strftime('%Y-%m-%d %H:%M:%S.%f')
                endtime = today.strftime('%Y-%m-%d %H:%M:%S.%f')

                results = self._db_query(conn, QUERY_TABLE_COUNT)
                stats.gauge('aws.redshift_status.table_count', results[0][0], tags=tags)
                self.log.debug('aws.redshift_status.table_count is %s' % results[0][0])

                results = self._db_query(conn, QUERY_NODE)
                for row in results:
                    gauge_tags = tags[:]
                    gauge_tags.append('node:%s' % row[0])
                    stats.gauge('aws_redshift_status.node_slice', row[1], tags=gauge_tags)
                    self.log.debug('aws_redshift_status.node_slice is %s' % row[1])

                results = self._db_query(conn, QUERY_TABLE_RECORD)
                for row in results:
                    gauge_tags = tags[:]
                    gauge_tags.append('table:%s' % row[0])
                    stats.gauge('aws_redshift_status.table_records', row[1], tags=gauge_tags)
                    self.log.debug('aws_redshift_status.table_records is %s' % row[1])

                results = self._db_query(conn, QUERY_TABLE_STATUS)
                for row in results:
                    gauge_tags = tags[:]
                    gauge_tags.append('table:%s' % row[0])
                    stats.gauge('aws_redshift_status.table_status.size', row[1], tags=gauge_tags)
                    self.log.debug('aws_redshift_status.table_status.size is %s' % row[1])
                    stats.gauge('aws_redshift_status.table_status.tbl_rows', row[2], tags=gauge_tags)
                    self.log.debug('aws_redshift_status.table_status.tbl_rows is %s' % row[2])
                    stats.gauge('aws_redshift_status.table_status.skew_rows', row[3], tags=gauge_tags)
                    self.log.debug('aws_redshift_status.table_status.skew_rows is %s' % row[3])

                for q in [ 'select', 'insert', 'update', 'delete', 'analyze' ]:
                    results = self._db_query(conn, QUERY_LOG_TYPE % (starttime, endtime, '%s %%' % q))
                    for row in results:
                        stats.gauge('aws_redshift_status.query.%s' % q, row[0], tags=tags)
                        self.log.debug('aws_redshift_status.query.%s is %s' % (q, row[0]))

                running_time = time.time() - start
                stats.gauge('aws_redshift_status.response_time', running_time, tags=tags)
                self.log.debug('aws_redshift_status.response_time is %s' % running_time)

            stats.flush()
            stop = stats.stop()
            self.log.debug('Stopping is %s' % stop)
        except Exception:
            self.log.warning(sys.exc_info())

        finally:
            conn.close()


if __name__ == '__main__':
    status = AwsRedshiftStatus(config)
    status.check()
