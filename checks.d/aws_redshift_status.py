import boto.redshift
import boto.utils
import datetime
import psycopg2

from checks import AgentCheck


QUERY_NODE = """\
select node, sum(rows)
  from stv_slices m
  join stv_tbl_perm s on s.slice = m.slice
  group by node
"""

QUERY_TABLE = """\
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

class AwsRedshiftStatus(AgentCheck):
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def _load_conf(self, instance):
        cluster_name = instance.get('cluster_name')
        cluster_address = instance.get('cluster_address')
        cluster_port = instance.get('cluster_port')
        if cluster_name == None and (cluster_address == None and cluster_port == None):
            raise Exception("Bad configuration. You must specify a cluster_name or cluster_address and cluster_port")

        db_name = instance.get('db_name')
        if db_name == None:
            raise Exception("Bad configuration. You must specify a db_name")

        user_name = instance.get('user_name')
        if user_name == None:
            raise Exception("Bad configuration. You must specify a user_name")

        user_password = instance.get('user_password')
        if user_password == None:
            raise Exception("Bad configuration. You must specify a user_password")

        aws_access_key_id = instance.get('aws_access_key_id')
        aws_secret_access_key = instance.get('aws_secret_access_key')
        aws_region = instance.get('aws_region')
        if aws_region == None:
            aws_region = boto.utils.get_instance_metadata()['placement']['availability-zone'][:-1]

        tags = instance.get('tags', [])

        tags.append('cluster_name:%s' % cluster_name)
        tags.append('aws_region:%s' % aws_region)

        return cluster_name, cluster_address, cluster_port, db_name, user_name, user_password, \
            aws_access_key_id, aws_secret_access_key, aws_region, tags

    def _db_query(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def check(self, instance):
        cluster_name, cluster_address, cluster_port, db_name, user_name, user_password, \
            aws_access_key_id, aws_secret_access_key, aws_region, \
            tags = self._load_conf(instance)
        if cluster_address == None and cluster_port == None:
            service_check_tags = [ "cluster_name:%s" % cluster_name ]
        else:
            service_check_tags = [ "cluster_address:%s" % cluster_address, "cluster_port:%s" % cluster_port ]

        redshift = boto.redshift.connect_to_region(aws_region,
                                                   aws_access_key_id=aws_access_key_id,
                                                   aws_secret_access_key=aws_secret_access_key)

        try:
            if cluster_address == None and cluster_port == None:
                clusters = redshift.describe_clusters(cluster_name)
                if len(clusters) == 0:
                    raise Exception("Cluster is empty")

                cluster = clusters['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]
                endpoint = cluster['Endpoint']
                cluster_address = endpoint['Address']
                cluster_port = endpoint['Port']

            conn = psycopg2.connect(
                host=cluster_address,
                port=cluster_port,
                database=db_name,
                user=user_name,
                password=user_password
            )

            results = self._db_query(conn, QUERY_NODE)
            for row in results:
                self.gauge('aws_redshift_status.node_slice.%d' % row[0], row[1], tags=tags)

            results = self._db_query(conn, QUERY_TABLE)
            for row in results:
                self.gauge('aws_redshift_status.table.%s' % row[0], row[1], tags=tags)

            results = self._db_query(conn, QUERY_TABLE_STATUS)
            for row in results:
                self.gauge('aws_redshift_status.table_status.%s.size' % row[0], row[1], tags=tags)
                self.gauge('aws_redshift_status.table_status.%s.tbl_rows' % row[0], row[2], tags=tags)
                self.gauge('aws_redshift_status.table_status.%s.skew_rows' % row[0], row[3], tags=tags)

            min_collection_interval = instance.get('min_collection_interval', self.init_config.get(
                    'min_collection_interval',
                        self.DEFAULT_MIN_COLLECTION_INTERVAL
                    )
            )
            today = datetime.datetime.utcnow()
            starttime = (today - datetime.timedelta(seconds=min_collection_interval)).strftime('%Y-%m-%d %H:%M:%S.%f')
            endtime = today.strftime('%Y-%m-%d %H:%M:%S.%f')

            for query in [ 'select', 'insert', 'update', 'delete', 'analyze' ]:
                results = self._db_query(conn, QUERY_LOG_TYPE % (starttime, endtime, '%s %%' % query))
                for row in results:
                    self.gauge('aws_redshift_status.query.%s' % query, row[0], tags=tags)

            self.service_check(
                'aws_redshift_status.up',
                AgentCheck.OK,
                tags=service_check_tags,
            )
        except Exception, e:
            self.warning(e)
            self.service_check(
                'aws_redshift_status.up',
                AgentCheck.WARNING,
                tags=tags,
                message="Exception - %s" % (e)
            )

        finally:
            conn.close()
