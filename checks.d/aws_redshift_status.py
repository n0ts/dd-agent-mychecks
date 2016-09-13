import boto.redshift
import boto.utils
import datetime
import psycopg2
import time

from checks import AgentCheck


QUERY_TABLE_COUNT = """\
SELECT count(DISTINCT tablename) FROM pg_table_def WHERE schemaname = 'public'
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
        start = time.time()
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

            min_collection_interval = instance.get('min_collection_interval', self.init_config.get(
                    'min_collection_interval',
                        self.DEFAULT_MIN_COLLECTION_INTERVAL
                    )
            )
            today = datetime.datetime.utcnow()
            starttime = (today - datetime.timedelta(seconds=min_collection_interval)).strftime('%Y-%m-%d %H:%M:%S.%f')
            endtime = today.strftime('%Y-%m-%d %H:%M:%S.%f')

            results = self._db_query(conn, QUERY_TABLE_COUNT)
            self.gauge('aws.redshift_status.table_count', results[0][0], tags=tags)

            running_time = time.time() - start
            self.gauge('aws.redshift_status.response_time', running_time, tags=tags)

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
