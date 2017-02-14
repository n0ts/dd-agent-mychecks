import boto.ec2.elb
import boto.utils
import re

from checks import AgentCheck


class AwsEc2ElbCheck(AgentCheck):
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def _load_conf(self, instance):
        load_balancer_name = instance.get('load_balancer_name')
        if load_balancer_name is None:
            raise Exception('Bad configuration. You must specify a load_balancer_name')

        instance_id = instance.get('instance_id')

        aws_access_key_id = instance.get('aws_access_key_id')
        aws_secret_access_key = instance.get('aws_secret_access_key')
        aws_region = instance.get('aws_region')
        if aws_region is None:
            aws_region = boto.utils.get_instance_metadata()['placement']['availability-zone'][:-1]

        thresholds = instance.get('thresholds')

        tags = instance.get('tags', [])
        if instance_id is not None:
            tags.append('instance_id:%s' % instance_id)

        tags.append('aws_region:%s' % aws_region)

        return load_balancer_name, instance_id, \
            aws_access_key_id, aws_secret_access_key, aws_region, thresholds, tags

    def _service_check(self, instance, tags, thresholds, instance_by_state, in_service_index):
        status = AgentCheck.OK
        if thresholds is None:
            return self.warning('thresholds configuration is empty')

        if in_service_index is not False:
            count = instance_by_state[in_service_index]['count']
            threshold = '-'

        if in_service_index is False:
            status = AgentCheck.CRITICAL
        elif instance_by_state[in_service_index]['count'] < thresholds['warning']:
            status = AgentCheck.WARNING
            count = threshold = thresholds['warning']
        elif instance_by_state[in_service_index]['count'] < thresholds['critical']:
            status = AgentCheck.CRITICAL
            count = threshold = thresholds['critical']

        status_str = {
            AgentCheck.OK: 'OK',
            AgentCheck.WARNING: 'WARNING',
            AgentCheck.CRITICAL: 'CRITICAL'
        }
        message_str = 'instance status \'InService\' is %s - %s/%s'
        self.service_check(
            'aws_ec2_elb_check.up_in_service',
            status,
            tags=tags,
            message=message_str % (status_str[status], count, threshold)
        )

    def check(self, instance):
        load_balancer_name, instance_id, \
            aws_access_key_id, aws_secret_access_key, aws_region, \
            thresholds, tags = self._load_conf(instance)
        service_check_tags = [ 'load_balancer_name:%s' % load_balancer_name ]

        elb = boto.ec2.elb.connect_to_region(aws_region,
                                             aws_access_key_id=aws_access_key_id,
                                             aws_secret_access_key=aws_secret_access_key)

        instances = None
        if instance_id is not None:
            instances = [instance_id]

        try:
            instance_by_state = [
                { 'state': 'in_service', 'count': 0 },
                { 'state': 'out_of_service', 'count': 0 },
                { 'state': 'unknown', 'count': 0 },
            ]
            health_states = elb.describe_instance_health(load_balancer_name, instances=instances)
            for health_state in health_states:
                if health_state.state == 'InService':
                    instance_by_state[0]['count'] += 1
                elif health_state.state == 'OutOfService':
                    instance_by_state[1]['count'] += 1
                    self.warning('%s is %s - %s'
                                 % (health_state.instance_id, health_state.state,
                                    health_state.reason_code))
                else:
                    instance_by_state[2]['count'] += 1
                    self.warning('%s is %s - %s'
                                 % (health_state.instance_id, health_state.state,
                                    health_state.reason_code))

                for state in instance_by_state:
                    self.gauge('aws_ec2_elb_check.%s' % state['state'], state['count'], tags=tags)

            self._service_check(instance, tags=service_check_tags, thresholds=thresholds,
                                instance_by_state=instance_by_state, in_service_index=0)
        except Exception, e:
            self.warning(e)
