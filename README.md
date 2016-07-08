dd-agent-mychecks
===================================
My dd-agent checks plugin


Requirements
------------
- datadog-agent version 5.8.x or later


Installation
------------

1. Copy `checks.d/*.py` to `/etc/dd-agent/checks.d`
2. Copy `conf.d/*.yaml.example` to `/etc/dd-agent/conf.d/*.yaml`


My checks
------------

[screenshots](screenshots) is available.

- aws_ec2_elb_check.py: Monitoring number of `ELB` instances
- aws_redshift_status.py: Monitoring `Redshift` status
