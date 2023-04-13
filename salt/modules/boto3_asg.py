# -*- coding: utf-8 -*-
'''
NB: This module is a copy of `boto_asg.py`, which has then been lightly modified to migrate from `boto` to `boto3` as
required (read: we haven't expunged `boto` here, but it creates a place in which we can migrate in an ad-hoc fashion).
Connection module for Amazon Autoscale Groups

.. versionadded:: 2014.7.0

:configuration: This module accepts explicit autoscale credentials but can also
    utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in a pillar or
    in the minion's config file:

    .. code-block:: yaml

        asg.keyid: GKTADJGHEIQSXMKKRBJ08H
        asg.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

    A region may also be specified in the configuration:

    .. code-block:: yaml

        asg.region: us-east-1

    If a region is not specified, the default is us-east-1.

    It's also possible to specify key, keyid and region via a profile, either
    as a passed in dict, or as a string to pull from pillars or minion config:

    .. code-block:: yaml

        myprofile:
            keyid: GKTADJGHEIQSXMKKRBJ08H
            key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
            region: us-east-1

:depends: boto
:depends: boto3
'''
# keep lint from choking on _get_conn and _cache_id
#pylint: disable=E0602

# Import Python libs
from __future__ import absolute_import, print_function, unicode_literals
# import datetime
import logging
import sys
import time
# import email.mime.multipart

log = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Import third party libs
# from salt.ext import six
try:
    import boto
    import boto.ec2
    import boto.ec2.instance
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    import boto3  # pylint: disable=unused-import
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False


# Import Salt libs
import salt.utils.compat
import salt.utils.json
import salt.utils.odict as odict
import salt.utils.versions


def __virtual__():
    '''
    Only load if boto libraries exist.
    '''
    has_boto_reqs = salt.utils.versions.check_boto_reqs()
    if has_boto_reqs is True:
        __utils__['boto.assign_funcs'](__name__, 'asg', module='ec2.autoscale', pack=__salt__)
        setattr(sys.modules[__name__], '_get_ec2_conn',
                __utils__['boto.get_connection_func']('ec2'))
    return has_boto_reqs


def __init__(opts):
    salt.utils.compat.pack_dunder(__name__)
    if HAS_BOTO:
        __utils__['boto3.assign_funcs'](
            __name__, 'autoscaling',
            get_conn_funcname='_get_conn_autoscaling_boto3')


def get_config(name, region=None, key=None, keyid=None, profile=None):
    '''
    Get the configuration for an autoscale group.

    CLI example::

        salt myminion boto_asg.get_config myasg region=us-east-1
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    # Obtain the boto3 client: note that this is a non-idiomatic way to obtain the client (in salt-land).
    # I believe we should be using _get_conn (or similar) somehow, but in the interests of moving quickly,
    # this should get us back up and running.
    client = boto3.client("autoscaling")

    # This lookup table allows us to map the (boto-inspired) key names expected by callers of this function, to the
    # actual key names returned by boto3.
    attr_key_lookup = {
        "name": "AutoScalingGroupName",
        "availability_zones": "AvailabilityZones",
        "default_cooldown": "DefaultCooldown",
        "desired_capacity": "DesiredCapacity",
        "health_check_period": "HealthCheckGracePeriod",
        "health_check_type": "HealthCheckType",
        "launch_config_name": "LaunchConfigurationName",
        "load_balancers": "LoadBalancerNames",
        "max_size": "MaxSize",
        "min_size": "MinSize",
        # boto3 doesn't seem to return a "placement group"-like key in its response, so this key is omitted.
        # "placement_group": None,
        "vpc_zone_identifier": "VPCZoneIdentifier",
        "tags": "Tags",
        "termination_policies": "TerminationPolicies",
        "suspended_processes": "SuspendedProcesses",
    }

    retries = 30
    while True:
        try:
            asg_paginator = client.get_paginator("describe_auto_scaling_groups")
            # Obtain the first ASG, defaulting to None if no ASGs were returned
            asg = next(
                iter(
                    asg_paginator.paginate(AutoScalingGroupNames=[name]).build_full_result()["AutoScalingGroups"]
                ),
                None
            )
            if not asg:
                return {}
            ret = odict.OrderedDict()
            for attr, keyname in attr_key_lookup.items():
                if attr == "tags":
                    _tags = []
                    for tag in asg[keyname]:
                        _tag = odict.OrderedDict()
                        _tag["key"] = tag["Key"]
                        _tag["value"] = tag["Value"]
                        _tag["propagate_at_launch"] = tag["PropagateAtLaunch"]
                        _tags.append(_tag)
                    ret["tags"] = _tags
                # Boto accepts a string or list as input for vpc_zone_identifier,
                # but always returns a comma separated list. We require lists in
                # states.
                elif attr == "vpc_zone_identifier":
                    ret[attr] = asg[keyname].split(",")
                # convert SuspendedProcess objects to names
                elif attr == "suspended_processes":
                    ret[attr] = sorted([p["ProcessName"] for p in asg[keyname]])
                # Note that boto3 doesn't return a placement group, we so skip it.
                elif attr == "placement_group":
                    continue
                else:
                    ret[attr] = asg[keyname]

            # scaling policies
            policies = conn.get_all_policies(as_group=name)
            ret["scaling_policies"] = []
            for policy in policies:
                ret["scaling_policies"].append(
                    dict([
                        ("name", policy.name),
                        ("adjustment_type", policy.adjustment_type),
                        ("scaling_adjustment", policy.scaling_adjustment),
                        ("min_adjustment_step", policy.min_adjustment_step),
                        ("cooldown", policy.cooldown)
                    ])
                )
            # scheduled actions
            actions = conn.get_all_scheduled_actions(as_group=name)
            ret['scheduled_actions'] = {}
            for action in actions:
                end_time = None
                if action.end_time:
                    end_time = action.end_time.isoformat()
                ret['scheduled_actions'][action.name] = dict([
                  ("min_size", action.min_size),
                  ("max_size", action.max_size),
                  # AWS bug
                  ("desired_capacity", int(action.desired_capacity)),
                  ("start_time", action.start_time.isoformat()),
                  ("end_time", end_time),
                  ("recurrence", action.recurrence)
                ])
            return ret
        except boto.exception.BotoServerError as e:
            if retries and e.code == 'Throttling':
                log.debug('Throttled by AWS API, retrying in 5 seconds...')
                time.sleep(5)
                retries -= 1
                continue
            log.error(e)
            return {}
