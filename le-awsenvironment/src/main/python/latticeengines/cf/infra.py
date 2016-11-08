"""
ECS stack for network, i.e. load balancers
"""

import argparse
import boto3
import os

from .consul import write_to_stack
from .module.elb2 import TargetGroup, ApplicationLoadBalancer, Listener, ListenerRule
from .module.parameter import *
from .module.stack import Stack, teardown_stack, check_stack_not_exists, wait_for_stack_creation
from ..conf import AwsEnvironment

_S3_CF_PATH='cloudformation/infra'
TOMCAT_APP_HEALTH_MAP = {
    "swaggerprivate": "/",
    "swaggerpublic": "/",

    "eai": "/eai/v2/api-docs",
    "metadata": "/metadata/v2/api-docs",
    "scoring": "/scoring/v2/api-docs",
    "modeling": "/modeling/v2/api-docs",
    "dataflowapi": "/workflowapi/v2/api-docs",
    "workflowapi": "/workflowapi/v2/api-docs",
    "quartz": "/quartz/v2/api-docs",

    "scoringapi": "/score/health",
    "matchapi": "/match/health",
    "oauth2": "/oauth2/health",
    "playmaker": "/api/health",
    "pls": "/pls/health",
    "admin": "/admin/health"
}
PUBLIC_APPS = ["scoringapi", "oauth2", "playmaker", "pls"]
UI_APPS = ["lpi", "adminconsole"]

PARAM_TOMCAT_SECURITY_GROUP = Parameter("TomcatSecurityGroupId", "The security group to be used by tomcat", type="AWS::EC2::SecurityGroup::Id")
PARAM_NODEJS_SECURITY_GROUP = Parameter("NodeJsSecurityGroupId", "The security group to be used by nodejs", type="AWS::EC2::SecurityGroup::Id")
PARAM_PUBLIC_SUBNET_1 = Parameter("PublicSubnetId1", "The first public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_PUBLIC_SUBNET_2 = Parameter("PublicSubnetId2", "The second public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_PUBLIC_SUBNET_3 = Parameter("PublicSubnetId3", "The third public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")

LISTENER_RULE_COUNTER = {}

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.upload)

def template(environment, upload=False):
    stack = create_template()
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template():
    stack = Stack("AWS CloudFormation template for LPI infrastructure.")
    stack.add_params([PARAM_TOMCAT_SECURITY_GROUP, PARAM_NODEJS_SECURITY_GROUP, PARAM_SSL_CERTIFICATE_ARN, PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])

    # target groups
    tgs, tg_map = create_taget_groups()
    stack.add_resources(tgs)

    resources, albs = create_load_balancers(tg_map)
    stack.add_resources(resources)

    stack.add_ouputs(add_outputs(albs))
    return stack

def create_taget_groups():
    tgs = []
    tg_map = {}
    for app, health in TOMCAT_APP_HEALTH_MAP.items():
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health)
        tg.add_tag("product", "lpi")
        tgs.append(tg)
        tg_map[app] = tg
    for app in UI_APPS:
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon="/")
        tg.add_tag("product", "lpi")
        tgs.append(tg)
        tg_map[app] = tg
    return tgs, tg_map

def create_load_balancers(tg_map):
    albs = {}
    resources = []

    # private tomcat
    private_lb = ApplicationLoadBalancer("private", PARAM_TOMCAT_SECURITY_GROUP, [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3])
    for k, v in tg_map.items():
        private_lb.depends_on(v)
    resources.append(private_lb)
    albs["private"] = private_lb

    # public tomcat
    public_lb = ApplicationLoadBalancer("public", PARAM_TOMCAT_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])
    for k, v in tg_map.items():
        private_lb.depends_on(v)
    resources.append(public_lb)
    albs["public"] = public_lb

    # lpi
    lpi_lb = ApplicationLoadBalancer("lpi", PARAM_NODEJS_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])
    lpi_lb.depends_on(tg_map["lpi"])
    resources.append(lpi_lb)
    albs["lpi"] = lpi_lb

    # adminconsole
    ac_lb = ApplicationLoadBalancer("adminconsole", PARAM_NODEJS_SECURITY_GROUP, [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3])
    ac_lb.depends_on(tg_map["adminconsole"])
    resources.append(ac_lb)
    albs["adminconsole"] = ac_lb

    # listeners
    private_lsnr = create_listener(private_lb, tg_map["swaggerprivate"])
    resources.append(private_lsnr)
    public_lsnr = create_listener(public_lb, tg_map["swaggerpublic"])
    resources.append(public_lsnr)
    resources.append(create_listener(lpi_lb, tg_map["lpi"]))
    resources.append(create_listener(ac_lb, tg_map["adminconsole"]))

    # listener rules
    resources.append(create_listner_rule(private_lsnr, tg_map["matchapi"], "/match/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["metadata"], "/metadata/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["eai"], "/eai/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["workflowapi"], "/workflows/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["dataflowapi"], "/dataflows/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["scoring"], "/scoring/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["modeling"], "/modeling/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["quartz"], "/quartz/*"))
    resources.append(create_listner_rule(private_lsnr, tg_map["admin"], "/admin/*"))

    resources.append(create_listner_rule(public_lsnr, tg_map["pls"], "/pls/*"))
    resources.append(create_listner_rule(public_lsnr, tg_map["scoringapi"], "/scores/*"))
    resources.append(create_listner_rule(public_lsnr, tg_map["scoringapi"], "/scoreinternal/*"))
    resources.append(create_listner_rule(public_lsnr, tg_map["oauth2"], "/oauth2/*"))
    resources.append(create_listner_rule(public_lsnr, tg_map["playmaker"], "/api/*"))

    return resources, albs

def create_listener(lb, tg):
    listener = Listener(lb.name() + "Listener", lb, tg)
    listener.depends_on(lb)
    listener.depends_on(tg)
    return listener

def create_listner_rule(listener, tg, path):
    if listener.logical_id() not in LISTENER_RULE_COUNTER:
        LISTENER_RULE_COUNTER[listener.logical_id()] = 1
    else:
        LISTENER_RULE_COUNTER[listener.logical_id()] += 1
    priority = LISTENER_RULE_COUNTER[listener.logical_id()]
    lr = ListenerRule(listener.logical_id() + tg.logical_id(), listener, priority, tg, path)
    lr.depends_on(listener)
    lr.depends_on(tg)
    return lr

def add_outputs(albs):
    outputs = {}
    for k, v in albs.items():
        outputs["%sDNSName" % k] = {
            "Description" : "DNS name for load balancer " + k,
            "Value" : { "Fn::GetAtt" : [ v.logical_id(), "DNSName" ]}
        }
    return outputs

def provision_cli(args):
    teardown(args.stack)
    provision(args.environment, args.stack, args.consul)

def provision(environment, stackname, consul=None):
    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://s3.amazonaws.com/%s' % os.path.join(config.cf_bucket(), _S3_CF_PATH, 'template.json'),
        Parameters=[
            PARAM_VPC_ID.config(config.vpc()),
            PARAM_SUBNET_1.config(config.private_subnet_1()),
            PARAM_SUBNET_2.config(config.private_subnet_2()),
            PARAM_SUBNET_3.config(config.private_subnet_3()),
            PARAM_PUBLIC_SUBNET_1.config(config.private_subnet_1()),
            PARAM_PUBLIC_SUBNET_2.config(config.private_subnet_2()),
            PARAM_PUBLIC_SUBNET_3.config(config.private_subnet_3()),
            PARAM_KEY_NAME.config(config.ec2_key()),
            PARAM_ENVIRONMENT.config(environment),

            PARAM_TOMCAT_SECURITY_GROUP.config(config.tomcat_sg()),
            PARAM_NODEJS_SECURITY_GROUP.config(config.nodejs_sg()),
            PARAM_SSL_CERTIFICATE_ARN.config(config.ssl_certificate_arn())
        ],
        TimeoutInMinutes=60,
        OnFailure='ROLLBACK',
        Capabilities=[
            'CAPABILITY_IAM',
        ],
        Tags=[
            {
                'Key': 'product',
                'Value': 'lpi'
            }
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, stackname)

    if consul is not None:
        albs = get_albs(stackname)
        for k, v in albs.items():
            write_to_stack(consul, environment, stackname, k, v)

def get_albs(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    albs = {}
    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if 'DNSName' in key:
            albs[key] = value
            print "DNS name for %s is %s" % (key, value)
    return albs

def teardown(stackname):
    client = boto3.client('cloudformation')
    teardown_stack(client, stackname)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster CloudFormation cli')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("template")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    parser1.add_argument('-s', dest='stack', type=str, required=True, help='the LE_STACK to be created')
    parser1.add_argument('-c', dest='consul', type=str, help='consul server address')
    parser1.set_defaults(func=provision_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
