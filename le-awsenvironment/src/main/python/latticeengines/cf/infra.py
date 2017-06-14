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

    "api": "/rest/add/1/2",
    "eai": "/eai/v2/api-docs",
    "metadata": "/metadata/v2/api-docs",
    "scoring": "/scoring/v2/api-docs",
    "modeling": "/modeling/v2/api-docs",
    "dataflowapi": "/dataflowapi/v2/api-docs",
    "workflowapi": "/workflowapi/v2/api-docs",
    "quartz": "/quartz/v2/api-docs",
    "modelquality": "/modelquality/v2/api-docs",
    "propdata": "/propdata/v2/api-docs",
    "datacloudapi": "/datacloudapi/v2/api-docs",
    "dellebi": "/dellebi/v2/api-docs",
    "objectapi": "/objectapi/health",

    "scoringapi": "/score/health",
    "matchapi": "/match/health",
    "ulysses": "/ulysses/health",
    "oauth2": "/oauth2/health",
    "playmaker": "/api/health",
    "pls": "/pls/health",
    "admin": "/admin/health"
}
PUBLIC_APPS = ["scoringapi", "oauth2", "playmaker", "pls", "ulysses"]
UI_APPS = ["lpi", "adminconsole"]

API_APPS = [ 'playmaker', 'scoringapi', 'ulysses' ]
APP_APPS = [ 'lpi', 'pls' ]
OAUTH_APPS = [ 'oauth2' ]
ADMINCONSOLE_APPS = [ 'adminconsole', 'matchapi', 'api', 'propdata' ]

PARAM_TOMCAT_SECURITY_GROUP = Parameter("TomcatSecurityGroupId", "The security group to be used by tomcat", type="AWS::EC2::SecurityGroup::Id")
PARAM_NODEJS_SECURITY_GROUP = Parameter("NodeJsSecurityGroupId", "The security group to be used by nodejs", type="AWS::EC2::SecurityGroup::Id")
PARAM_HTTPS_SECURITY_GROUP = Parameter("HTTPSGroupId", "The security group to be used by public https", type="AWS::EC2::SecurityGroup::Id")
PARAM_PUBLIC_SUBNET_1 = Parameter("PublicSubnetId1", "The first public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_PUBLIC_SUBNET_2 = Parameter("PublicSubnetId2", "The second public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_PUBLIC_SUBNET_3 = Parameter("PublicSubnetId3", "The third public subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")

PARAM_LE_STACK = Parameter("LeStack", "Tag value for le-stack")

LISTENER_RULE_COUNTER = {}

def main():
    args = parse_args()
    args.func(args)

def template_cli(args):
    template(args.environment, args.stack, args.public, upload=args.upload)

def template(environment, stack, public, ui=False, upload=False):
    if public:
        stack = create_public_template(environment)
    else:
        stack = create_template(environment, stack)
    if upload:
        stack.validate()
        stack.upload(environment, _S3_CF_PATH)
    else:
        print stack.json()
        stack.validate()

def create_template(env, stack_tag):
    stack = Stack("AWS CloudFormation template for LPI infrastructure.")
    stack.add_params([PARAM_TOMCAT_SECURITY_GROUP, PARAM_NODEJS_SECURITY_GROUP, PARAM_SSL_CERTIFICATE_ARN, PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3, PARAM_LE_STACK])

    # target groups
    tgs, tg_map = create_taget_groups(env, stack_tag)
    stack.add_resources(tgs)

    resources, albs = create_load_balancers(tg_map, stack_tag)
    stack.add_resources(resources)

    stack.add_ouputs(add_outputs(albs))
    return stack

def create_public_template(env):
    stack = Stack("AWS CloudFormation template for LPI public urls.")
    stack.add_params([PARAM_HTTPS_SECURITY_GROUP, PARAM_NODEJS_SECURITY_GROUP, PARAM_SSL_CERTIFICATE_ARN, PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])

    # target groups
    tgs, tg_map = create_public_target_groups(env)
    stack.add_resources(tgs)

    resources, albs = create_public_load_balancers(env, tg_map)
    stack.add_resources(resources)

    stack.add_ouputs(add_outputs(albs))
    return stack


def create_public_target_groups(environment):
    tgs = []
    tg_map = {}
    for app in API_APPS:
        if app in TOMCAT_APP_HEALTH_MAP:
            health = TOMCAT_APP_HEALTH_MAP[app]
        else:
            health = '/'
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health, name="api-" + app)
        tg.add_tag("le-env", environment.replace("cluster", ""))
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tgs.append(tg)
        tg_map[app] = tg

    for app in APP_APPS:
        if app in TOMCAT_APP_HEALTH_MAP:
            health = TOMCAT_APP_HEALTH_MAP[app]
        else:
            health = '/'
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health, name="app-" + app)
        tg.add_tag("le-env", environment.replace("cluster", ""))
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tgs.append(tg)
        tg_map[app] = tg

    for app in OAUTH_APPS:
        if app in TOMCAT_APP_HEALTH_MAP:
            health = TOMCAT_APP_HEALTH_MAP[app]
        else:
            health = '/'
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health, name="oauth2-" + app)
        tg.add_tag("le-env", environment.replace("cluster", ""))
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tgs.append(tg)
        tg_map[app] = tg

    for app in ADMINCONSOLE_APPS:
        if app in TOMCAT_APP_HEALTH_MAP:
            health = TOMCAT_APP_HEALTH_MAP[app]
        else:
            health = '/'
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health, name="adminconsole-" + app)
        tg.add_tag("le-env", environment.replace("cluster", ""))
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tgs.append(tg)
        tg_map[app] = tg

    return tgs, tg_map

def create_public_load_balancers(env, tg_map):
    albs = {}
    resources = []

    # app.lattice-engines.com
    app_lb = ApplicationLoadBalancer("app", PARAM_HTTPS_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3], internet_facing=True)
    app_lb.idle_timeout(600)
    app_lb.add_tag("le-env", env.replace("cluster", ""))
    app_lb.add_tag("le-product", "lpi")
    for k, v in tg_map.items():
        app_lb.depends_on(v)
    resources.append(app_lb)
    albs["app"] = app_lb

    # api.lattice-engines.com
    api_lb = ApplicationLoadBalancer("api", PARAM_HTTPS_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3], internet_facing=True)
    api_lb.idle_timeout(600)
    app_lb.add_tag("le-env", env.replace("cluster", ""))
    api_lb.add_tag("le-product", "lpi")
    for k, v in tg_map.items():
        app_lb.depends_on(v)
    resources.append(api_lb)
    albs["api"] = api_lb

    # oauth.lattice-engines.com
    oauth_lb = ApplicationLoadBalancer("oauth2", PARAM_HTTPS_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3], internet_facing=True)
    oauth_lb.idle_timeout(600)
    oauth_lb.add_tag("le-env", env.replace("cluster", ""))
    oauth_lb.add_tag("le-product", "lpi")
    for k, v in tg_map.items():
        oauth_lb.depends_on(v)
    resources.append(oauth_lb)
    albs["oauth2"] = oauth_lb

    # admin.prod.lattice.local
    adminconsole_lb = ApplicationLoadBalancer("adminconsole", PARAM_TOMCAT_SECURITY_GROUP, [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3])
    adminconsole_lb.idle_timeout(600)
    adminconsole_lb.add_tag("le-env", env.replace("cluster", ""))
    adminconsole_lb.add_tag("le-product", "lpi")
    for k, v in tg_map.items():
        adminconsole_lb.depends_on(v)
    resources.append(adminconsole_lb)
    albs["adminconsole"] = adminconsole_lb

    # app listeners
    lpi_lsnr = create_listener(app_lb, tg_map["lpi"])
    resources.append(lpi_lsnr)
    resources.append(create_listener_rule(lpi_lsnr, tg_map["pls"], "/pls/*"))
    resources.append(create_listener_rule(lpi_lsnr, tg_map["pls"], "/lp2/*"))

    # api listeners
    api_lsnr = create_listener(api_lb, tg_map["playmaker"])
    resources.append(api_lsnr)
    resources.append(create_listener_rule(api_lsnr, tg_map["scoringapi"], "/score/*"))
    resources.append(create_listener_rule(api_lsnr, tg_map["ulysses"], "/ulysses/*"))

    # oauth listeners
    oauth_lsnr = create_listener(oauth_lb, tg_map["oauth2"])
    resources.append(oauth_lsnr)

    # adminconsole listeners
    adminconsole_lsnr = create_listener(ac_lb, tg_map["adminconsole"])
    resources.append(adminconsole_lsnr)
    resources.append(create_listener_rule(adminconsole_lsnr, tg_map["matchapi"], "/match/*"))
    resources.append(create_listener_rule(adminconsole_lsnr, tg_map["api"], "/api/*"))
    resources.append(create_listener_rule(adminconsole_lsnr, tg_map["propdata"], "/propdata/*"))

    return resources, albs

def create_taget_groups(env, stack):
    tgs = []
    tg_map = {}
    for app, health in TOMCAT_APP_HEALTH_MAP.items():
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon=health)
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tg.add_tag("le-stack", stack)
        tg.add_tag("le-env", env.replace("cluster", ""))
        tgs.append(tg)
        tg_map[app] = tg

    for app in UI_APPS:
        tg = TargetGroup(app, port="443", protocol="HTTPS", checkon="/")
        tg.add_tag("le-product", "lpi")
        tg.add_tag("le-service", app)
        tg.add_tag("le-stack", stack)
        tg.add_tag("le-env", env.replace("cluster", ""))
        tgs.append(tg)
        tg_map[app] = tg
    return tgs, tg_map

def create_load_balancers(tg_map, stack):
    albs = {}
    resources = []

    # private tomcat
    private_lb = ApplicationLoadBalancer("private", PARAM_TOMCAT_SECURITY_GROUP, [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3])
    private_lb.idle_timeout(600)
    private_lb.add_tag("le-product", "lpi")
    private_lb.add_tag("le-stack", stack)
    for k, v in tg_map.items():
        private_lb.depends_on(v)
    resources.append(private_lb)
    albs["private"] = private_lb

    # public tomcat
    public_lb = ApplicationLoadBalancer("public", PARAM_TOMCAT_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])
    public_lb.idle_timeout(600)
    public_lb.add_tag("le-product", "lpi")
    public_lb.add_tag("le-stack", stack)
    for k, v in tg_map.items():
        private_lb.depends_on(v)
    resources.append(public_lb)
    albs["public"] = public_lb

    # listeners
    private_lsnr = create_listener(private_lb, tg_map["swaggerprivate"])
    resources.append(private_lsnr)
    public_lsnr = create_listener(public_lb, tg_map["swaggerpublic"])
    resources.append(public_lsnr)

    # listener rules
    resources.append(create_listener_rule(private_lsnr, tg_map["matchapi"], "/match/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["admin"], "/admin/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["workflowapi"], "/workflowapi/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["dataflowapi"], "/dataflowapi/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["objectapi"], "/objectapi/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["metadata"], "/metadata/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["eai"], "/eai/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["scoring"], "/scoring/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["modeling"], "/modeling/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["api"], "/rest/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["datacloudapi"], "/datacloudapi/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["modelquality"], "/modelquality/*"))
    resources.append(create_listener_rule(private_lsnr, tg_map["propdata"], "/propdata/*"))

    resources.append(create_listener_rule(public_lsnr, tg_map["pls"], "/pls/*"))
    resources.append(create_listener_rule(public_lsnr, tg_map["scoringapi"], "/score/*"))
    resources.append(create_listener_rule(public_lsnr, tg_map["ulysses"], "/ulysses/*"))
    resources.append(create_listener_rule(public_lsnr, tg_map["scoringapi"], "/scoreinternal/*"))
    resources.append(create_listener_rule(public_lsnr, tg_map["oauth2"], "/oauth2/*"))
    resources.append(create_listener_rule(public_lsnr, tg_map["playmaker"], "/api/*"))

    # lpi
    lpi_lb = ApplicationLoadBalancer("lpi", PARAM_NODEJS_SECURITY_GROUP, [PARAM_PUBLIC_SUBNET_1, PARAM_PUBLIC_SUBNET_2, PARAM_PUBLIC_SUBNET_3])
    lpi_lb.idle_timeout(600)
    lpi_lb.depends_on(tg_map["lpi"])
    lpi_lb.add_tag("le-product", "lpi")
    lpi_lb.add_tag("le-stack", stack)
    resources.append(lpi_lb)
    albs["lpi"] = lpi_lb

    # adminconsole
    ac_lb = ApplicationLoadBalancer("adminconsole", PARAM_NODEJS_SECURITY_GROUP, [PARAM_SUBNET_1, PARAM_SUBNET_2, PARAM_SUBNET_3])
    ac_lb.idle_timeout(600)
    ac_lb.depends_on(tg_map["adminconsole"])
    ac_lb.add_tag("le-product", "lpi")
    ac_lb.add_tag("le-stack", stack)
    resources.append(ac_lb)
    albs["adminconsole"] = ac_lb

    resources.append(create_listener(lpi_lb, tg_map["lpi"]))
    resources.append(create_listener(ac_lb, tg_map["adminconsole"]))

    return resources, albs

def create_listener(lb, tg, port=443):
    listener = Listener(lb.name() + "Listener", lb, tg, port=port)
    listener.depends_on(lb)
    listener.depends_on(tg)
    return listener

def create_listener_rule(listener, tg, path):
    if listener.logical_id() not in LISTENER_RULE_COUNTER:
        LISTENER_RULE_COUNTER[listener.logical_id()] = 1
    else:
        LISTENER_RULE_COUNTER[listener.logical_id()] += 1
    priority = LISTENER_RULE_COUNTER[listener.logical_id()]
    lr = ListenerRule(listener.logical_id() + str(priority), listener, priority, tg, path)
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
    provision(args.environment, args.stack)

def provision(environment, stackname):
    config = AwsEnvironment(environment)
    client = boto3.client('cloudformation')
    check_stack_not_exists(client, stackname)

    response = client.create_stack(
        StackName=stackname,
        TemplateURL='https://%s/%s' % (config.s3_endpoint(), os.path.join(config.cf_bucket(), _S3_CF_PATH, 'template.json')),
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
            PARAM_LE_STACK.config(stackname),

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
                'Key': 'le-env',
                'Value': config.tag_le_env()
            },
            {
                'Key': 'le-product',
                'Value': 'lpi'
            },
            {
                'Key': 'le-stack',
                'Value': stackname.replace('lpi-', '')
            }
        ]
    )
    print 'Got StackId: %s' % response['StackId']
    wait_for_stack_creation(client, stackname)

    consul = config.consul_server()
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
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['devcluster', 'qacluster','prodcluster', 'dr'], help='environment')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.add_argument('-s', dest='stack', type=str, required=False, help='the short stack name for tagging')
    parser1.add_argument('--public', dest='public', action='store_true', help='bootstrap the public urls. ignore -s opt')
    parser1.set_defaults(func=template_cli)

    parser1 = commands.add_parser("provision")
    parser1.add_argument('-e', dest='environment', type=str, default='qacluster', choices=['devcluster', 'qacluster','prodcluster', 'dr'], help='environment')
    parser1.add_argument('-s', dest='stack', type=str, required=False, help='the LE_STACK to be created')
    parser1.add_argument('--public', dest='public', action='store_true', help='bootstrap the public urls. ignore -s opt')
    parser1.set_defaults(func=provision_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
