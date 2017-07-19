import argparse
import httplib2
import json
import logging
import time
import zc.zk

ENVIRONMENT = None
ZK = None
POD = 'Default'
HTTP = httplib2.Http(".cache", disable_ssl_certificate_validation=True)
AD_TOKEN = None
BOOTSTRAP_OK = 'OK'
BOOTSTRAP_ERROR = 'ERROR'


logger = logging.getLogger(__name__)

def main():
    global ENVIRONMENT
    args = parseCliArgs()

    ENVIRONMENT = args.environment
    start_zk()

    admin_login()

    dante = export_dante_tree(args.tenant)
    logger.info('Back up current Dante configurations.')

    state = admin_get_bootstrap_state(args.tenant)
    if state == BOOTSTRAP_OK or state == BOOTSTRAP_ERROR:
        raise Exception('Bootstrap State is already %s!' % state)

    logger.info('Current bootstrap state is %s' % state)

    template = post_body_template()
    template = template.replace('{% LatticeAdminEmails %}', pls_lattice_admins().replace('"', '\\\"'))
    template = template.replace('{% SuperAdminEmails %}', pls_super_admins().replace('"', '\\\"'))
    template = template.replace('{% TenantName %}', args.tenant)
    admin_create_tenant(args.tenant, template)

    wait_bootstrap_ok(args.tenant)
    logger.info('Successfully bootstrapped [%s]' % args.tenant)
    import_dante_tree(args.tenant, dante)
    logger.info('Restore original Dante configurations.')


def default_config_tree(service):
    node = "%s/Default/%s" % (pod_path(), service)
    return ZK.export_tree(node)

def default_config_value(service, relative_path):
    node = "%s/Default/%s/%s" % (pod_path(), service, relative_path)
    return ZK.get(node)[0]

def pls_lattice_admins():
    return default_config_value('PLS', 'LatticeAdminEmails')

def pls_super_admins():
    return default_config_value('PLS', 'SuperAdminEmails')

def export_dante_tree(tenant):
    node = "%s/Contracts/%s/Tenants/%s/Spaces/Production/Services/Dante" % (pod_path(), tenant, tenant)
    return ZK.export_tree(node)

def import_dante_tree(tenant, tree):
    node = "%s/Contracts/%s/Tenants/%s/Spaces/Production/Services" % (pod_path(), tenant, tenant)
    return ZK.import_tree(tree, node)

def pod_path():
    return '/Pods/%s' % POD

def post_body_template():
    with open('template.json', 'r') as file:
        return file.read()

def start_zk():
    global ZK
    if ZK is None:
        global POD
        if ENVIRONMENT == 'qa':
            ZK = zc.zk.ZooKeeper('internal-zookeeper-1213348105.us-east-1.elb.amazonaws.com:2181')
            POD = 'QA'
        elif ENVIRONMENT == 'prod':
            ZK = zc.zk.ZooKeeper('internal-Zookeeper-227174924.us-east-1.elb.amazonaws.com:2181')
            POD = 'Production'
        else:
            ZK = zc.zk.ZooKeeper('127.0.0.1:2181')
            POD = 'Default'

def admin_url(path):
    if ENVIRONMENT == 'prod':
        root = 'http://localhost:8085/admin'
    elif ENVIRONMENT == 'qa':
        root = 'https://admin-qa.lattice.local/admin'
    else:
        root = 'http://localhost:8085/admin'
    return '%s/%s' % (root, path)

def admin_login():
    global AD_TOKEN
    url = admin_url('adlogin')
    (resp, content) = HTTP.request(url, "POST", body=json.dumps({'Username': 'testuser1', 'Password': 'Lattice1'}),
                                headers={'Content-Type':'application/json', 'Accept':'application/json'} )
    AD_TOKEN = json.loads(content)['Token']
    logger.info('got AD token: ' + AD_TOKEN)

def admin_create_tenant(tenant, body):
    url = admin_url('/tenants/%s?contractId=%s' % (tenant, tenant))
    (resp, content) = HTTP.request(url, "POST", body=json.dumps(body),
                                   headers={'Authorization': AD_TOKEN, 'Content-Type':'application/json',
                                            'Accept':'application/json'} )
    logger.info(resp)

def admin_get_tenant(tenant):
    url = admin_url('/tenants/%s?contractId=%s' % (tenant, tenant))
    (resp, content) = HTTP.request(url, "GET", headers={'Authorization': AD_TOKEN, 'Accept':'application/json'} )
    return json.loads(content)

def admin_get_bootstrap_state(tenant):
    tenant_doc = admin_get_tenant(tenant)
    return tenant_doc['BootstrapState']['state']

def wait_bootstrap_ok(tenant):
    t1 = time.time()
    half_hour = 1800
    state = admin_get_bootstrap_state(tenant)
    logger.info('Bootstrap state is %s.' % state)
    while True:
        time.sleep(10)
        state = admin_get_bootstrap_state(tenant)
        logger.info('Bootstrap state is %s after %.2f sec.' % (state, time.time() - t1))
        if state == BOOTSTRAP_OK:
            return
        elif state == BOOTSTRAP_ERROR:
            raise Exception('Final bootstrap state is %s' % state)
        elif time.time() - t1 > half_hour:
            raise Exception('Did not finish bootstrap with in half hour.')


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Deploy wars to local tomcat')
    parser.add_argument('-t', dest='tenant', required=True, help='tenant to be upgraded')
    parser.add_argument('-e', dest='environment', choices=['dev', 'qa', 'prod'], required=True, help='environment')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    logger.setLevel(logging.INFO)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s [%(threadName)s] %(levelname)s %(name)s: %(message)s')
    ch.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    main()
