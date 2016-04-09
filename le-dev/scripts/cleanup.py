import httplib
import json
from kazoo.client import KazooClient

print 'Clean up PLS_MultiTenantDB and GlobalAuth'
headers = {"MagicAuthentication":"Security through obscurity!", "Content-type": "application/json", "Accept": "application/json"}
conn = httplib.HTTPConnection("localhost", 8081)
conn.request("GET", "/pls/admin/tenants", "", headers)
response = conn.getresponse()
print response.status, response.reason
tenants = json.loads(response.read())

for tenant in tenants:
	tenantId = tenant['Identifier']
	tenantName = tenant['DisplayName']
	if 'LETest' in tenantName:
		print 'Found a testing tenant %s, deleting it using REST call ...' % tenantName
		conn.request("DELETE", "/pls/admin/tenants/%s" % tenantId, "", headers)
		response = conn.getresponse()
		print response.status, response.reason, response.read()

conn.request("GET", "/pls/admin/tenants", "", headers)
response = conn.getresponse()
print 'Remaining tenants in PLS_MultiTenantDB:'
data = json.loads(response.read())
print json.dumps(data, sort_keys=True, indent=2, separators=(',', ': '))


print 'Clean up contracts in ZK:'
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
contracts = zk.get_children("/Pods/Default/Contracts")

for contract in contracts:
	if 'LETest' in contract:
		print 'Found a testing tenant %s, deleting it from ZK ...' % contract
		zk.delete("/Pods/Default/Contracts/" + contract, recursive=True)

print 'Remaining contracts in ZK:'
data = zk.get_children("/Pods/Default/Contracts")
print json.dumps(data, sort_keys=True, indent=2, separators=(',', ': '))

zk.stop()

print 'Clean up folders in HDFS:'
webhdfs = httplib.HTTPConnection("localhost", 50070)
for tenant in tenants:
	tenantId = tenant['Identifier']
	tenantName = tenant['DisplayName']
	print 'Clean up for tenant %s ...' % tenantName
	if 'LETest' in tenantId:
		path = '/webhdfs/v1/user/s-analytics/customers/%s?user.name=yarn&doas=yarn&op=DELETE&&recursive=true' % tenantId
		conn.request("DELETE", path, "", headers)
		response = conn.getresponse()
		print response.status, response.reason, response.read()

		path = '/webhdfs/v1/user/s-analytics/customers/%s?user.name=yarn&doas=yarn&op=DELETE&&recursive=true' % tenantName
		conn.request("DELETE", path, "", headers)
		response = conn.getresponse()
		print response.status, response.reason, response.read()

		path = '/webhdfs/v1/Pods/Default/Contracts/%s?user.name=yarn&doas=yarn&op=DELETE&&recursive=true' % tenantName
		conn.request("DELETE", path, "", headers)
		response = conn.getresponse()
		print response.status, response.reason, response.read()
