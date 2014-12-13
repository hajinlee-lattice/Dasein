import os
import sys
import urllib2

baton = os.path.join("..", "..", "le-baton", "target", "le-baton-1.0.19-SNAPSHOT-shaded.jar")

os.system("java -jar " + baton + " -createPod --connectionString localhost:2181 --podId test")

os.system("java -jar " + baton + " -createTenant --connectionString localhost:2181 --podId test --contractId Lattice_Relaunch --tenantId Lattice_Relaunch --spaceId Production")

os.system("java -jar " + baton + " -loadDirectory --connectionString localhost:2181 --podId test --source stub\Interfaces --destination Interfaces --force")

request = urllib2.Request("http://localhost:8040/SetModelTags",
                          headers={"Content-Type": "application/json"},
                          data="""
{
"space": {"contractId": "Lattice_Relaunch", "tenantId": "Lattice_Relaunch", "spaceId": "Production"},
"tags": {"Q_PLS_ModelingLattice_Relaunch": {"active": 1, "test": 2}}
}""")
urllib2.urlopen(request);

request = urllib2.Request("http://localhost:8040/SetModelCombination",
                          headers={"Content-Type": "application/json"},
                          data="""
{
"space": {"contractId": "Lattice_Relaunch", "tenantId": "Lattice_Relaunch", "spaceId": "Production"},
"name": "realtime",
"combination": [{"filter": null, "model": "Q_PLS_ModelingLattice_Relaunch"}]
}""")
urllib2.urlopen(request);
