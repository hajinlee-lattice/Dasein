'use strict';

describe('TenantUtility tests', function (){
    var tenantUtility;

    beforeEach(function (){

        module("app.tenants.util.TenantUtility");

        inject(['TenantUtility', function (TenantUtility) {
                tenantUtility = TenantUtility;
            }
        ]);
    });

    it('should render status correctly', function () {
        var displayName = tenantUtility.getStatusDisplayName("OK");
        expect(tenantUtility.getStatusTemplate(displayName)).toContain(displayName);
        expect(tenantUtility.getStatusTemplate(displayName)).toContain("text-success");

        displayName = tenantUtility.getStatusDisplayName("ERROR");
        expect(tenantUtility.getStatusTemplate(displayName)).toContain(displayName);
        expect(tenantUtility.getStatusTemplate(displayName)).toContain("text-danger");

        displayName = tenantUtility.getStatusDisplayName("INITIAL");
        expect(tenantUtility.getStatusTemplate(displayName)).toContain(displayName);
        expect(tenantUtility.getStatusTemplate(displayName)).toContain("text-warning");

        displayName = tenantUtility.getStatusDisplayName("whatever");
        expect(tenantUtility.getStatusTemplate(displayName)).toContain(displayName);
        expect(tenantUtility.getStatusTemplate(displayName)).toContain("text-muted");
    });

    it('should render status correctly', function () {
        var component = {
            Component: "Component",
            RootPath: "/",
            Nodes: [
                {Node: "Config1", Data: '["ysong@lattice-engines.com"]', Metadata: {Type: "array"}},
                {Node: "Config2", Data: "A string"}
            ]
        };

        var featureFlags = {
            Feature1: true,
            Feature2: false
        };

        //========================================
        // use empty infos
        //========================================
        var tenantReg =
            tenantUtility.constructTenantRegistration([component], "tenantId", "contractId", {}, {},  featureFlags);

        expect(tenantReg.hasOwnProperty("CustomerSpaceInfo")).toBe(true);
        expect(tenantReg.hasOwnProperty("TenantInfo")).toBe(true);
        expect(tenantReg.hasOwnProperty("ContractInfo")).toBe(true);

        var configDirs = tenantReg.ConfigDirectories;
        expect(configDirs.length).toEqual(1);

        var spaceInfo = tenantReg.CustomerSpaceInfo;

        var parsedFlags = JSON.parse(spaceInfo.featureFlags);
        expect(parsedFlags.Feature1).toBe(true);
        expect(parsedFlags.Feature2).toBe(false);

        //========================================
        // provide spaceInfo
        //========================================
        var CustomerSpaceInfo = {
            properties: {
                displayName: "LPA_Tenant",
                description: "A LPA solution",
                product: "LPA 2.0",
                topology: "MARKETO"
            },
            featureFlags: ""
        };

        tenantReg = tenantUtility.constructTenantRegistration(
            [component], "tenantId", "contractId", {CustomerSpaceInfo: CustomerSpaceInfo}, {}, featureFlags);

        expect(tenantReg.hasOwnProperty("CustomerSpaceInfo")).toBe(true);
        expect(tenantReg.hasOwnProperty("TenantInfo")).toBe(true);
        expect(tenantReg.hasOwnProperty("ContractInfo")).toBe(true);

        configDirs = tenantReg.ConfigDirectories;
        expect(configDirs.length).toEqual(1);

        spaceInfo = tenantReg.CustomerSpaceInfo;
        parsedFlags = JSON.parse(spaceInfo.featureFlags);
        expect(parsedFlags.Feature1).toBe(true);
        expect(parsedFlags.Feature2).toBe(false);
    });


    it('should validate tenant Id', function () {
        //========================================
        // not empty
        //========================================
        var validation = tenantUtility.validateTenantId(null);
        expect(validation.valid).toBe(false);
        validation = tenantUtility.validateTenantId("");
        expect(validation.valid).toBe(false);

        //========================================
        // no space
        //========================================
        validation = tenantUtility.validateTenantId(" ");
        expect(validation.valid).toBe(false);
        validation = tenantUtility.validateTenantId("dafads fdasfsd");
        expect(validation.valid).toBe(false);
        validation = tenantUtility.validateTenantId("dafads\tfdasfsd");
        expect(validation.valid).toBe(false);

        //========================================
        // no periods
        //========================================
        validation = tenantUtility.validateTenantId("dafads.fdasfsd");
        expect(validation.valid).toBe(false);

        //========================================
        // no slashes
        //========================================
        validation = tenantUtility.validateTenantId("dafads/fdasfsd");
        expect(validation.valid).toBe(false);
        validation = tenantUtility.validateTenantId("dafads\\fdasfsd");
        expect(validation.valid).toBe(false);
    });

    it('should parse tenant document to table record', function () {
        var document = {
            "ContractInfo": {"properties": {"displayName": null, "description": null}},
            "TenantInfo": {
                "properties": {
                    "displayName": "Test LPA tenant",
                    "description": "A LPA tenant under the contract TopologyDev",
                    "created": 1430854682618,
                    "lastModified": 1430854682643
                }
            },
            "CustomerSpaceInfo": {
                "properties": {
                    "displayName": "LPA_Marketo",
                    "description": "A LPA solution for Marketo in TopologyDev",
                    "sfdcOrgId": null,
                    "sandboxSfdcOrgId": null
                }, "featureFlags": "{\"Dante\":false}"
            },
            "CustomerSpace": {"contractId": "TopologyDev", "tenantId": "Marketo", "spaceId": "Production"},
            "SpaceConfiguration": {"Product": "Lead Prioritization", "Topology": "Marketo"},
            "BootstrapState": {"state": "INITIAL", "desiredVersion": -1, "installedVersion": -1, "errorMessage": null}
        };

        var record = tenantUtility.convertTenantRecordToGridData(document);

        expect(record.TenantId).toEqual("Marketo");
        expect(record.ContractId).toEqual("TopologyDev");
        expect(record.Status).toEqual("New");
        expect(record.Product).toEqual("Lead Prioritization");
        expect(record.CreatedDate).toEqual(new Date("2015-05-05T19:38:02.618Z"));
        expect(record.LastModifiedDate).toEqual(new Date("2015-05-05T19:38:02.643Z"));
    });

    it('should parse errormessage correctly', function () {
        var message = "[LE-ysong-ubuntu] An intented exception for the purpose of testing.:: java.lang.RuntimeException: An intented exception for the purpose of testing.\n\tat com.latticeengines.admin.tenant.batonadapter.dante.DanteInstaller.installCore(DanteInstaller.java:17)\n\tat com.latticeengines.baton.exposed.camille.LatticeComponentInstaller.install(LatticeComponentInstaller.java:43)\n";
        expect(tenantUtility.parseBootstrapErrorMsg(message)).toBe("[LE-ysong-ubuntu] An intented exception for the purpose of testing.");
    });

    it('should find the correct node by path', function () {
        var component = {
            Component: "comp1",
            Nodes: [
                {Node: "node1", Children: [
                    {Node:"child1", Data:true, Metadata: {Type:"boolean"} },
                    {Node:"child2", Data:2, Metadata: {Type:"number"} }
                ]},
                {Node: "node2", Data: "string"}
            ]
        };
        var node = tenantUtility.findNodeByPath(component, "/node1/child2");
        expect(node.Data).toBe(2);
    });

    it('should find the derived parameter', function () {
        var component1 = {
            Component: "comp1",
            Nodes: [
                {Node: "node1", Children: [
                    {Node:"child1", Data:2, Metadata: {Type:"number"} }
                ]}
            ]
        };
        var component2 = {
            Component: "comp2",
            Nodes: [
                {Node: "node1", Children: [
                    {Node:"child1", Data:"string" }
                ]}
            ]
        };
        var component3 = {
            Component: "comp3",
            Nodes: [
                {Node: "node1", Data: false, Metadata: {Type:"boolean"} }
            ]
        };
        var components = [component1, component2, component3];
        var parameter = {
            Component: "comp1",
            NodePath: "/node1/child1"
        };
        expect(tenantUtility.getDerivedParameter(components, parameter)).toBe(2);

        parameter = {
            Component: "comp3",
            NodePath: "/node1/child1"
        };
        expect(tenantUtility.getDerivedParameter(components, parameter)).toBe(null);

        parameter = {
            Component: "comp1",
            NodePath: "/node1/child2"
        };
        expect(tenantUtility.getDerivedParameter(components, parameter)).toBe(null);
    });

    it('should evaluate expressions with values', function () {
        var expression = "{0} + {1} / {0}"
        var values = [1, 2];
        var result = tenantUtility.evalExpressionWithValues(expression, values);
        expect(result).toBe(3);

        expression = "\"//{0}/{1}/{2}/{0}\"";
        values = ["root", "dir", "folder"];
        result = tenantUtility.evalExpressionWithValues(expression, values);
        expect(result).toBe("//root/dir/folder/root");
    });

    it('should calculate derived values', function () {
        var component1 = {
            Component: "comp1",
            Nodes: [
                {Node: "node1", Children: [
                    {Node:"child1", Data:2, Metadata: {Type:"number"} }
                ]}
            ]
        };
        var component2 = {
            Component: "comp2",
            Nodes: [
                {Node: "node1", Data: "root"}
            ]
        };
        var component3 = {
            Component: "comp3",
            Nodes: [
                {Node: "node1", Children: [
                    {Node:"child1", Data:"dir" }
                ]}
            ]
        };
        var components = [component1, component2, component3];
        var par1 = {
            Component: "comp2",
            NodePath: "/node1"
        };
        var par2 = {
            Component: "comp3",
            NodePath: "/node1/child1"
        };
        var derivation= {
            Expression: "\"//{0}/{1}\"",
            Parameters: [par1, par2]
        };
        expect(tenantUtility.calcDerivation(components, derivation)).toBe("//root/dir");
    });

});

