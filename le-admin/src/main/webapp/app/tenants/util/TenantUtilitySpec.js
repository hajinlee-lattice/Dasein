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
            tenantUtility.constructTenantRegistration([component], "tenantId", "contractId", {}, featureFlags);

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
            [component], "tenantId", "contractId", {CustomerSpaceInfo: CustomerSpaceInfo}, featureFlags);

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
                    "sandboxSfdcOrgId": null,
                    "product": "LPA",
                    "topology": "MARKETO"
                }, "featureFlags": "{\"Dante\":false}"
            },
            "CustomerSpace": {"contractId": "TopologyDev", "tenantId": "Marketo", "spaceId": "Production"},
            "BootstrapState": {"state": "INITIAL", "desiredVersion": -1, "installedVersion": -1, "errorMessage": null}
        };

        var record = tenantUtility.convertTenantRecordToGridData(document);

        expect(record.TenantId).toEqual("Marketo");
        expect(record.ContractId).toEqual("TopologyDev");
        expect(record.Status).toEqual("New");
        expect(record.Product).toEqual("LPA");
        expect(record.CreatedDate).toEqual(new Date("2015-05-05T19:38:02.618Z"));
        expect(record.LastModifiedDate).toEqual(new Date("2015-05-05T19:38:02.643Z"));
    });
});

