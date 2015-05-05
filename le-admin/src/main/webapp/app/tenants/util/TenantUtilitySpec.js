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
});