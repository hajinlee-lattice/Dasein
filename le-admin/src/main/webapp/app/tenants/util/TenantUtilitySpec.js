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

    it('should have an exciteText function', function () {
        var displayName = tenantUtility.getStatusDisplayName("OK");
        expect(tenantUtility.getStatusTemplate(displayName)).toContain(displayName);
    });
});