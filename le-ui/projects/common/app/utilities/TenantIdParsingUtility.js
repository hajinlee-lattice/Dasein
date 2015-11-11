angular.module('mainApp.appCommon.utilities.TenantIdParsingUtility', [])
.service('TenantIdParsingUtility', function() {
    this.getDataLoaderTenantNameFromTenantId = function(tenantId) {
        tenantNameArray = tenantId.split("\.");
        if (tenantNameArray.length == 3) {
            return tenantNameArray[1];
        }
        return tenantNameArray[0];
    };
});