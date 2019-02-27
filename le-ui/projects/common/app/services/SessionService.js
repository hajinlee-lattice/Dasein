angular.module('mainApp.core.services.SessionService', [
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('SessionService', function (BrowserStorageUtility, ResourceUtility) {
    
    this.ClearSession = function () {
        BrowserStorageUtility.clear(false);
        ResourceUtility.clearResourceStrings();
        window.location.reload();
    };
    
    this.HandleResponseErrors = function (data, status) {
        if (status === 401) {
            this.ClearSession();
        }
    };
});