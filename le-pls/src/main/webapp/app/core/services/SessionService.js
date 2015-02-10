angular.module('mainApp.core.services.SessionService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('SessionService', function (BrowserStorageUtility, ResourceUtility) {
    
    this.ClearSession = function () {
        BrowserStorageUtility.clear(false);
        ResourceUtility.clearResourceStrings();
        window.location.reload();
    };
});