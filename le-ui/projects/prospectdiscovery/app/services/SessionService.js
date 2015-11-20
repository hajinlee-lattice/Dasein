angular.module('mainApp.core.services.SessionService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('SessionService', function (BrowserStorageUtility, ResourceUtility) {
    
    this.ClearSession = function () {
        alert('CLEAR SESSION'); return;
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