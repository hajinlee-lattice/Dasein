angular.module('le.common.util.SessionUtility', [
    'le.common.util.BrowserStorageUtility'
])
.service('SessionUtility', function (BrowserStorageUtility) {

    this.ClearSession = function () {
        BrowserStorageUtility.clear(false);
        //ResourceUtility.clearResourceStrings();
        window.location.reload();
    };

    this.HandleResponseErrors = function (data, status) {
        if (status === 401) {
            this.ClearSession();
        }
    };
});