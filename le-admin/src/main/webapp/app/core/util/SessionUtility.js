var app = angular.module("app.core.util.SessionUtility", [
    'ui.router',
    'le.common.util.BrowserStorageUtility'
]);

app.service('SessionUtility', function($state, BrowserStorageUtility){
    this.handleAJAXError = function (err, status) {
        if (status === 401) {
            BrowserStorageUtility.clear();
            $state.go('LOGIN');
        }
    };
});


