//Initial load of the application    
var mainApp = angular.module('insightsApp', [
    'templates-main',
    'ui.router',
    'ui.bootstrap',

    'common.datacloud',
    'common.utilities.SessionTimeout',
    'pd.navigation.pagination',

    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.modules.ServiceErrorModule',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.login.services.LoginService'
])
.config(function($httpProvider) {
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};    
    }

    $httpProvider.defaults.headers.get['If-Modified-Since'] = 'Mon, 26 Jul 1997 05:00:00 GMT';
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('authInterceptor');
})
.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
    return {
        request: function (config) {
            config.headers = config.headers || {};
            
            if (BrowserStorageUtility.getTokenDocument()) {
                config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
            }

            return config;
        },
        response: function (response) {
            if (response.status === 401) {
                // handle the case where the user is not authenticated
            }

            return response || $q.when(response);
        }
    };
});