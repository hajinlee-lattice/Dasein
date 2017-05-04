//Initial load of the application    
var mainApp = angular.module('insightsApp', [
    'templates-main',
    'ui.router',
    'ui.bootstrap',

    'common.datacloud',
    'common.utilities.SessionTimeout',
    'pd.navigation.pagination',
    'angulartics', 
    'angulartics.mixpanel',

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
.service('AuthStore', function($q) {
    this.Authorization = '';
    
    this.get = function() {
        return this.Authorization;
    }

    this.set = function(value) {
        this.Authorization = value;
    }
})
.factory('authInterceptor', function ($q, AuthStore) {
    return {
        request: function(config) {
            config.headers = config.headers || {};
            
            if (AuthStore.get()) {
                config.headers.Authorization = AuthStore.get();
            }

            return config;
        },
        response: function(response) {
            return response || $q.when(response);
        },
        responseError: function(response) {
            if (response.status === 401) {
                $('.loading-spinner').hide();
            }

            return $q.reject(response)
        }
    };
});