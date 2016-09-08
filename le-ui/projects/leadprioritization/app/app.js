//Initial load of the application
var mainApp = angular.module('mainApp', [
    'templates-main',
    //'ngAnimate',
    'ngRoute',
    'ui.router',
    'ui.bootstrap',
    'oc.lazyLoad',

    //'lp.header',
    'common.modules',
    'pd.navigation',
    'lp.jobs',
    'lp.apiconsole',
    'lp.models.list',
    'lp.models.review',
    'lp.create.import',
    'lp.enrichment.leadenrichment',
    'lp.sfdc.credentials',
    'lp.managefields'
])
.controller('MainController', function (
    $scope, $state, $rootScope, BrowserStorageUtility, SessionTimeoutUtility, TimestampIntervalUtility
) {
    var previousSession = BrowserStorageUtility.getClientSession();
    var loginDocument = BrowserStorageUtility.getLoginDocument();

    SessionTimeoutUtility.init();

    if (loginDocument && mustUserChangePassword(loginDocument)) {
        window.open("/login", "_self");
    } else if (previousSession != null && !SessionTimeoutUtility.hasSessionTimedOut()) {
        //SessionTimeoutUtility.refreshPreviousSession(previousSession.Tenant);
    } else {
        window.open("/login", "_self");
    }

    function mustUserChangePassword(loginDocument) {
        return loginDocument.MustChangePassword || TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(loginDocument.PasswordLastModified);
    }
})
// adds Authorization token to $http requests to access API
.factory('authInterceptor', function ($rootScope, $q, BrowserStorageUtility) {
    return {
        request: function(config) {
            config.headers = config.headers || {};
            
            if (config.headers.Authorization == null && BrowserStorageUtility.getTokenDocument()) {
                config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
                config.headers.TenantId = $rootScope.tenantId;
            }
            
            return config;
        },
        response: function(response) {
            return response || $q.when(response);
        }
    };
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('authInterceptor');
})
// prevent $http caching of API results
.config(function($httpProvider) {
    //initialize get if not there
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};    
    }

    //disable IE ajax request caching
    $httpProvider.defaults.headers.get['If-Modified-Since'] = 'Mon, 26 Jul 1997 05:00:00 GMT';
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
})
.config(function($animateProvider){
    $animateProvider.classNameFilter(/ngAnimate/);
})
// add escape filter to angular {{ foobar | escape }}
.filter('escape', function() {
    return window.escape;
});