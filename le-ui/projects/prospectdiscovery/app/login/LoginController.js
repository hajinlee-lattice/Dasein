angular
    .module('pd.login', [
        'mainApp.core.utilities.ServiceErrorUtility',
        'mainApp.core.utilities.BrowserStorageUtility',
        'mainApp.appCommon.utilities.UnderscoreUtility',
        'mainApp.appCommon.utilities.ResourceUtility',
        'mainApp.appCommon.utilities.StringUtility',
        'mainApp.core.services.SessionService'
    ])
    .controller('LoginController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, NavUtility, FeatureFlagService, ConfigService) {

    }
);
