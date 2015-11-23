angular.module('mainApp.core.controllers.MainViewController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.markets.controllers.TargetMarketController',
    'mainApp.insights.controllers.InsightsController',
    'mainApp.reports.controllers.ReportsController',
    'mainApp.admin.controllers.AdminController',
    'mainApp.config.services.ConfigService',
    'mainApp.config.controllers.ManageCredentialsController'
])

.controller('MainViewController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, NavUtility, FeatureFlagService, ConfigService) {
    if ($scope.isLoggedInWithTempPassword || $scope.isPasswordOlderThanNinetyDays) {
        // redirect to login portal
        window.open('/', '_self');
    }

    console.log('MainViewController init');
});