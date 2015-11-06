angular.module('mainApp.setup.controllers.SetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.setup.controllers.ManageFieldsController',
    'mainApp.setup.services.MetadataService'
])

.controller('SetupController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, RightsUtility, NavUtility, WidgetConfigUtility, WidgetFrameworkService, WidgetService) {
    $scope.ResourceUtility = ResourceUtility;
    if (BrowserStorageUtility.getClientSession() == null) { return; }
});