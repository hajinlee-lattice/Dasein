angular.module('mainApp.userManagement.controllers.UserManagementController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.userManagement.services.UserManagementService'
])

.controller('UserManagementController', function ($scope, BrowserStorageUtility, ResourceUtility, RightsUtility, WidgetService, WidgetConfigUtility, WidgetFrameworkService, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    if (BrowserStorageUtility.getClientSession() == null) { return; }

    $("#userManagementError").hide();
    $scope.errorMessage = ResourceUtility.getString("USER_MANAGEMENT_GET_USERS_ERROR");

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "userManagementScreenWidget"
    );

    if (screenWidgetConfig == null) {
        return;
    }

    var contentContainer = $('#userManagementContainer');
    UserManagementService.GetUsers().then(function(result) {
        $scope.loading = false;
        if (result.Success) {
            WidgetFrameworkService.CreateWidget({
                element:      contentContainer,
                widgetConfig: screenWidgetConfig,
                metadata:     null,
                data:         result.ResultObj,
                parentData:   null
            });
        }
    });
});