angular.module('mainApp.userManagement.controllers.UserManagementController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.userManagement.services.UserManagementService'
])

.controller('UserManagementController', function ($scope, ResourceUtility, GriotWidgetService, WidgetConfigUtility, WidgetFrameworkService, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $("#userManagementError").hide();
    $scope.errorMessage = ResourceUtility.getString("USER_MANAGEMENT_GET_USERS_ERROR");

    var widgetConfig = GriotWidgetService.GetApplicationWidgetConfig();
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
    WidgetFrameworkService.CreateWidget({
        element: contentContainer,
        widgetConfig: screenWidgetConfig,
        metadata: null,
        data: null,
        parentData: null
    });
});