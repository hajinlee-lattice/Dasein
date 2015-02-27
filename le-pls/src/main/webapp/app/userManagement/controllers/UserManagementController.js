angular.module('mainApp.userManagement.controllers.UserManagementController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.userManagement.services.UserManagementService'
])

.controller('UserManagementController', function ($scope, BrowserStorageUtility, ResourceUtility, RightsUtility, GriotWidgetService, WidgetConfigUtility, WidgetFrameworkService, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    console.log(clientSession);
    if (clientSession == null) { return; }

    var metadata = {CanAddUser: RightsUtility.canAddUser(clientSession.availableRights)};

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
    var tenantId = clientSession.Tenant.Identifier;
    UserManagementService.GetUsers(tenantId).then(function(result) {
        WidgetFrameworkService.CreateWidget({
            element:      contentContainer,
            widgetConfig: screenWidgetConfig,
            metadata:     metadata,
            data:         null,
            parentData:   null
        });
    });
});