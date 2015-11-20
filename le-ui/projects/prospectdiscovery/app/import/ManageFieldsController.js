angular.module('mainApp.setup.controllers.ManageFieldsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility'
])

.controller('ManageFieldsController', function ($scope, $timeout, ResourceUtility, BrowserStorageUtility, RightsUtility, NavUtility, WidgetConfigUtility, WidgetFrameworkService, WidgetService) {
    $scope.ResourceUtility = ResourceUtility;

    if (BrowserStorageUtility.getClientSession() == null) { return; }

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "manageFieldsScreenWidget"
    );

    if (screenWidgetConfig == null) {
        return;
    }

    var contentContainer = $('#manageFieldsContainer');
    WidgetFrameworkService.CreateWidget({
        element:      contentContainer,
        widgetConfig: screenWidgetConfig,
        metadata:     null,
        data:         null,
        parentData:   null
    });

})

.directive('manageFields', function () {
    return {
        templateUrl: 'app/setup/views/ManageFieldsView.html'
    };
});