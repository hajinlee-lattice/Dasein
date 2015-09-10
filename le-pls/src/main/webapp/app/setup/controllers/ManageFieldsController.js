angular.module('mainApp.setup.controllers.ManageFieldsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService'
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

    $scope.topNavClicked = function ($event, nodeName) {
        if ($event != null) {
            $event.preventDefault();
        }

        $timeout(function () {
            $("#setup .setup-sidebar li[node-name='" + nodeName + "']").trigger('click');
        });
    };
})

.directive('manageFields', function () {
    return {
        templateUrl: 'app/setup/views/ManageFieldsView.html'
    };
});