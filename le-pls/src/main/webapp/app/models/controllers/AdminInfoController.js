angular.module('mainApp.models.controllers.AdminInfoController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService'
])
.controller('AdminInfoController', function ($scope, $rootScope, $http, ResourceUtility, WidgetService, WidgetConfigUtility, WidgetFrameworkService) {
    $scope.ResourceUtility = ResourceUtility;

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "adminInfoScreenWidget"
    );

    if (screenWidgetConfig == null) {
        return;
    }

    var contentContainer = $('#adminInfoContainer');
    WidgetFrameworkService.CreateWidget({
        element:      contentContainer,
        widgetConfig: screenWidgetConfig,
        metadata:     null,
        data:         $scope.data,
        parentData:   null
    });

});