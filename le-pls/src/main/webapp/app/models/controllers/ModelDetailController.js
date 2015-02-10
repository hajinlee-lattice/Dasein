angular.module('mainApp.models.controllers.ModelDetailController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.appCommon.widgets.ThresholdExplorerWidget'
])

.controller('ModelDetailController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, WidgetConfigUtility, GriotNavUtility, WidgetFrameworkService, GriotWidgetService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var model = $scope.data;
    $scope.displayName = model.DisplayName;
    
    var widgetConfig = GriotWidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }
    
    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "modelDetailsScreenWidget"
    );
    
    if (screenWidgetConfig == null) {
        return;
    }

    var contentContainer = $('#modelDetailContainer');
    WidgetFrameworkService.CreateWidget({
        element: contentContainer,
        widgetConfig: screenWidgetConfig,
        metadata: null,
        data: model,
        parentData: model
    });
    
});