angular.module('mainApp.models.controllers.ModelDetailController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.appCommon.widgets.ThresholdExplorerWidget',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.models.services.ModelService'
])

.controller('ModelDetailController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, WidgetConfigUtility, GriotNavUtility, WidgetFrameworkService, GriotWidgetService, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var modelId = $scope.data.Id;
    $scope.displayName = $scope.data.DisplayName;

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

    ModelService.GetModelById(modelId).then(function(result) {
        if (result != null && result.success === true) {
            var model = result.resultObj;

            var contentContainer = $('#modelDetailContainer');
            WidgetFrameworkService.CreateWidget({
                element: contentContainer,
                widgetConfig: screenWidgetConfig,
                metadata: null,
                data: model,
                parentData: model
            });
        }

    });



});