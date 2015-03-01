angular.module('mainApp.models.controllers.ModelListController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelListController', function ($scope, ResourceUtility, WidgetConfigUtility, WidgetFrameworkService, GriotWidgetService, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    var widgetConfig = GriotWidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }
    
    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "modelListScreenWidget"
    );
    
    if (screenWidgetConfig == null) {
        return;
    }

    ModelService.GetAllModels().then(function(result) {
        $scope.loading = false;
        if (result != null && result.success === true) {
            var modelList = result.resultObj;
            if (modelList == null || modelList.length == 0) {
                $scope.showNoModels = true;
            } else {
                var contentContainer = $('#modelListContainer');
                WidgetFrameworkService.CreateWidget({
                    element: contentContainer,
                    widgetConfig: screenWidgetConfig,
                    metadata: null,
                    data: modelList,
                    parentData: modelList
                });
            }
        } else if (result.resultErrors === "NO TENANT FOUND") {
            $scope.showNoModels = true;
        }
    });

});