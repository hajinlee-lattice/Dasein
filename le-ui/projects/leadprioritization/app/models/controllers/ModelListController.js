angular.module('mainApp.models.controllers.ModelListController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelListController', function ($scope, ResourceUtility, WidgetConfigUtility, WidgetFrameworkService, WidgetService, ModelService, ModelStore) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
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

    $scope.showNoModels = false;
    $scope.length = 0;
    function getModels(use_cache) {
        ModelStore.getModels(use_cache).then(function(result) {
            $scope.loading = false;
            if (result && result.length > 0) {
                var modelList = result;
                if (modelList == null || modelList.length === 0) {
                    $scope.showNoModels = true;
                } else {
                    $scope.length = modelList.length;
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
    }

    getModels(true);
    getModels();

});