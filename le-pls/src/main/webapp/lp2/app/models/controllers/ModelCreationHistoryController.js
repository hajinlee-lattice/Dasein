angular.module('mainApp.models.controllers.ModelCreationHistoryController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.models.services.ModelService'
])

.controller('ModelCreationHistoryController', function ($scope, BrowserStorageUtility, ResourceUtility, RightsUtility, WidgetService, WidgetConfigUtility, 
		WidgetFrameworkService, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession == null) { return; }

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
            widgetConfig,
            "modelListCreationHistoryWidget"
    );
    if (screenWidgetConfig == null) {
        return;
    }
                    
    $scope.showNoModels = false;
    ModelService.GetAllModels(false).then(function(result) {
        $scope.loading = false;
        if (result != null && result.success === true) {
            var modelList = result.resultObj;
            var contentContainer = $('#modelCreationHistoryContainer');
            WidgetFrameworkService.CreateWidget({
                element: contentContainer,
                widgetConfig: screenWidgetConfig,
                metadata: null,
                data: modelList,
                parentData: modelList
            });
        } else if (result.resultErrors === "NO TENANT FOUND") {
            $scope.showNoModels = true;
        }
    });

});