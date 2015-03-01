angular.module('mainApp.models.controllers.ModelListController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelListController', function ($scope, ResourceUtility, BrowserStorageUtility, RightsUtility, WidgetConfigUtility, WidgetFrameworkService, GriotWidgetService, ModelService) {
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
            var contentContainer = $('#modelListContainer');

            var clientSession = BrowserStorageUtility.getClientSession();
            if (clientSession == null) { return; }

            var metadata = {mayEditModels: RightsUtility.mayEditModels(clientSession.availableRights)};

            WidgetFrameworkService.CreateWidget({
                element: contentContainer,
                widgetConfig: screenWidgetConfig,
                metadata: null,
                data: modelList,
                parentData: modelList
            });
        }

    });

});