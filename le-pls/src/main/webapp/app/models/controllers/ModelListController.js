angular.module('mainApp.models.controllers.ModelListController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.models.services.GriotModelService',
    'mainApp.appCommon.widgets.ModelListTileWidget',
])
.controller('ModelListController', function ($scope, ResourceUtility, BrowserStorageUtility, WidgetConfigUtility, WidgetFrameworkService, GriotWidgetService, GriotModelService) {
    $scope.ResourceUtility = ResourceUtility;
    
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

    GriotModelService.GetAllModels().then(function(result) {
        if (result != null && result.success === true) {
            var modelList = result.resultObj;
            var contentContainer = $('#modelListContainer');
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