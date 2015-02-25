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
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.services.TopPredictorService'
])

.controller('ModelDetailController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, WidgetConfigUtility, 
    GriotNavUtility, WidgetFrameworkService, GriotWidgetService, ModelService, TopPredictorService) {
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
            model.ChartData = TopPredictorService.FormatDataForChart(model);
            model.InternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, false, model);
            model.ExternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, true, model);
            model.TopSample = ModelService.FormatLeadSampleData(model.TopSample);
            model.BottomSample = ModelService.FormatLeadSampleData(model.BottomSample);

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