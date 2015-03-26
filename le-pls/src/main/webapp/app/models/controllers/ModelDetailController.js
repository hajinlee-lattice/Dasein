angular.module('mainApp.models.controllers.ModelDetailController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.GriotWidgetService',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.appCommon.services.ThresholdExplorerService'
])

.controller('ModelDetailController', function ($scope, $rootScope, _, ResourceUtility, RightsUtility, BrowserStorageUtility, WidgetConfigUtility,
    GriotNavUtility, WidgetFrameworkService, GriotWidgetService, ModelService, TopPredictorService, ThresholdExplorerService) {
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
            model.ModelId = modelId;
            model.ChartData = TopPredictorService.FormatDataForTopPredictorChart(model);
            model.InternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, false, model);
            model.ExternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, true, model);
            model.TopSample = ModelService.FormatLeadSampleData(model.TopSample);
            model.BottomSample = ModelService.FormatLeadSampleData(model.BottomSample);
            model.TotalAttributeValues = model.InternalAttributes.totalAttributeValues + model.ExternalAttributes.totalAttributeValues;

            thresholdData = ThresholdExplorerService.PrepareData(model);
            model.ThresholdChartData = thresholdData.ChartData;
            model.ThresholdDecileData = thresholdData.DecileData;

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