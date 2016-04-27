angular.module('mainApp.models.controllers.ModelDetailController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.controllers.ModelDetailController', 
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.appCommon.services.ThresholdExplorerService'
])
.controller('ModelDetailController', function ($compile, $stateParams, $scope, $rootScope, _, ResourceUtility, RightsUtility, BrowserStorageUtility, WidgetConfigUtility,
    NavUtility, WidgetFrameworkService, WidgetService, ModelService, ModelStore, TopPredictorService, ThresholdExplorerService, Model) {
    $scope.ResourceUtility = ResourceUtility;
    
    var modelId = $stateParams.modelId;

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
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

    var model = Model;
    model.ModelId = modelId;
    model.ChartData = TopPredictorService.FormatDataForTopPredictorChart(model);
    model.InternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, false, model);
    model.ExternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, true, model);
    model.TopSample = ModelService.FormatLeadSampleData(model.TopSample);
    var bottomLeads = ModelService.FormatLeadSampleData(model.BottomSample);
    model.BottomSample = filterHighScoresInBottomLeads(bottomLeads);
    model.TotalPredictors = model.InternalAttributes.totalAttributeValues + model.ExternalAttributes.totalAttributeValues;

    thresholdData = ThresholdExplorerService.PrepareData(model);
    model.ThresholdChartData = thresholdData.ChartData;
    model.ThresholdDecileData = thresholdData.DecileData;
    model.ThresholdLiftData = thresholdData.LiftData;

    model.SuppressedCategories = TopPredictorService.GetSuppressedCategories(model);

    angular.extend(ModelStore, {
        widgetConfig: screenWidgetConfig,
        metadata: null,
        data: model,
        parentData: model
    });


    $compile($('#ModelDetailsArea').html('<div data-model-details-widget></div>'))($scope);
    
    function filterHighScoresInBottomLeads(bottomLeads) {
        if (bottomLeads === null) {
            return bottomLeads;
        }
        var toReturn = _.reject(bottomLeads, function(bottomLead){ return bottomLead.Score > 10; });
        return toReturn;
    }
});