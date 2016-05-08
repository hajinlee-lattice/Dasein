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

    // UI BAND-AID for DP-2854 here
    combineInternalAndExternalAttributesDups(model.InternalAttributes.categories, model.ExternalAttributes.categories);

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

    /**
    ATTENTION: this function is due to we have issues where certain predictor categories show up on both left and right side
    of the donut chart on the model details page. This is caused by the 'Tag' attribute is set to internal where it should
    be external (DP-2854). This is an UI BAND-AID until the back end change is in page.
    */
    function combineInternalAndExternalAttributesDups(internalAttr, externalAttr) {
        for (var j = 0; j < externalAttr.length; j++) {
            for (var i = 0; i < internalAttr.length; i++) {
                if (externalAttr[j].name == internalAttr[i].name) {
                    externalAttr[j].count += internalAttr[i].count;
                    internalAttr.splice(i, 1);
                }
            }
        }
    }

    function filterHighScoresInBottomLeads(bottomLeads) {
        if (bottomLeads === null) {
            return bottomLeads;
        }
        var toReturn = _.reject(bottomLeads, function(bottomLead){ return bottomLead.Score > 10; });
        return toReturn;
    }
});