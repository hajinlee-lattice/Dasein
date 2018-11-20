angular.module('mainApp.models.controllers.ModelDetailController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.appCommon.services.ThresholdExplorerService',
    'mainApp.create.csvBulkUpload',
    'mainApp.models.remodel',
    'mainApp.models.review',
    'lp.navigation.review'
])
.controller('ModelDetailController', function ($compile, $stateParams, $scope, $rootScope, _, ResourceUtility, RightsUtility, BrowserStorageUtility,
    NavUtility, ModelService, ModelStore, TopPredictorService, ThresholdExplorerService, Model, IsPmml, RatingEngine) {
    $scope.ResourceUtility = ResourceUtility;
    if(Model !== null){

        var modelId = $stateParams.modelId;
        var model = Model;
        
        model.IsPmml = IsPmml;
        model.ModelId = modelId;
        model.ChartData = TopPredictorService.FormatDataForTopPredictorChart(model);
        if (model.ChartData) {
            model.InternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, false, model);
            model.ExternalAttributes = TopPredictorService.GetNumberOfAttributesByCategory(model.ChartData.children, true, model);
            combineInternalAndExternalAttributesDups(model.InternalAttributes, model.ExternalAttributes);
            model.TotalPredictors = model.InternalAttributes.totalAttributeValues + model.ExternalAttributes.totalAttributeValues;
        }

        // UI BAND-AID for DP-2854 here

        model.TopSample = ModelService.FormatLeadSampleData(model.TopSample);
        var bottomLeads = ModelService.FormatLeadSampleData(model.BottomSample);
        model.BottomSample = filterHighScoresInBottomLeads(bottomLeads);

        var thresholdData = ThresholdExplorerService.PrepareData(model);
        model.ThresholdChartData = thresholdData.ChartData;
        model.ThresholdDecileData = thresholdData.DecileData;
        model.ThresholdLiftData = thresholdData.LiftData;

        model.SuppressedCategories = TopPredictorService.GetSuppressedCategories(model);

        angular.extend(ModelStore, {
            metadata: null,
            data: model,
            parentData: model
        });

    } else {
        $scope.RatingEngine = RatingEngine;
    }

    $compile($('#ModelDetailsArea').html('<div data-model-details-widget></div>'))($scope);

    /**
    ATTENTION: this function is due to we have issues where certain predictor categories show up on both left and right side
    of the donut chart on the model details page. This is caused by the 'Tag' attribute is set to internal where it should
    be external (DP-2854). This is an UI BAND-AID until the back end change is in page.
    */
    function combineInternalAndExternalAttributesDups(internalAttr, externalAttr) {
        var internalCategories = internalAttr.categories;
        var externalCategories = externalAttr.categories;

        for (var j = 0; j < externalCategories.length; j++) {
            for (var i = 0; i < internalCategories.length; i++) {
                if (externalCategories[j].name == internalCategories[i].name) {
                    externalAttr.total += internalCategories[i].count;
                    internalAttr.total -= internalCategories[i].count;

                    externalCategories[j].count += internalCategories[i].count;

                    internalCategories.splice(i, 1);
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