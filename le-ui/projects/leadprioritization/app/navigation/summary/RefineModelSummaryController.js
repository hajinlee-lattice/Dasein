angular.module('lp.navigation.review', [
    'mainApp.appCommon.utilities.StringUtility',
    'lp.models.review',
    'mainApp.setup.modals.UpdateFieldsModal',
    'mainApp.models.modals.RefineModelThresholdModal'
])
.controller('RefineModelSummaryController', function($scope, $stateParams, StringUtility, Model, ReviewData,
    ModelReviewService, ModelReviewStore, UpdateFieldsModal, RefineModelThresholdModal) {
    var vm = this;

    angular.extend(vm, {
        modelId: $stateParams.modelId,
        totalRecords: StringUtility.AddCommas(Model.ModelDetails.TotalLeads),
        totalRecordsAfter: Model.ModelDetails.TotalLeads,
        totalRecordsAfterDisplay: StringUtility.AddCommas(Model.ModelDetails.TotalLeads),
        successEvents: StringUtility.AddCommas(Model.ModelDetails.TotalConversions),
        successEventsAfter: Model.ModelDetails.TotalConversions,
        successEventsAfterDisplay: StringUtility.AddCommas(Model.ModelDetails.TotalConversions),
        conversionRate: getConversionRate(Model.ModelDetails.TotalConversions, Model.ModelDetails.TotalLeads),
        conversionRateAfter: getConversionRate(Model.ModelDetails.TotalConversions, Model.ModelDetails.TotalLeads),
        eventTableName: Model.EventTableProvenance.EventTableName
    });

    vm.createModelClicked = function() {
        if (vm.totalRecordsAfter < 7000 || vm.successEventsAfter < 150 || vm.conversionRateAfter > 10) {
            RefineModelThresholdModal.show(vm.totalRecordsAfter, vm.successEventsAfter, vm.conversionRateAfter);
        } else {
            UpdateFieldsModal.show(false, false, vm.modelId, null, Model.ModelDetails.DisplayName, ModelReviewStore.GetDataRules(vm.modelId));
        }
    };

    ModelReviewService.GetModelReviewData(vm.modelId, vm.eventTableName).then(function(result) {
        var oldReviewData = result.Result;
        oldReviewData.dataRules.forEach(function(dataRule) {
            var storedDataRules = ModelReviewStore.GetDataRules(vm.modelId);
            storedDataRules.forEach(function(storedDataRule) {
                if (storedDataRule.name == dataRule.name && dataRule.name in ReviewData.ruleNameToRowRuleResults
                    && dataRule.enabled != storedDataRule.enabled) {
                    updateDisplay(ReviewData.ruleNameToRowRuleResults[storedDataRule.name], storedDataRule);
                }
            });
        });
    });

    $scope.$on('RowWarningToggled', function(event, warning, dataRule) {
        updateDisplay(warning, dataRule);
    });
    $scope.$on('ShowCreateModelPopup', function(event) {
        UpdateFieldsModal.show(false, false, vm.modelId, null, Model.ModelDetails.DisplayName, ModelReviewStore.GetDataRules(vm.modelId));
    });

    function updateDisplay(warning, dataRule) {
        if (dataRule.enabled) {
            vm.totalRecordsAfter -= warning.flaggedItemCount;
            vm.successEventsAfter -= warning.numPositiveEvents;
        } else {
            vm.totalRecordsAfter += warning.flaggedItemCount;
            vm.successEventsAfter += warning.numPositiveEvents;
        }
        vm.totalRecordsAfterDisplay = StringUtility.AddCommas(vm.totalRecordsAfter);
        vm.successEventsAfterDisplay = StringUtility.AddCommas(vm.successEventsAfter);
        vm.conversionRateAfter = getConversionRate(vm.successEventsAfter, vm.totalRecordsAfter);
    }

    function getConversionRate(successEvents, totalRecords) {
        return (successEvents / totalRecords * 100).toFixed(1);
    }
});
