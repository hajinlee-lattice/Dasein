angular.module('lp.navigation.review', [
    'mainApp.appCommon.utilities.StringUtility',
    'lp.models.review',
    'mainApp.setup.modals.UpdateFieldsModal'
])
.controller('RefineModelSummaryController', function($scope, $stateParams, StringUtility, Model, ReviewData, ModelReviewStore, UpdateFieldsModal) {
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
        conversionRateAfter: getConversionRate(Model.ModelDetails.TotalConversions, Model.ModelDetails.TotalLeads)
    });

    vm.createModelClicked = function() {
       UpdateFieldsModal.show(false, vm.modelId, null, ModelReviewStore.GetDataRules(vm.modelId));
    };

    var rowRulesChanged = [];
    ReviewData.dataRules.forEach(function(dataRule) {
        var storedDataRules = ModelReviewStore.GetDataRules(vm.modelId);
        storedDataRules.forEach(function(storedDataRule) {
            if (storedDataRule.name == dataRule.name && dataRule.name in ReviewData.ruleNameToRowRuleResults
                && dataRule.enabled != storedDataRule.enabled) {
                rowRulesChanged.push(storedDataRule);
            }
        });
    })

    rowRulesChanged.forEach(function(rowRuleChanged) {
        updateDisplay(ReviewData.ruleNameToRowRuleResults[rowRuleChanged.name], rowRuleChanged);
    });

    $scope.$on('RowWarningToggled', function(event, warning, dataRule) {
        updateDisplay(warning, dataRule);
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
