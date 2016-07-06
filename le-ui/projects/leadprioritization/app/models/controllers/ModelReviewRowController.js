angular.module('mainApp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService'
])
.controller('ModelReviewRowController', function($scope, _, ResourceUtility, ModelService, ReviewData) {
    var vm = this;
        ruleNameToDataRules = {};

    for (var i in ReviewData.dataRules) {
        ruleNameToDataRules[ReviewData.dataRules[i].name] = ReviewData.dataRules[i];
    }

    angular.extend(vm, {
        ruleNameToDataRules: ruleNameToDataRules,
        rowWarnings : _.values(ReviewData.ruleNameToRowRuleResults)
    });
})
.controller('ModelReviewColumnController', function($scope, _, ResourceUtility, ModelService, ReviewData) {
    var vm = this;
        ruleNameToDataRules = {};

    for (var i in ReviewData.dataRules) {
        ruleNameToDataRules[ReviewData.dataRules[i].name] = ReviewData.dataRules[i];
    }

    angular.extend(vm, {
        ruleNameToDataRules: ruleNameToDataRules,
        columnWarnings : _.values(ReviewData.ruleNameToColumnRuleResults)
    });
});
