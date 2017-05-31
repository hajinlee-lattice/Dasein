angular.module('lp.playbook.wizard.targets', [])
.controller('PlaybookWizardTargets', function(
    $state, $stateParams, $scope, ResourceUtility, PlaybookWizardStore, Type, MatchingFields, AnalysisFields
) {
    var vm = this;

    angular.extend(vm, {
    	importType: Type,
    	matchingFields: MatchingFields,
    	analysisFields: AnalysisFields
    });

    vm.init = function() {

    }

    vm.init();
});