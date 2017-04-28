angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, Type, MatchingFields, AnalysisFields
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