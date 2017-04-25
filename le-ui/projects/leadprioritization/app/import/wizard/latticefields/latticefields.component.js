angular.module('lp.import.wizard.latticefields', [])
.controller('ImportWizardLatticeFields', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore, AccountMatchingFields, AnalysisFields
) {
    var vm = this;

    angular.extend(vm, {
    	accountMatching: AccountMatchingFields,
    	analysisFields: AnalysisFields
    });

    vm.init = function() {

    }

    vm.init();
});