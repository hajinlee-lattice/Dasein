angular.module('lp.import.wizard', [
    'lp.import.wizard.controls',
    'lp.import.wizard.progress',
    'lp.import.wizard.accountids',
    'lp.import.wizard.thirdpartyids',
    'lp.import.wizard.latticefields',
    'lp.import.wizard.customfields',
    'lp.import.wizard.jobstatus'
])
.controller('ImportWizard', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility, ImportWizardStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        title: ''
    });

    vm.init = function() {
        var name = $state.current.name.split('.')[3];
        vm.title = name;
    }

    vm.init();
});