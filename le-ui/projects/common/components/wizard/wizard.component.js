angular.module('common.wizard', [
    'common.wizard.progress',
    'common.wizard.controls'
])
.controller('ImportWizard', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility, WizardHeaderTitle
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        title: WizardHeaderTitle || ''
    });

    vm.init = function() {
        vm.title = WizardHeaderTitle || name;
    }

    vm.init();
});