angular.module('common.wizard', [
    'common.wizard.progress',
    'common.wizard.controls'
])
.controller('ImportWizard', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility, WizardHeaderTitle, WizardContainerId
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        title: WizardHeaderTitle || '',
        container_id: WizardContainerId || ''
    });

    vm.init = function() {
        vm.title = WizardHeaderTitle || name;
    }

    vm.init();
});