angular.module('common.wizard', [
    'common.wizard.progress',
    'common.wizard.controls',
    'common.wizard.header'
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
        vm.title = WizardHeaderTitle || name; //set WizardHeaderTitle to false to use the header component
    }
 
    vm.init();
});