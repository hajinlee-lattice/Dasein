angular.module('lp.playbook.wizard.settings', [])
.controller('PlaybookWizardSettings', function(
    $state, $stateParams, $timeout, ResourceUtility, PlaybookWizardStore,
) {
    var vm = this;

    angular.extend(vm, {
        stored: PlaybookWizardStore.settings_form
    });

    vm.init = function() { };

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        PlaybookWizardStore.setValidation('settings', form.$valid);
    }

    vm.init();
});