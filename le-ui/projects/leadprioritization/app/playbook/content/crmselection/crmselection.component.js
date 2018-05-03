angular.module('lp.playbook.wizard.crmselection', [])
.component('crmSelection', {
    templateUrl: 'app/playbook/content/crmselection/crmselection.component.html',
    bindings: {
        orgs: '<'
    },
    controller: function(
        $scope, $state, $timeout, 
        ResourceUtility, BrowserStorageUtility, SfdcService, ModalStore
    ) {
        var vm = this;

        vm.$onInit = function() {
            console.log(vm.orgs);
        }

        vm.checkValid = function(form) {
            PlaybookWizardStore.setValidation('crmselection', form.$valid);
            if(vm.stored.segment_selection) {
                PlaybookWizardStore.setSettings({
                    segment: vm.stored.segment_selection
                });
            }
        }

    }
});