angular.module('lp.playbook.wizard.crmselection', [])
.component('crmSelection', {
    templateUrl: 'app/playbook/content/crmselection/crmselection.component.html',
    bindings: {
        orgs: '<'
    },
    controller: function(
        $scope, $state, $timeout, $stateParams,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, SfdcService
    ) {
        var vm = this            

        vm.$onInit = function() {
            vm.stored = PlaybookWizardStore.crmselection_form;
            // console.log(vm.orgs);
            PlaybookWizardStore.setValidation('crmselection', false);
            if($stateParams.play_name) {
                // PlaybookWizardStore.setValidation('settings', true);
                PlaybookWizardStore.getPlay($stateParams.play_name).then(function(play){
                    vm.savedSegment = play.crmselection;
                    vm.stored.crm_selection = play.crmselection;
                    if(play.crmselection) {
                        PlaybookWizardStore.setValidation('crmselection', true);
                    }
                });
            }
        }

        vm.checkValid = function(form) {
            PlaybookWizardStore.setValidation('crmselection', form.$valid);
            if(vm.stored.crm_selection) {
                PlaybookWizardStore.setSettings({
                    destinationOrgId: vm.stored.crm_selection.orgId,
                    destinationSysType: vm.stored.crm_selection.externalSystemType,
                    destinationAccountId: vm.stored.crm_selection.accountId
                });
            }
        }

    }
});