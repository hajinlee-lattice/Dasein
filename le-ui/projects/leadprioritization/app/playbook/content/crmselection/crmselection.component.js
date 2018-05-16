angular.module('lp.playbook.wizard.crmselection', [])
.component('crmSelection', {
    templateUrl: 'app/playbook/content/crmselection/crmselection.component.html',
    bindings: {
        orgs: '<'
    },
    controller: function(
        $scope, $state, $timeout, $stateParams,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService, SfdcService, QueryStore
    ) {
        var vm = this;       

        vm.$onInit = function() {

            vm.totalCount = 0;
            vm.nonNullCount = 0;
            vm.nullCount = 0;

            vm.stored = PlaybookWizardStore.crmselection_form;
            vm.ratingEngine = PlaybookWizardStore.getSavedRating();

            console.log(vm.orgs);
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

        vm.checkValid = function(form, accountId) {

            vm.loadingCoverageCounts = true;

            PlaybookWizardStore.setValidation('crmselection', form.$valid);
            if(vm.stored.crm_selection) {
                // PlaybookWizardStore.setSettings({
                //     destinationOrgId: vm.stored.crm_selection.orgId,
                //     destinationSysType: vm.stored.crm_selection.externalSystemType,
                //     destinationAccountId: vm.stored.crm_selection.accountId
                // });

                QueryStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                QueryStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                QueryStore.setDestinationAccountId(vm.stored.crm_selection.accountId);
            }


            var accountId = accountId;

            PlaybookWizardService.getRatingsCounts([vm.ratingEngine.id], false).then(function(result){
                var engineId = vm.ratingEngine.id;
                vm.totalCount = result.ratingEngineIdCoverageMap[engineId].accountCount;

                PlaybookWizardService.getLookupCounts(vm.ratingEngine.id, accountId).then(function(result){
                    vm.loadingCoverageCounts = false;
                    vm.nonNullCount = result.ratingIdLookupColumnPairsCoverageMap[accountId].accountCount;

                    vm.nullCount = (vm.totalCount - vm.nonNullCount);

                });

            });
            

        }

    }
});