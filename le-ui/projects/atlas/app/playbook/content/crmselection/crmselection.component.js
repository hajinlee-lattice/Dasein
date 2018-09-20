angular.module('lp.playbook.wizard.crmselection', [])
.component('crmSelection', {
    templateUrl: 'app/playbook/content/crmselection/crmselection.component.html',
    bindings: {
        play: '<',
        featureflags: '<',
        orgs: '<'
    },
    controller: function(
        $scope, $state, $timeout, $stateParams,
        ResourceUtility, BrowserStorageUtility, PlaybookWizardStore, PlaybookWizardService, SfdcService, QueryStore
    ) {
        var vm = this;
        vm.showMAPSystems = vm.featureflags.EnableCdl;

        vm.$onInit = function() {
            vm.nullCount = null;
            vm.loadingCoverageCounts = false;
            $scope.excludeItemsWithoutSalesforceId = false;
            vm.setExcludeItems(false);

            PlaybookWizardStore.setValidation('crmselection', false);

            if(vm.orgs){
                vm.stored = PlaybookWizardStore.crmselection_form;
                vm.ratingEngine = PlaybookWizardStore.getSavedRating();

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

        }

        vm.setExcludeItems = function(excludeItemsWithoutSalesforceId) {
            PlaybookWizardStore.setExcludeItems(excludeItemsWithoutSalesforceId);
        }

        vm.checkValid = function(form, accountId, isRegistered) {
            vm.orgIsRegistered = isRegistered;

            $scope.excludeItemsWithoutSalesforceId = false;
            vm.setExcludeItems(false);
            PlaybookWizardStore.setValidation('crmselection', false);

            if(vm.stored.crm_selection) {
                PlaybookWizardStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                PlaybookWizardStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                PlaybookWizardStore.setDestinationAccountId(vm.stored.crm_selection.accountId);
            }

            var accountId = accountId;
            if (accountId && isRegistered){

                vm.nullCount = null;
                vm.loadingCoverageCounts = true;

                var allCountsQuery = { 
                        freeFormTextSearch: vm.search || '',
                        entityType: 'Account',
                        selectedBuckets: PlaybookWizardStore.getBucketsToLaunch(),
                    },
                    engineId = vm.ratingEngine.id;


                PlaybookWizardService.getTargetCount(engineId, allCountsQuery).then(function(result){
                    vm.totalCount = result;

                    var accountIdCountQuery = { 
                        freeFormTextSearch: vm.search || '',
                        restrictNotNullSalesforceId: true,
                        entityType: 'Account',
                        selectedBuckets: PlaybookWizardStore.getBucketsToLaunch(),
                        lookupIdColumn: accountId
                    };
                    PlaybookWizardService.getTargetCount(engineId, accountIdCountQuery).then(function(result){
                        PlaybookWizardStore.setValidation('crmselection', form.$valid);

                        vm.loadingCoverageCounts = false;
                        vm.nonNullCount = result;
                        vm.nullCount = (vm.totalCount - vm.nonNullCount);

                    });


                });
            } else {
                PlaybookWizardStore.setValidation('crmselection', form.$valid);                
            }
        }


    }
});