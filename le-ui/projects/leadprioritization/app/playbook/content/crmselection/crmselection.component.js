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

            // console.log(vm.orgs);

            vm.nullCount = null;
            vm.loadingCoverageCounts = false;
            $scope.excludeItemsWithoutSalesforceId = false;

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
            QueryStore.setExcludeItems(excludeItemsWithoutSalesforceId);
        }

        vm.checkValid = function(form, accountId) {

            if(vm.stored.crm_selection) {
                // PlaybookWizardStore.setSettings({
                //     destinationOrgId: vm.stored.crm_selection.orgId,
                //     destinationSysType: vm.stored.crm_selection.externalSystemType,
                //     destinationAccountId: vm.stored.crm_selection.accountId
                // });

                // This will be refactored in M21
                QueryStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                QueryStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                QueryStore.setDestinationAccountId(vm.stored.crm_selection.accountId);
            }

            var accountId = accountId;
            if (accountId){

                vm.nullCount = null;
                vm.loadingCoverageCounts = true;

                var countsQuery = { 
                        freeFormTextSearch: vm.search || '',
                        restrictNotNullSalesforceId: true,
                        entityType: 'Account',
                        selectedBuckets: QueryStore.getBucketsToLaunch(),
                        lookupIdColumn: accountId
                    },
                    engineId = vm.ratingEngine.id;


                PlaybookWizardService.getTargetCount(engineId, countsQuery).then(function(result){
                    
                    console.log(result);

                    vm.totalCount = result;
                    PlaybookWizardService.getLookupCounts(engineId, accountId).then(function(result){

                        console.log(result);

                        PlaybookWizardStore.setValidation('crmselection', form.$valid);

                        vm.loadingCoverageCounts = false;
                        vm.nonNullCount = 0;
                        if(result.ratingIdLookupColumnPairsCoverageMap[accountId] && result.ratingIdLookupColumnPairsCoverageMap[accountId].accountCount){
                            vm.nonNullCount = result.ratingIdLookupColumnPairsCoverageMap[accountId].accountCount;
                        }
                        vm.nullCount = (vm.totalCount - vm.nonNullCount);

                    });


                });
            }
        }

    }
});