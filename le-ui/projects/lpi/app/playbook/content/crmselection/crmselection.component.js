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

        angular.extend(vm, {
            status: $stateParams.status
        });

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
                    var play = PlaybookWizardStore.getCurrentPlay(),
                        crmselection = (play && 
                                        play.launchHistory && 
                                        play.launchHistory.mostRecentLaunch && 
                                        play.launchHistory.mostRecentLaunch.destinationOrgId ? 
                                        vm.orgs.find(function(org) { return org.orgId === play.launchHistory.mostRecentLaunch.destinationOrgId}) : '');

                    vm.savedSegment = crmselection;
                    vm.stored.crm_selection = crmselection;

                    PlaybookWizardStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                    PlaybookWizardStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                    PlaybookWizardStore.setDestinationAccountId(vm.stored.crm_selection.accountId);

                    if(crmselection) {
                        PlaybookWizardStore.setValidation('crmselection', true);
                    }
                }
            }

        }

        vm.setExcludeItems = function(excludeItemsWithoutSalesforceId) {
            PlaybookWizardStore.setExcludeItems(excludeItemsWithoutSalesforceId);
        }

        vm.checkValidDelay = function(form, accountId, orgId, isRegistered) {
            $timeout(function() {
                if(vm.stored.crm_selection.orgId === orgId) {
                    vm.checkValid(form, accountId, orgId, isRegistered);
                }
            }, 1);
        }

        vm.checkValid = function(form, accountId, orgId, isRegistered) {
            vm.orgIsRegistered = isRegistered;
            vm.nullCount = null;
            vm.totalCount = null;

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


                var segmentName = PlaybookWizardStore.getCurrentPlay().targetSegment.name;
                PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId]).then(function(result) {
                    vm.totalCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].accountCount; // small
                    PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId], {
                    lookupId: accountId, 
                    restrictNullLookupId: true
                    }).then(function(result) {
                        PlaybookWizardStore.setValidation('crmselection', form.$valid);

                        vm.loadingCoverageCounts = false;
                        vm.nonNullCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].accountCount; // big
                        vm.nullCount = (vm.totalCount - vm.nonNullCount);
                    });
                });

                // PlaybookWizardService.getTargetCount(engineId, allCountsQuery).then(function(result) {
                //     vm.totalCount = result;

                //     var accountIdCountQuery = { 
                //         freeFormTextSearch: vm.search || '',
                //         restrictNotNullSalesforceId: true,
                //         entityType: 'Account',
                //         selectedBuckets: PlaybookWizardStore.getBucketsToLaunch(),
                //         lookupIdColumn: accountId
                //     };
                //     PlaybookWizardService.getTargetCount(engineId, accountIdCountQuery).then(function(result){
                //         PlaybookWizardStore.setValidation('crmselection', form.$valid);

                //         vm.loadingCoverageCounts = false;
                //         vm.nonNullCount = result;
                //         vm.nullCount = (vm.totalCount - vm.nonNullCount);
                //     });
                // });
            } else {
                PlaybookWizardStore.setValidation('crmselection', form.$valid);                
            }
        }


    }
});