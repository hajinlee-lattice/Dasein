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

                    if(vm.stored && vm.stored.crm_selection) {
                        PlaybookWizardStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                        PlaybookWizardStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                        PlaybookWizardStore.setDestinationAccountId(vm.stored.crm_selection.accountId);
                    }

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
                if(vm.stored && vm.stored.crm_selection && vm.stored.crm_selection.orgId === orgId) {
                    vm.checkValid(form, accountId, orgId, isRegistered);
                }
            }, 1);
        }

        // vm.calculateUnscoredCounts = function(form, segment, accountId, scoredNotNullCount){
        //     var template = {
        //         //lookupId: accountId, 
        //         account_restriction: {
        //             restriction: {
        //                 logicalRestriction: {
        //                     operator: "AND",
        //                     restrictions: []
        //                 }
        //             }
        //         },
        //         page_filter: {  
        //             num_rows: 10,
        //             row_offset: 0
        //         }
        //     };
        //     template.account_restriction.restriction.logicalRestriction.restrictions.push(segment.account_restriction.restriction);
        //     template.account_restriction.restriction.logicalRestriction.restrictions.push({
        //         bucketRestriction: {
        //             attr: 'Account.' + accountId,
        //             bkt: {
        //                 Cmp: 'IS_NULL',
        //                 Id: 1,
        //                 ignored: false,
        //                 Vals: []
        //             }
        //         }
        //     });
        //     // vm.totalCount = segment.accounts // small
        //     QueryStore.getEntitiesCounts(template).then(function(result) {
        //         PlaybookWizardStore.setValidation('crmselection', form.$valid);

        //         vm.loadingCoverageCounts = false;
        //         vm.notNullCount = result.Account + scoredNotNullCount;
        //         vm.nullCount = vm.totalCount - vm.notNullCount;
        //     });
        // }

        vm.checkValid = function(form, accountId, orgId, isRegistered) {
            vm.orgIsRegistered = isRegistered;
            vm.nullCount = null;
            vm.totalCount = null;

            $scope.excludeItemsWithoutSalesforceId = false;
            vm.setExcludeItems(false);
            PlaybookWizardStore.setValidation('crmselection', false);

            if(vm.stored && vm.stored.crm_selection) {
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
                    engineId = (vm.ratingEngine && vm.ratingEngine.id ? vm.ratingEngine.id : '');


                var segment = PlaybookWizardStore.getCurrentPlay().targetSegment,
                    segmentName = segment.name;

                vm.totalCount = segment.accounts;
                if(engineId) {
                    PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId], {
                        lookupId: accountId, 
                        restrictNullLookupId: true,
                        loadContactsCount: true,
                        loadContactsCountByBucket: true
                    }).then(function(result) {
                        PlaybookWizardStore.setValidation('crmselection', form.$valid);

                        vm.loadingCoverageCounts = false;
                        var scoredNotNullCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].accountCount;
                        var unscoredNotNullCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].unscoredAccountCount;
                        vm.notNullCount = scoredNotNullCount + unscoredNotNullCount;
                        vm.nullCount = vm.totalCount - vm.notNullCount;
                        //vm.calculateUnscoredCounts(form, segment, accountId, scoredNotNullCount);
                            // scoredNullCount = (totalScoredCount - scoredNonNullCount);
                    });
                } else {
                    vm.calculateUnscoredCounts(form, segment, accountId, 0);
                }
            } else {
                PlaybookWizardStore.setValidation('crmselection', form.$valid);                
            }
        }


    }
});