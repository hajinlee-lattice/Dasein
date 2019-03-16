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
        vm.externalIntegrationEnabled = vm.featureflags.EnableExternalIntegration;

        this.showErrorApi = false;
        angular.extend(vm, {
            status: $stateParams.status
        });
        vm.isEmpty = (obj) => {
            for(var key in obj) {
                if(obj.hasOwnProperty(key))
                    return false;
            }
            return true;
        }
        vm.$onInit = function() {
            vm.nullCount = null;
            vm.loadingCoverageCounts = false;

            vm.excludeItemsWithoutSalesforceId = (PlaybookWizardStore.getCurrentPlay() &&
                                                PlaybookWizardStore.getCurrentPlay().launchHistory &&
                                                PlaybookWizardStore.getCurrentPlay().launchHistory.mostRecentLaunch &&
                                                PlaybookWizardStore.getCurrentPlay().launchHistory.mostRecentLaunch.excludeItemsWithoutSalesforceId ?
                                                PlaybookWizardStore.getCurrentPlay().launchHistory.mostRecentLaunch.excludeItemsWithoutSalesforceId : false);
            vm.setExcludeItems(vm.excludeItemsWithoutSalesforceId);

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
                        PlaybookWizardStore.setExternalAuthentication(vm.stored.crm_selection.externalAuthentication)
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

        vm.calculateUnscoredCounts = function(form, segment, accountId){
            if (!segment.account_restriction.restriction || !segment.account_restriction.restriction.length) {
                vm.loadingCoverageCounts = false;
                PlaybookWizardStore.setValidation('crmselection', form.$valid);
                return;
            }

            var template = {
                //lookupId: accountId,
                account_restriction: {
                    restriction: {
                        logicalRestriction: {
                            operator: "AND",
                            restrictions: []
                        }
                    }
                },
                page_filter: {
                    num_rows: 10,
                    row_offset: 0
                }
            };
            template.account_restriction.restriction.logicalRestriction.restrictions.push(segment.account_restriction.restriction);
            template.account_restriction.restriction.logicalRestriction.restrictions.push({
                bucketRestriction: {
                    attr: 'Account.' + accountId,
                    bkt: {
                        Cmp: 'IS_NOT_NULL',
                        Id: 1,
                        ignored: false,
                        Vals: []
                    }
                }
            });
            // vm.totalCount = segment.accounts // small
            QueryStore.getEntitiesCounts(template).then(function(result) {
                PlaybookWizardStore.setValidation('crmselection', form.$valid);

                vm.loadingCoverageCounts = false;
                vm.notNullCount = result.Account;
                vm.nullCount = vm.totalCount - vm.notNullCount;
            });
        }

        vm.checkValid = function(form, accountId, orgId, isRegistered) {
            console.log("sdfsdfsfsdfsdf");
            vm.orgIsRegistered = isRegistered;
            vm.nullCount = null;
            vm.totalCount = null;

            vm.setExcludeItems(vm.excludeItemsWithoutSalesforceId);
            PlaybookWizardStore.setValidation('crmselection', false);

            if (vm.stored && vm.stored.crm_selection) {
                PlaybookWizardStore.setDestinationOrgId(vm.stored.crm_selection.orgId);
                PlaybookWizardStore.setDestinationSysType(vm.stored.crm_selection.externalSystemType);
                PlaybookWizardStore.setDestinationAccountId(vm.stored.crm_selection.accountId);
                PlaybookWizardStore.setExternalAuthentication(vm.stored.crm_selection.externalAuthentication)
            }

            var engineId = (vm.ratingEngine && vm.ratingEngine.id ? vm.ratingEngine.id : ''),
                segment = PlaybookWizardStore.getCurrentPlay().targetSegment,
                segmentName = segment.name;

            if (accountId && vm.orgIsRegistered){

                vm.nullCount = null;
                vm.loadingCoverageCounts = true;

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
                        if(vm.isEmpty(result.errorMap)){
                            var scoredNotNullCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]] ? result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].accountCount : 0;
                            var unscoredNotNullCount = result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]] ? result.ratingModelsCoverageMap[Object.keys(result.ratingModelsCoverageMap)[0]].unscoredAccountCount : 0;
                            vm.notNullCount = scoredNotNullCount + unscoredNotNullCount;
                            vm.nullCount = vm.totalCount - vm.notNullCount;
                        }else{
                            vm.showErrorApi = true;
                        }
                    });
                } else {
                    vm.calculateUnscoredCounts(form, segment, accountId);
                }
            } else if (vm.externalIntegrationEnabled && vm.stored.crm_selection.externalSystemName == 'Marketo' && engineId) {
                // Find contacts without emails for play launches to Marketo
                PlaybookWizardService.getRatingSegmentCounts(segmentName, [engineId], {
                    lookupId: accountId,
                    restrictNullLookupId: false,
                    loadContactsCount: true,
                    loadContactsCountByBucket: false
                }).then(function(result) {
                    PlaybookWizardStore.setValidation('crmselection', form.$valid);
                    vm.loadingCoverageCounts = false;
                    if(vm.isEmpty(result.errorMap)){
                        var coverageCounts = result.ratingModelsCoverageMap ? result.ratingModelsCoverageMap[engineId] : {};
                        vm.nullCount = coverageCounts.contactCountWithoutEmail || 0;
                        vm.totalCount = coverageCounts.contactCount || 0;
                    }else{
                        vm.showErrorApi = true;
                    }
                });
            } else {
                PlaybookWizardStore.setValidation('crmselection', form.$valid);
            }
        }


    }
});
