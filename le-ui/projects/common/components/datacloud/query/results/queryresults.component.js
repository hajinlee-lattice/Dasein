angular.module('common.datacloud.query.results', [
    'common.utilities.browserstorage'
])
.controller('QueryResultsCtrl', function(
    $q, $scope, $state, $stateParams, $filter, $rootScope, $timeout, 
    BrowserStorageUtility, NumberUtility, QueryStore, QueryService, 
    SegmentService, SegmentStore, LookupStore, Config, Accounts,
    AccountsCoverage, Contacts, PlaybookWizardStore, PlaybookWizardService, NoSFIdsCount, orgs
) {
    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        section: $stateParams.section,
        page: $stateParams.pageTitle,
        accounts: Accounts,
        counts: {},
        contacts: Contacts,
        noSFCount: NoSFIdsCount,
        loading: true,
        saving: false,
        saved: false,
        segment: QueryStore.getSegment(),
        accountRestriction: QueryStore.getAccountRestriction(),
        contactRestriction: QueryStore.getContactRestriction(),
        current: 1,
        pagesize: 10,
        showAccountPagination: false,
        showContactPagination: false,
        showErrorApi: false,
        search: '',
        searchOptions: {
            updateOn: 'default blur',
            debounce: 1500
        },
        accountsCoverage: AccountsCoverage ? AccountsCoverage.ratingModelsCoverageMap : null,

        excludeNonSalesForce: false,
        sortType: 'CompanyName',
        sortDesc: false,
        sortReverse: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false,
        config: Config,
        currentTargetTab: 'Accounts',
        tmpAccounts: [],
        tmpContacts: [],
        recommendationCounts: {},
        launchUnscored: PlaybookWizardStore.getLaunchUnscored(),
        bypassBuckets: false,
        destinationOrgId: PlaybookWizardStore.getDestinationOrgId(),
        orgs: orgs,
        exportFolder: ''
    });
    vm.isEmpty = (obj) => {
        for(var key in obj) {
            if(obj.hasOwnProperty(key))
                return false;
        }
        return true;
    }
    vm.init = function() {
        // Set Counts for Segment and PLay Targets
        if (vm.section === 'segment.analysis') {
            vm.counts = QueryStore.getCounts();
        } else {
            vm.showErrorApi = !vm.isEmpty(AccountsCoverage.errorMap);
            vm.selectedBuckets = [];
            if (vm.section === 'wizard.targets') {
                // Now instead we get sum of scored and unscored accounts
                var unscoredAccountCount = vm.accountsCoverage ? vm.accountsCoverage.unscoredAccountCount : 0;
                var accountCount = vm.accountsCoverage ? vm.accountsCoverage.accountCount : 0;
                var numAccounts = unscoredAccountCount + accountCount;

                vm.unscoredAccounts = {
                    total: unscoredAccountCount,
                    percentage: NumberUtility.MakePercentage(unscoredAccountCount, (unscoredAccountCount + accountCount), '%', 1)
                }

                //setting defaults
                var bucketsToLaunch = null;
                vm.launchUnscored = PlaybookWizardStore.currentPlay.ratingEngine ? false : true;

                if (PlaybookWizardStore.currentPlay.launchHistory.mostRecentLaunch != null){
                    bucketsToLaunch = PlaybookWizardStore.currentPlay.launchHistory.mostRecentLaunch.bucketsToLaunch;
                    vm.launchUnscored = PlaybookWizardStore.currentPlay.launchHistory.mostRecentLaunch.launchUnscored;
                    vm.topNCount = PlaybookWizardStore.currentPlay.launchHistory.mostRecentLaunch.topNCount;
                    vm.topNClicked = vm.topNCount ? true : false;
                }

                vm.launchUnscoredClick();
                vm.makeRecommendationCounts();

                // Create array (vm.selectedBuckets) of bucket names (e.g. ["A", "B", "C"]) 
                // to be used when launching play, and assign percentage to the bucket for display purposes
                if(vm.accountsCoverage){
                    vm.accountsCoverage.bucketCoverageCounts.forEach(function(bucket){
                        if(bucketsToLaunch == null) {
                            vm.selectedBuckets.push(bucket.bucket);
                        }
                        else if(bucketsToLaunch.length && bucketsToLaunch.indexOf(bucket.bucket) !== -1) {
                            vm.selectedBuckets.push(bucket.bucket);
                        }


                        // Use this if you want to round up to nearest integer percentage
                        // If you do use this, use this in the view HTML ({{ ::bucket.percentage }}%)
                        // result is (1%) for 0.3%
                        // bucket.percentage = Math.ceil((bucket.count / numAccounts) * 100);

                        // Use this if you want more precise percentage in the display
                        // If you do use this, use this in the view HTML ({{ ::bucket.percentage | percentage: 1 }})
                        // result is (0.3%) for 0.3%
                        bucket.percentage = bucket.count / numAccounts;               
                    });
                    PlaybookWizardStore.setBucketsToLaunch(vm.selectedBuckets);
                }
                vm.getExportFolder(vm.destinationOrgId, vm.orgs);
            } else if (vm.section === 'dashboard.targets') {
                PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){
                    if(data && data.ratingEngine && data.ratingEngine.bucketMetadata) {
                        var buckets = data.ratingEngine.bucketMetadata;
                        buckets.forEach(function(bucket){
                            vm.selectedBuckets.push(bucket.bucket_name);
                        });
                    }
                });

            }
        }
    }

    function updatePage() {
        vm.loading = true;
        var offset = (vm.current - 1) * vm.pagesize;

        if (vm.section === 'segment.analysis') {
            
            // My Data or Segment Account & Contacts pages
            var dataQuery = {
                "free_form_text_search": vm.search,
                "account_restriction": vm.accountRestriction,
                "contact_restriction": vm.contactRestriction,
                "preexisting_segment_name": $stateParams.segment,
                "restrict_with_sfdcid": vm.excludeNonSalesForce,
                "lookups": [
                    {
                        "attribute": {
                            "entity": "Account",
                            "attribute": "AccountId"
                        }
                    },
                    {
                        "attribute": {
                            "entity": "Account",
                            "attribute": "CompanyName"
                        }
                    },
                    {
                        "attribute": {
                            "entity": "Account",
                            "attribute": "City"
                        }
                    },
                    {
                        "attribute": {
                            "entity": "Account",
                            "attribute": "Website"
                        }
                    }, 
                    { 
                        "attribute": { 
                            "entity": "Account", 
                            "attribute": "State" 
                        } 
                    }, 
                    { 
                        "attribute": { 
                            "entity": "Account", 
                            "attribute": "Country" 
                        } 
                    } 
                ],
                "main_entity": "Account",
                "page_filter": {
                    "num_rows": vm.pagesize,
                    "row_offset": offset
                },
                "sort": {
                    "attributes": [{
                        "attribute": vm.sortType,
                        "entity": "Account"
                    }]
                }
            };

            if (vm.page === 'Accounts'){
                QueryStore.setAccounts(dataQuery).then(function(response) {
                    vm.accounts = response.data;
                    vm.loading = false;
                });
            } else if (vm.page === 'Contacts'){
                QueryStore.setContacts(dataQuery).then(function(response) {
                    vm.contacts = response.data;
                    vm.loading = false;
                });
            }

            var query = {
               "free_form_text_search": vm.search,
               "account_restriction": vm.accountRestriction,
               "contact_restriction": vm.contactRestriction,
               "restrict_without_sfdcid": false,
               "page_filter":{  
                    "num_rows": vm.pagesize,
                    "row_offset": offset
               }
            };

            QueryStore.getEntitiesCounts(query).then(function(data){ 
                vm.counts[vm.page.toLowerCase()].value = data[vm.page == 'Contacts' ? 'Contact' : 'Account'];
                vm.counts[vm.page.toLowerCase()].loading = false;

                if (vm.page == 'Accounts' || vm.page === 'Playbook') {
                    vm.showAccountPagination =  data['Account'] > 10;
                    vm.showContactPagination = false;
                }

                if (vm.page == 'Contacts') {
                    vm.showAccountPagination = false;
                    vm.showContactPagination = data['Contact'] > 10;
                }
            });
        } else {

            // Targets page for create Play flow
            var dataQuery = { 
                    free_form_text_search: vm.search || '',
                    restrictNotNullSalesforceId: false,
                    entityType: 'Account',
                    bucketFieldName: 'ScoreBucket',
                    maximum: vm.pagesize,
                    offset: offset,
                    sortBy: vm.sortType,
                    descending: vm.sortDesc,
                    selectedBuckets: vm.selectedBuckets
                };

            PlaybookWizardStore.setValidation('targets', false);
            PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){
            
                // Get play rating engine and create array object literal for getting the counts.
                var engineId = (data.ratingEngine && data.ratingEngine.id ? data.ratingEngine.id : ''),
                    engineIdObject = (engineId ? [{id: engineId}] : []);
                    
                vm.hasModel = (engineId ? true : false);

                // Get Account Data
                if(engineId) {
                    PlaybookWizardService.getTargetData(engineId, dataQuery).then(function(results) { 
                        PlaybookWizardStore.setTargetData(results.data);
                        vm.accounts = PlaybookWizardStore.getTargetData();
                    });
                } else {
                    var accountQuery = {
                            "free_form_text_search": vm.search || '',
                            "preexisting_segment_name": data.targetSegment.name,
                            "lookups": [{
                                    "attribute": {
                                        "entity": "Account",
                                        "attribute": "AccountId"
                                    }
                                },{
                                    "attribute": {
                                        "entity": "Account",
                                        "attribute": "LDC_Name"
                                    }
                                },{
                                    "attribute": {
                                        "entity": "Account",
                                        "attribute": "CompanyName"
                                    }
                                }, {
                                    "attribute": {
                                        "entity": "Account",
                                        "attribute": "Website"
                                    }
                                }
                            ],
                            "page_filter": {
                                "num_rows": vm.pagesize,
                                "row_offset": offset
                            }
                        };
                    
                    PlaybookWizardService.getAccountsData(accountQuery).then(function(results) { 
                        PlaybookWizardStore.setTargetData(results.data);
                        vm.accounts = PlaybookWizardStore.getTargetData();
                        vm.bypassBuckets = true;
                    });
                }

                // Get Account Counts for Pagination
                if (!vm.search) {
                    vm.noBuckets = [{
                        bucket: 'A',
                        count: 0,
                    },{
                        bucket: 'B',
                        count: 0,
                    },{
                        bucket: 'C',
                        count: 0,
                    },{
                        bucket: 'D',
                        count: 0,
                    },{
                        bucket: 'E',
                        count: 0,
                    },{
                        bucket: 'F',
                        count: 0,
                    }];
                    if(engineIdObject.length) {
                        PlaybookWizardStore.getRatingsCounts(engineIdObject).then(function(data){

                            var accountsCoverage = (data.ratingEngineIdCoverageMap && data.ratingEngineIdCoverageMap[engineId] ? data.ratingEngineIdCoverageMap[engineId] : {bucketCoverageCounts: []});
                            
                            var filteredAccountsCoverage = accountsCoverage.bucketCoverageCounts.filter(function (bucket) {
                                  return vm.selectedBuckets.indexOf(bucket.bucket) >= 0; 
                                });

                            var calculateCountsFromFiltered = function(array) {
                                var accounts = 0,
                                    count;
                                for (var i = 0; i < array.length; i++) {
                                    accounts += filteredAccountsCoverage[i].count;
                                }
                                count = accounts;
                                return count;
                            };

                            if (vm.section === 'create.targets' || vm.section === 'dashboard.targets') {
                                if(!vm.bypassBuckets) {
                                    vm.counts = { 
                                        accounts: { 
                                            value: calculateCountsFromFiltered(filteredAccountsCoverage) 
                                        },
                                        contacts: {
                                            value: (vm.accountsCoverage && vm.accountsCoverage.contactCount ? vm.accountsCoverage.contactCount : 0)
                                        }
                                    };
                                }
                            } else {
                                let accountValue = calculateCountsFromFiltered(filteredAccountsCoverage);
                                let contactsValue = vm.accountsCoverage ? vm.accountsCoverage.contactCount : 0;
                                vm.counts = { 
                                    accounts: { 
                                        value: accountValue
                                    },
                                    contacts: {
                                        value: contactsValue
                                    }
                                };
                            }
                            
                            if(vm.counts.accounts.value > 10){
                                vm.showAccountPagination = true;
                                vm.showContactPagination = false;
                            }

                            if (vm.section == 'wizard.targets' && vm.selectedBuckets.length == 0) {
                                vm.showAccountPagination = false;
                            }

                            //only sets topNCount here if coming in for the first time
                            // if(vm.topNCount == null){
                            //     vm.topNCount = vm.recommendationCounts.selected;
                            //     PlaybookWizardStore.setValidation('targets', (vm.topNCount > 0) || vm.launchUnscored);
                            // }
                            vm.updateTopNCount();
                        });
                    } else { // no rating engine
                        var countsQuery = {
                            "preexisting_segment_name": data.targetSegment.name,
                        };

                        PlaybookWizardStore.getAccountsCount(countsQuery).then(function(data) {
                            vm.counts = {
                                accounts: {
                                    value: data
                                }
                            };
                        });
                    }
                } else if (vm.search) { 
                    if(engineId) {
                        var countsQuery = { 
                                freeFormTextSearch: vm.search || '',
                                restrictNotNullSalesforceId: vm.excludeNonSalesForce,
                                entityType: 'Account',
                                selectedBuckets: vm.selectedBuckets
                            };

                        PlaybookWizardService.getTargetCount(engineId, countsQuery).then(function(data) {
                            vm.counts.accounts.value = data;
                            
                            vm.showAccountPagination = vm.counts.accounts.value > 10;
                            vm.showContactPagination = false;

                            if (vm.section == 'wizard.targets' && vm.selectedBuckets.length == 0) {
                                vm.counts.accounts.value = 0;
                                vm.showAccountPagination = false;
                            }
                        });
                    } else {
                        var countsQuery = {
                            "free_form_text_search": vm.search,
                            "preexisting_segment_name": data.targetSegment.name,
                        };

                        PlaybookWizardStore.getAccountsCount(countsQuery).then(function(data) {
                            vm.counts.accounts.value = data;
                            vm.showAccountPagination = vm.counts.accounts.value > 10;
                        });
                    }
                }

                PlaybookWizardStore.setBucketsToLaunch(vm.selectedBuckets);

            });

            vm.loading = false;

        }

        vm.checkSaveButtonState();
    };

    vm.setValidation = function(){
        if (vm.showError == true || vm.recommendationCounts.launched <= 0){
            PlaybookWizardStore.setValidation('targets', false);
        }
        else{
            PlaybookWizardStore.setValidation('targets', true);
        }
    }

    vm.updateTopNCount = function() {
        vm.topNCount = Math.floor(vm.topNCount);
        //sync issue with vm.counts.accounts using vm.recommendationCounts.selected instead
        // vm.maxTargetValue = vm.recommendationCounts.selected;
        // if (vm.topNCount <= vm.maxTargetValue && vm.topNCount > 0) {
        //     vm.showError = false;
        //     PlaybookWizardStore.setValidation('targets', true);
        //     vm.topNClicked ? PlaybookWizardStore.setTopNCount(vm.topNCount) : PlaybookWizardStore.setTopNCount(null);
        // } else {
        //     vm.showError = vm.maxTargetValue == 0 ? false : true;
        //     PlaybookWizardStore.setValidation('targets', false || (vm.recommendationCounts.selected && !vm.topNClicked));
        // }
        if (vm.topNClicked && (vm.topNCount <= 0 || vm.topNCount == null)){
            vm.showError = true;
        } else {
            vm.showError = false;
            vm.topNClicked ? PlaybookWizardStore.setTopNCount(vm.topNCount) : PlaybookWizardStore.setTopNCount(null);
        }
    }

    vm.topNInputClick = function($event) {
        vm.topNClicked = true;
        $event.target.select();
        vm.updateTopNCount();
        vm.makeRecommendationCounts();
    }

    vm.excludeNonSalesForceCheckbox = function(excludeAccounts){
        excludeAccounts = !excludeAccounts;

        if (excludeAccounts){
            vm.excludeNonSalesForce = true;
        } else {
            vm.excludeNonSalesForce = false;
        }

        updatePage();
        
    };

    vm.bucketClick = function(bucket) {
        var index = vm.selectedBuckets.indexOf(bucket.bucket);

        if (index > -1) {
            vm.selectedBuckets.splice( index, 1 );
        } else {
            vm.selectedBuckets.push( bucket.bucket );
        }

        PlaybookWizardStore.setBucketsToLaunch(vm.selectedBuckets);
        //reset topNcount on bucket click, issue with sync and faster update speed
        updatePage();
        vm.makeRecommendationCounts();
    }

    vm.getExportFolder = function(orgId, orgs) {
        var folder = '';
        if(orgId === 'Lattice_S3') {
            folder = orgs.find(function(org) {
                return org.orgId === orgId;
            });
        }
        if(folder.exportFolder) {
            vm.exportFolder = folder.exportFolder;
        }
    }

    vm.launchUnscoredClick = function() {
        $timeout(function() {
            PlaybookWizardStore.setLaunchUnscored(vm.launchUnscored);
            updatePage();
            vm.makeRecommendationCounts();
        });
    }

    vm.showNoResultsText = function(accounts, contacts) {
        var accounts = accounts || {},
            contacts = contacts || {};
        switch (vm.page) {
            case 'Accounts': 
                return accounts.length === 0;

            case 'Playbook': 
                return accounts.length === 0;
            
            case 'Contacts': 
                return contacts.length === 0;
            
            default: 
                return accounts.length === 0 && contacts.length === 0;
        }
    }

    var prevQuery = vm.search;
    vm.submitQuery = function() {
        
        vm.loading = true;
        vm.current = 1;

        if ((vm.search && prevQuery) && (vm.search.toUpperCase() === prevQuery.toUpperCase())) {
            return;
        }
        updatePage();
    };

    vm.refineQuery = function() {
        return vm.inModel() 
            ? 'home.model.analysis.explorer.builder'
            : 'home.segment.explorer.builder'
    };

    $rootScope.$on('clearSegment', function(e){
        vm.clearSearch();
    });

    vm.clearSearch = function() {
        vm.search = '';
        vm.current = 1;

        vm.submitQuery();
    };


    vm.sort = function(columnName) {
        vm.sortType = columnName;
        vm.sortDesc = !vm.sortDesc;
        vm.current = 1;

        updatePage();
    };

    vm.checkSaveButtonState = function(){
        var oldVal = QueryStore.getDefaultRestrictions(),
            newAccountVal = JSON.stringify(QueryStore.getAccountRestriction()),
            newContactVal = JSON.stringify(QueryStore.getContactRestriction()),
            newVal = newAccountVal + newContactVal;

        if(oldVal === newVal){
            vm.saveSegmentEnabled = false;
            vm.saved = false;
        } else {
            vm.saveSegmentEnabled = true;
        };
    };

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.hideMessage = function() {
        vm.saved = false;
    }

    vm.saveSegment = function() {
        var segmentName = $stateParams.segment,
            ts = new Date().getTime();

        if (segmentName === 'Create') {
            var accountRestriction = QueryStore.getAccountRestriction(),
                contactRestriction = QueryStore.getContactRestriction(),
                segment = {
                    'name': 'segment' + ts,
                    'display_name': 'segment' + ts,
                    'account_restriction': accountRestriction,
                    'contact_restriction': contactRestriction,
                    'page_filter': {
                        'row_offset': 0,
                        'num_rows': 10
                    }
                };

            SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                QueryStore.setupStore(result.data);

                vm.saveSegmentEnabled = false;
                $state.go('.', { segment: 'segment' + ts }, { notify: false });
                vm.saved = true;
            });
        } else {
            SegmentStore.getSegmentByName(segmentName).then(function(result) {
                var segmentData = result,
                    accountRestriction = QueryStore.getAccountRestriction(),
                    contactRestriction = QueryStore.getContactRestriction(),
                    segment = {
                        'name': segmentData.name,
                        'display_name': segmentData.display_name,
                        'account_restriction': accountRestriction,
                        'contact_restriction': contactRestriction,
                        'page_filter': {
                            'row_offset': 0,
                            'num_rows': 10
                        }
                    };

                SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                    QueryStore.setupStore(result.data);
                    
                    vm.saveSegmentEnabled = false;
                    $state.go('.', { segment: 'segment' + ts }, { notify: false });
                    vm.saved = true;
                });
            });
        };
    };

    vm.makeRecommendationCounts = function() {
        //var opts = opts || {};

        if(!vm.accountsCoverage || !vm.accountsCoverage.bucketCoverageCounts) {
            vm.recommendationCounts = null;
            return false;
        }
        var sections = {
                total: 0,
                selected: 0,
                suppressed: 0,
                launched: 0,
                contacts: 0
            },
            buckets = {};

        // vm.accountsCoverage.bucketCoverageCounts.forEach(function(count) {
        //     sections.total += parseInt(count.count);
        // });
        //sections.total = vm.accountsCoverage.accountCount + vm.accountsCoverage.unscoredAccountCount;

        sections.total = PlaybookWizardStore.currentPlay.targetSegment.accounts;


        // sections.unscored = (vm.launchUnscored ? vm.unscoredAccounts.total : 0);
        // sections.selected += sections.unscored;

        var _contacts = 0;
        for(var i in vm.selectedBuckets) {
            var bucket = vm.selectedBuckets[i],
                count = vm.accountsCoverage.bucketCoverageCounts.find(function(value) {
                    return value.bucket === bucket;
                });

            count = count || {count: 0};
            sections.selected += parseInt(count.count);
            _contacts = _contacts + parseInt(count.contactCount || 0);
        }

        // if(resetTopNCount){
        //     vm.topNCount = sections.selected;
        // }

        sections.selected = vm.launchUnscored ? vm.accountsCoverage.unscoredAccountCount + sections.selected : sections.selected;

        // if(resetTopNCount){
        //     vm.topNCount = sections.selected;
        // }

        if (vm.topNClicked && vm.topNCount <= sections.selected){
            sections.launched = vm.topNCount <= 0 ? 0 : vm.topNCount;
            // if(vm.topNCount <= 0){
            //     sections.launched = 0;
            // }
            // else {
            //     sections.launched = vm.topNCount;
            // }
        } else {
            sections.launched = sections.selected;
        }

        sections.suppressed = sections.total >= sections.launched ? sections.total - sections.launched : sections.total;

        sections.contacts = _contacts + (vm.launchUnscored ? (vm.accountsCoverage.unscoredContactCount ? vm.accountsCoverage.unscoredContactCount  : 0) : 0); //vm.accountsCoverage.contactCount || 0; // need to find campaign with contactCount to test this

        // sections.suppressed = parseInt(sections.total - sections.selected);

        // sections.launched = vm.topNCount && opts.suppressed ? vm.topNCount : sections.selected;//(sections.selected > sections.suppressed ? sections.total - sections.suppressed : sections.selected));

        // var $topNCountEl = angular.element('input#topNCount');
        
        // if($topNCountEl.is(':checked')) {
        //     sections.suppressed = Math.max(sections.total - vm.topNCount, sections.suppressed) || 0;
        // }

        vm.recommendationCounts = sections;
        PlaybookWizardStore.setRecommendationCounts(sections);
        vm.setValidation();
    }

    $scope.$watch('vm.current', function(newValue, oldValue) {
        vm.loading = true;
        updatePage();
    });

    vm.init();
}).filter('percentage', ['$filter', function ($filter) {
  return function (input, decimals) {
    return $filter('number')(input * 100, decimals) + '%';
  };
}]);


