angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function(
    $q, $scope, $state, $stateParams, $filter, $rootScope,
    BrowserStorageUtility, QueryStore, QueryService, 
    SegmentService, SegmentStore, LookupStore, Config, Accounts,
    AccountsCoverage, Contacts, PlaybookWizardStore, PlaybookWizardService, NoSFIdsCount
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
        search: '',
        searchOptions: {
            updateOn: 'default blur',
            debounce: 1500
        },
        accountsCoverage: AccountsCoverage,
        excludeNonSalesForce: false,
        sortType: 'LDC_Name',
        sortReverse: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false,
        config: Config,
        currentTargetTab: 'Accounts'
    });

    vm.init = function() {
        
        if(vm.segment != null && vm.section != 'wizard.targets'){
            $rootScope.$broadcast('header-back', { 
                path: '^home.segment.accounts',
                displayName: vm.segment.display_name,
                sref: 'home.segments'
            });
        }

        // Set Counts for Segment and PLay Targets
        if (vm.section === 'segment.analysis') {
            vm.counts = QueryStore.getCounts();
        } else {

            vm.selectedBuckets = [];
            if (vm.section === 'wizard.targets') {
                
                // Get sum of non-suppressed buckets to calculate percentage for each bucket
                var numAccounts = 0;
                for (var i = 0; i < vm.accountsCoverage.bucketCoverageCounts.length; i++) {
                    numAccounts += vm.accountsCoverage.bucketCoverageCounts[i].count;
                }
                // Create array (vm.selectedBuckets) of bucket names (e.g. ["A", "B", "C"]) 
                // to be used when launching play, and assign percentage to the bucket for display purposes
                vm.accountsCoverage.bucketCoverageCounts.forEach(function(bucket){
                    vm.selectedBuckets.push(bucket.bucket);
                    bucket.percentage = bucket.count / numAccounts;
                });

            } else if (vm.section === 'dashboard.targets') {

                PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){
                    data.ratings.forEach(function(bucket){
                        vm.selectedBuckets.push(bucket.bucket);
                    });
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
                QueryStore.GetCountByQuery('accounts').then(function(data){ 
                    vm.counts.accounts.value = data;
                    vm.counts.accounts.loading = false;
                    
                    if(data > 10){
                        vm.showAccountPagination = true;
                        vm.showContactPagination = false;
                    }
                });
            } else if (vm.page === 'Contacts'){
                QueryStore.setContacts(dataQuery).then(function(response) {
                    vm.contacts = response.data;
                    vm.loading = false;
                });
                QueryStore.GetCountByQuery('contacts').then(function(data){ 
                    vm.counts.contacts.value = data;
                    vm.counts.contacts.loading = false;

                    if(data > 10){
                        vm.showAccountPagination = false;
                        vm.showContactPagination = true;
                    }

                });
            }

        } else {

            // Targets page for create Play flow
            var dataQuery = { 
                    free_form_text_search: '',
                    restrictNotNullSalesforceId: vm.excludeNonSalesForce,
                    entityType: 'Account',
                    bucketFieldName: 'ScoreBucket',
                    maximum: vm.pagesize,
                    offset: offset,
                    sortBy: 'LDC_Name',
                    descending: false,
                    selectedBuckets: vm.selectedBuckets
                };

            PlaybookWizardStore.getPlay($stateParams.play_name, true).then(function(data){
            
                // Get play rating engine and create array object literal for getting the counts.
                var engineId = data.ratingEngine.id,
                    engineIdObject = [{id: engineId}];

                // Get Account Data             
                PlaybookWizardService.getTargetData(engineId, dataQuery).then(function(data){ 
                    PlaybookWizardStore.setTargetData(data.data);
                    vm.accounts = PlaybookWizardStore.getTargetData();
                });

                // -----------------------------------------
                // Uncomment this when backend supports 
                // the checkbox 'Exclude non SalesForce accounts' 
                // from target's list

                // var filtered = [];
                // console.log(vm.accounts);
                // for (var i = 0; i < vm.accounts.length; i++) {
                //     if (vm.accounts[i].SalesforceAccountID === "" || vm.accounts[i].SalesforceAccountID === null || vm.accounts[i].SalesforceAccountID === undefined) {
                //         filtered.push(vm.accounts[i]);
                //         vm.noSFCount = filtered.length;
                //     }
                // }
                // vm.noSFCount = filtered.length;
                // -----------------------------------------

                // Get Account Counts for Pagination
                PlaybookWizardStore.getRatingsCounts(engineIdObject).then(function(data){
                    var accountsCoverage = (data.ratingEngineIdCoverageMap && data.ratingEngineIdCoverageMap[engineId] ? data.ratingEngineIdCoverageMap[engineId] : null);
                    
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

                    if (vm.section === 'wizard.targets' || vm.section === 'dashboard.targets') {
                        vm.counts = { 
                            accounts: { 
                                value: calculateCountsFromFiltered(filteredAccountsCoverage) 
                            },
                            contacts: {
                                value: vm.accountsCoverage.contactCount
                            }
                        };

                    } else {
                        vm.counts = { 
                            accounts: { 
                                value: calculateCountsFromFiltered(filteredAccountsCoverage) 
                            },
                            contacts: {
                                value: vm.accountsCoverage.contactCount
                            }
                        };
                    }
                    if(vm.counts.accounts.value > 10){
                        vm.showAccountPagination = true;
                        vm.showContactPagination = false;
                    }
                    vm.ratedTargetsLimit = vm.counts.accounts.value;

                });

                QueryStore.setBucketsToLaunch(vm.selectedBuckets);

            });

            vm.loading = false;

        }

        vm.checkSaveButtonState();

    };

    vm.updateTargetLimit = function() {
        QueryStore.setRatedTargetsLimit(vm.ratedTargetsLimit);
    }
    vm.ratingLimitInputClick = function($event) {
        $scope.targetsLimit = true;
        $event.target.select();
    }

    vm.excludeNonSalesForceCheckbox = function(excludeAccounts){
        excludeAccounts = !excludeAccounts;

        console.log(excludeAccounts);

        if(excludeAccounts){
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

        QueryStore.setBucketsToLaunch(vm.selectedBuckets);

        updatePage();
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

    vm.clearSearch = function() {
        vm.search = '';
        vm.current = 1;

        vm.submitQuery();
    };


    vm.sort = function(columnName) {
        vm.sortBy = columnName;
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


