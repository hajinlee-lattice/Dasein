angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function(
    $q, $scope, $state, $stateParams, $filter, $rootScope,
    BrowserStorageUtility, QueryStore, QueryService, 
    SegmentService, SegmentStore, LookupStore, Config, Accounts, /*AccountsCount, ContactsCount,*/ 
    AccountsCoverage, Contacts, PlaybookWizardStore, PlaybookWizardService, NoSFCount
) {
    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        section: $stateParams.section,
        page: $stateParams.pageTitle,
        myDataOrSegmentAccounts: vm.section === 'segment.analysis' && vm.page === 'Accounts',
        myDataOrSegmentContacts: vm.section === 'segment.analysis' && vm.page === 'Contacts',
        accounts: Accounts,
        counts: {},
        accountsCount: null, //AccountsCount,
        contacts: Contacts,
        contactsCount: null, //ContactsCount,
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
        noSFCount: NoSFCount,
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

            // Get counts from QueryStore
            vm.counts = QueryStore.getCounts();

        } else {
            
            // Set counts for accounts and contacts
            vm.counts = { 
                accounts: { 
                    value: vm.accountsCoverage.accountCount 
                },
                contacts: {
                    value: vm.accountsCoverage.contactCount
                }
            };

            // Create array of buckets to be used when launching play
            vm.selectedBuckets = [];
            vm.accountsCoverage.bucketCoverageCounts.forEach(function(bucket){
                vm.selectedBuckets.push(bucket.bucket);
            });

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
            } else if (vm.page === 'Contacts'){
                QueryStore.setContacts(dataQuery).then(function(response) {
                    vm.contacts = response.data;
                    vm.loading = false;
                });
            }

        } else {

            // Targets page for create Play flow
            var query = { 
                free_form_text_search: '',
                restrictNotNullSalesforceId: false,
                entityType: 'Account',
                bucketFieldName: 'ScoreBucket',
                maximum: 10,
                offset: 0,
                sortBy: 'LDC_Name',
                descending: false,
                selectedBuckets: vm.selectedBuckets
            };

            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                console.log(data);

                var engineId = data.ratingEngine.id;

                PlaybookWizardService.getTargetData(engineId, query).then(function(data){ 
                    PlaybookWizardStore.setTargetData(data.data);
                    
                    vm.accounts = PlaybookWizardStore.getTargetData();
                });

            });


            PlaybookWizardStore.setValidation('targets', true);
            vm.loading = false;

        }

        if (vm.myDataOrSegmentAccounts){

            // Accounts page in the my data or segment screens
            // Get counts for accounts
            QueryStore.GetCountByQuery('accounts').then(function(data){ 
                vm.counts.accounts.value = data;
                vm.counts.accounts.loading = false;
                
                if(data > 10){
                    vm.showAccountPagination = true;
                    vm.showContactPagination = false;
                }
            });

        
        } else if (vm.myDataOrSegmentContacts){

            // Contacts page in the my data or segment screens
            // Get counts for contacts
            QueryStore.GetCountByQuery('contacts').then(function(data){ 
                vm.counts.contacts.value = data;
                vm.counts.contacts.loading = false;


                if(vm.counts.contacts.value < 10){
                    vm.showAccountPagination = false;
                    vm.showContactPagination = true;
                }

            });
        }

        // Show Account or Contact pagination
        // I need to refactor the pagination for this page - Jon (Nov 26)
        if((vm.page === 'Accounts' || vm.page === 'Playbook') && (vm.counts.accounts.value > 10)){
            vm.showAccountPagination = true;
            vm.showContactPagination = false;
        } else if (vm.page === 'Contacts' && (vm.counts.contacts.value > 10)){
            vm.showAccountPagination = false;
            vm.showContactPagination = true;
        }

        vm.checkSaveButtonState();

    };


    vm.excludeNonSalesForceCheckbox = function(excludeAccounts){
        excludeAccounts = !excludeAccounts;

        if(excludeAccounts){
            vm.excludeNonSalesForce = true;
        } else {
            vm.excludeNonSalesForce = false;
        }

        updatePage();
        
    };

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


    

    vm.bucketClick = function(bucket) {

        console.log(bucket);
        console.log(vm.selectedBuckets);

        var index = vm.selectedBuckets.indexOf(bucket);

        if (vm.selectedBuckets.indexOf(bucket) > -1) {
            vm.selectedBuckets.splice( index, 1 );
        } else {
            vm.selectedBuckets.push( bucket );
        }

        updatePage();

    }




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


