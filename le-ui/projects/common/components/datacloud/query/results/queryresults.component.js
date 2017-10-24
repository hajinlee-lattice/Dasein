angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function(
    $q, $scope, $state, $stateParams, $filter, $rootScope,
    BrowserStorageUtility, QueryStore, QueryService, 
    SegmentService, SegmentStore, LookupStore, Config, Accounts, /*AccountsCount, ContactsCount,*/ 
    CountWithoutSalesForce, Contacts, PlaybookWizardStore
) {
    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accounts: Accounts,
        counts: QueryStore.getCounts(),
        accountsCount: null, //AccountsCount,
        accountsWithoutSfId: CountWithoutSalesForce,
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
        excludeNonSalesForce: false,
        sortType: 'LDC_Name',
        sortReverse: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false,
        section: $stateParams.section,
        page: $stateParams.pageTitle,
        config: Config,
        currentTargetTab: 'Accounts'
    });

    vm.init = function() {
        if(vm.segment != null){

            //console.log("this");
            $rootScope.$broadcast('header-back', { 
                path: '^home.segment.accounts',
                displayName: vm.segment.display_name,
                sref: 'home.segments'
            });
        }

        vm.accountsCount = vm.counts.accounts.value;
        vm.contactsCount = vm.counts.contacts.value;
    }

    function updatePage() {
        vm.loading = true;
        var offset = (vm.current - 1) * vm.pagesize;

        if (vm.section === 'segment.analysis') {
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
            } else {
                QueryStore.setContacts(dataQuery).then(function(response) {
                    console.log(response.data);
                    vm.contacts = response.data;
                    vm.loading = false;
                });
            }

            vm.setCurrentRestrictionForSaveButton();

        } else {
            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){

                var engineId = data.ratingEngine.id,
                    query = { 
                        free_form_text_search: '',
                        restrictNotNullSalesforceId: false,
                        entityType: 'Account',
                        bucketFieldName: 'ScoreBucket',
                        maximum: 15,
                        offset: offset,
                        sortBy: 'LDC_Name',
                        descending: false
                    };

                PlaybookWizardStore.getTargetData(engineId, query).then(function(data){ 
                    vm.accounts = data.data; 
                    vm.loading = false;
                });

            });

            PlaybookWizardStore.getPlay($stateParams.play_name).then(function(data){
                var engineId = data.ratingEngine.id,
                    engineIdObject = [{id: engineId}];
                PlaybookWizardStore.getRatingsCounts(engineIdObject).then(function(data){
                    vm.counts.accounts.value = data.ratingEngineIdCoverageMap[engineId].accountCount;
                });
            });

            PlaybookWizardStore.setValidation('targets', true);
            vm.loading = false;
        }

        if(vm.page === 'Accounts' || vm.page === 'Available Targets' || vm.page === 'Playbook'){
            QueryStore.GetCountByQuery('accounts').then(function(data){ 
            
                vm.counts.accounts.value = data;
                vm.counts.accounts.loading = false;
                if(data > 10){
                    vm.showAccountPagination = true;
                    vm.showContactPagination = false;
                }
            });
        } else {
            QueryStore.GetCountByQuery('contacts').then(function(data){ 
                
                vm.counts.contacts.value = data;
                vm.counts.contacts.loading = false;

                if(vm.counts.contacts.value < 10){
                    vm.showAccountPagination = false;
                    vm.showContactPagination = true;
                }
            });
        }



        if((vm.page === 'Accounts' || vm.page === 'Available Targets') && vm.counts.accounts.value > 10){
            vm.showAccountPagination = true;
            vm.showContactPagination = false;
        } else if (vm.page === 'Contacts' && vm.counts.contacts.value > 10){
            vm.showAccountPagination = false;
            vm.showContactPagination = true;
        }

    };


    vm.excludeNonSalesForceCheckbox = function(excludeAccounts){
        excludeAccounts = !excludeAccounts;

        if(excludeAccounts){
            vm.excludeNonSalesForce = true;
        } else {
            vm.excludeNonSalesForce = false;
        }

        console.log(vm.excludeNonSalesForce);

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


    vm.setCurrentRestrictionForSaveButton = function(){
        var segmentName = $stateParams.segment;

        if (segmentName === 'Create') {
            var accountRestriction = JSON.stringify({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": []
                    }
                }
            });

            var contactRestriction = JSON.stringify({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": []
                    }
                }
            });

            $stateParams.defaultSegmentRestriction = accountRestriction + contactRestriction;
            
            vm.checkSaveButtonState();
        } else {
            SegmentStore.getSegmentByName(segmentName).then(function(result) {
                var accountRestriction = JSON.stringify(result.account_restriction),
                    contactRestriction = JSON.stringify(result.contact_restriction);
                
                $stateParams.defaultSegmentRestriction = accountRestriction + contactRestriction;
                
                vm.checkSaveButtonState();
            });
        };
    };


    vm.checkSaveButtonState = function(){
        var oldVal = $stateParams.defaultSegmentRestriction,
            newAccountVal = JSON.stringify(QueryStore.getAccountRestriction()),
            newContactVal = JSON.stringify(QueryStore.getContactRestriction()),
            newVal = newAccountVal + newContactVal;

        if(oldVal === newVal){
            vm.saveSegmentEnabled = false;
        } else {
            vm.saveSegmentEnabled = true;
        };
    };

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
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
                    'account_restriction': contactRestriction,
                    'page_filter': {
                        'row_offset': 0,
                        'num_rows': 10
                    }
                };

            SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                QueryStore.setupStore(result.data);

                vm.saveSegmentEnabled = false;
                $state.go('.', { segment: 'segment' + ts }, { notify: false });
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
                });
            });
        };
    };

    $scope.$watch('vm.current', function(newValue, oldValue) {
        vm.loading = true;
        updatePage();
    });

    vm.init();
});


