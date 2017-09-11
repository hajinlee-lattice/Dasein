angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($q, $scope, $state, $stateParams, $filter, 
    BrowserStorageUtility, QueryStore, QueryService, SegmentService, SegmentStore, LookupStore, Config, Accounts, AccountsCount, CountWithoutSalesForce, Contacts, ContactsCount) {

    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accounts: Accounts,
        accountsCount: AccountsCount,
        accountsWithoutSfId: CountWithoutSalesForce,
        contacts: Contacts,
        contactsCount: ContactsCount,
        loading: true,
        saving: false,
        saved: false,
        segment: QueryStore.getSegment(),
        accountRestriction: QueryStore.getAccountRestriction(),
        contactRestriction: QueryStore.getContactRestriction(),
        current: 1,
        pagesize: 15,
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
        config: Config
    });

    function updatePage() {

        vm.loading = true;

        var offset = (vm.current - 1) * vm.pagesize;
        var dataQuery = {
            free_form_text_search: vm.search,
            account_restriction: vm.accountRestriction,
            contact_restriction: vm.contactRestriction,
            preexisting_segment_name: $stateParams.segment,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            },
            restrict_with_sfdcid: vm.excludeNonSalesForce
        };

        if(vm.page === 'Accounts'){
            QueryStore.setAccounts(dataQuery).then(function(response){
                vm.accounts = response.data;
                vm.loading = false;
            });
        } else {
            QueryStore.setContacts(dataQuery).then(function(response){
                vm.contact = response.data;
                vm.loading = false;
            });
        }

        vm.setCurrentRestrictionForSaveButton();

    };


    vm.excludeNonSalesForceCheckbox = function(excludeAccounts){
        excludeAccounts = !excludeAccounts;

        if(excludeAccounts){
            vm.excludeNonSalesForce = false;
        } else {
            vm.excludeNonSalesForce = true;
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
        return vm.inModel
            ? 'home.model.analysis.explorer.query.advanced'
            : 'home.segment.explorer.query.advanced'
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


});


