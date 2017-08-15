angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($q, $scope, $state, $stateParams, $filter, 
    BrowserStorageUtility, QueryStore, QueryService, SegmentServiceProxy, LookupStore, Config, AccountsCount, CountWithoutSalesForce) {

    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accounts: [],
        accountsCount: AccountsCount,
        accountsWithoutSfId: CountWithoutSalesForce,
        loading: true,
        restriction: QueryStore.getRestriction(),
        current: 1,
        pagesize: 20,
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
        config: Config
    });



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
            ? 'home.model.analysis.explorer.query'
            : 'home.segment.explorer.query'
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

    vm.saveSegment = function () {
        vm.saving = true;
        SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
            if (!result.errorMsg) {
                if (vm.modelId) {
                    $state.go('home.model.segmentation', {}, {notify: true})
                } else {
                    $state.go('home.segments', {}, {notify: true});
                }
            }
        }).finally(function () {
            vm.saving = false;
        });
    };

    $scope.$watch('vm.current', function(newValue, oldValue) {
        vm.loading = true;
        updatePage();
    });

    $scope.$watch('vm.search', function (tmpStr){
        if (!tmpStr || tmpStr.length == 0) {
            return 0;
            if (tmpStr === vm.search){
              updatePage();
            }
        }
    });

    function updatePage() {

        vm.loading = true;
        console.log(vm.excludeNonSalesForce);


        var offset = (vm.current - 1) * vm.pagesize;
        var dataQuery = {
            free_form_text_search: vm.search,
            frontend_restriction: vm.restriction,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            },
            restrict_with_sfdcid: vm.excludeNonSalesForce
        };
        QueryStore.setAccounts(dataQuery, $stateParams.segment).then(function(response){
            vm.accounts = response.data;
            vm.loading = false;
        });

    };


});


