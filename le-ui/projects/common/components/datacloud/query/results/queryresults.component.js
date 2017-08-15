angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($q, $scope, $state, $stateParams, $filter, 
    BrowserStorageUtility, QueryStore, QueryService, SegmentServiceProxy, LookupStore, Config) {

    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accounts: [],
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
            updatePage();
        } else {
            vm.excludeNonSalesForce = true;
            updatePage();
        }
        
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


        if(vm.section === 'dashboard.targets' || vm.section === 'wizard.targets') {
            
            var deferred = $q.defer(),
                restriction = QueryStore.getRestriction(),
                query = {
                    'free_form_text_search': '',
                    'frontend_restriction': restriction,
                    'page_filter': {
                        'row_offset': 0,
                        'num_rows': 1000000
                    },
                    'restrict_without_sfdcid': true
                };

            var excludeCount = QueryStore.GetCountByQuery('accounts', query).then(function(response){ return response }); 

            deferred.resolve( vm.accountsWithoutSfId = excludeCount );

            return deferred.promise;
        };


        var offset = (vm.current - 1) * vm.pagesize;
        var query = {
            free_form_text_search: vm.search,
            frontend_restriction: vm.restriction,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            },
            restrict_without_sfdcid: vm.excludeNonSalesForce
        };
        QueryStore.setAccounts(query, $stateParams.segment).then(function(response){
            vm.accounts = response.data;
            vm.loading = false;
            //console.log(vm.accounts);
        });

    };


});


