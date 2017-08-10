angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($scope, $state, $stateParams, BrowserStorageUtility, QueryStore, QueryService, SegmentServiceProxy, LookupStore, AccountsCount) {

    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accounts: [],
        accountsCount: AccountsCount,
        loading: true,
        restriction: QueryStore.getRestriction(),
        current: 1,
        pagesize: 20,
        search: '',
        searchOptions: {
            updateOn: 'default blur',
            debounce: 1500
        },
        sortType: 'LDC_Name',
        sortReverse: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false,
        section: $stateParams.section
    });


    vm.init = function() {
        
        QueryStore.setAccounts('', $stateParams.segment).then(function(response){
            vm.accounts = response.data;
            vm.loading = false;
        });

    };
    vm.init();

    var prevQuery = vm.search;
    vm.submitQuery = function() {
        console.log("search");
        vm.loading = true;
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
      console.log(tmpStr);
      if (!tmpStr || tmpStr.length == 0)
        return 0;
        if (tmpStr === vm.search){
          updatePage();
        }
    });

    function updatePage() {
        var offset = (vm.current - 1) * vm.pagesize;
        var query = {
            free_form_text_search: vm.search,
            frontend_restriction: vm.restriction,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            }
        };

        QueryStore.setAccounts(query, $stateParams.segment).then(function(response){
            vm.accounts = response.data;
            vm.loading = false;

            console.log(vm.accounts);
        });

    };
});
