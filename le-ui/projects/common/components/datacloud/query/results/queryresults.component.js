angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($scope, $state, $stateParams, BrowserStorageUtility, QueryStore, QueryService, SegmentServiceProxy, LookupStore) {

    var vm = this;
    angular.extend(vm, {
        resourceType: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        inModel: $state.current.name.split('.')[1] === 'model',
        accountsCount: 0,
        accounts: [],
        restriction: QueryStore.getRestriction(),
        current: 1,
        pagesize: 20,
        search: '',
        sortBy: null,
        sortDesc: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false,
    });


    vm.init = function() {
    
        QueryStore.GetCountByQuery('accounts', '').then(function(data){
            vm.accountsCount = data;
        });
        QueryStore.GetDataByQuery('accounts', '').then(function(data){
            vm.accounts = data;
        });

    };
    vm.init();

    var prevQuery = vm.search;
    vm.submitQuery = function() {
        if ((vm.search && prevQuery) && (vm.search.toUpperCase() === prevQuery.toUpperCase())) {
            return;
        }

        var query = { free_form_text_search: vm.search };
        QueryStore.GetCountByQuery(vm.resourceType, query).then(function(results) {
            vm.accountsCount = results;
        });

        prevQuery = vm.search;
        vm.current = 1;
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
        updatePage();
    });

    function updatePage() {
        var offset = (vm.current - 1) * vm.pagesize;
        var query = {
            free_form_text_search: vm.search,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            }
        };

        if (vm.sortBy) {
            query.sort = {
                descending: vm.sortDesc,
                lookups: [
                    {
                        columnLookup: {
                            column_name: vm.sortBy
                        }
                    }
                ]
            };
        }

        QueryStore.GetDataByQuery(vm.resourceType, query).then(function(results) {
            vm.results = results.data;
        });
    };
});
