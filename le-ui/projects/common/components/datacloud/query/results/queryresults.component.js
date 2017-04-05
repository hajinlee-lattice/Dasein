angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($scope, $state, BrowserStorageUtility, QueryStore, SegmentServiceProxy, CountMetadata) {

    var vm = this;
    angular.extend(vm, {
        context: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        count: CountMetadata ? CountMetadata.count : 0,
        countMetadata: CountMetadata || {},
        columns: [{displayName: 'Company Name', key: 'business_name'}],
        results: [],
        current: 1,
        pagesize: 20,
        search: '',
        sortBy: null,
        sortDesc: false,
        authToken: BrowserStorageUtility.getTokenDocument()
    });

    var prevQuery = vm.search;
    vm.submitQuery = function() {
        if ((vm.search && prevQuery) && (vm.search.toUpperCase() === prevQuery.toUpperCase())) {
            return;
        }

        var query = { free_form_text_search: vm.search };
        QueryStore.GetCountByQuery(vm.context, query).then(function(results) {
            vm.count = results;
        });

        prevQuery = vm.search;
        vm.current = 1;
        updatePage();
    };

    vm.clearSearch = function() {
        vm.search = '';
        vm.current = 1;

        vm.submitQuery();
    };

    vm.sort = function(key) {
        return; // sort currently unavailable

        vm.sortBy = key;
        vm.sortDesc = !vm.sortDesc;
        vm.current = 1;

        updatePage();
    };

    vm.saveSegment = function () {
        SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
            if (!result.errorMsg) {
                $state.go('home.model.segmentation', {}, {notify: true})
            }
        });
    };

    $scope.$watch('vm.current', function(newValue, oldValue) {
        updatePage();
    });

    function updatePage() { // debounce this
        var offset = (vm.current - 1) * vm.pagesize;
        var query = {
            free_form_text_search: vm.search,
            page_filter: {
                num_rows: vm.pagesize,
                row_offset: offset
            },
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

        QueryStore.GetDataByQuery(vm.context, query).then(function(results) {
            vm.results = results.data;
        });
    }
});
