angular.module('common.datacloud.query.results', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('QueryResultsCtrl', function($scope, $state, BrowserStorageUtility, QueryStore, Columns, CountMetadata) {

    var context = $state.current.name.substring($state.current.name.lastIndexOf('.') + 1);

    var vm = this;
    angular.extend(vm, {
        current: 1,
        pagesize: 20,
        count: 0,
        countMetadata: CountMetadata || {},
        columns: Columns || [],
        results: [],
        search: null,
        sortBy: null,
        context: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        authToken: BrowserStorageUtility.getTokenDocument()
    });

    vm.clearSearch = function() {
        vm.search = null;
        vm.current = 1;

        updatePage();
    };

    var prevQuery = vm.search;
    vm.submitQuery = function() {
        if (vm.search === prevQuery) {
            return;
        }

        if (vm.search && vm.prevQuery && vm.search.toUpperCase() === prevQuery.toUpperCase()) {
            return;
        }

        prevQuery = vm.search;

        updatePage();
    };

    vm.sort = function(key) {
        if (key !== Columns[0].key) { return; }

        vm.sortBy = key;
        vm.sortDesc = !vm.sortDesc;
        vm.current = 1;

        updatePage();
    };

    $scope.$watch('vm.current', function(newValue, oldValue) {
        var offset = (newValue - 1) * vm.pagesize;

        updatePage(offset);
    });

    function updatePage(offset) {
        offset = offset || 0;

        setCount();

        //vm.results = QueryStore.getPage(vm.context, offset, vm.pagesize, vm.search, vm.sortBy, vm.sortDesc);
    }

    function setCount() {
    }
});
