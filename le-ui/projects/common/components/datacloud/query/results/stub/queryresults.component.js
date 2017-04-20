angular.module('common.datacloud.query.results')
.controller('QueryResultsStubCtrl', function($scope, $state, $stateParams, BrowserStorageUtility,
        SegmentServiceProxy, CountMetadata, Columns, Records) {

    var vm = this;
    angular.extend(vm, {
        context: $state.current.name.substring($state.current.name.lastIndexOf('.') + 1),
        modelId: $stateParams.modelId,
        count: CountMetadata,
        columns: Columns,
        results: Records,
        current: 1,
        max: 0,
        pagesize: 20,
        search: '',
        sortBy: (Columns && Columns.length > 0) ? Columns[0].column_name : null,
        sortDesc: false,
        authToken: BrowserStorageUtility.getTokenDocument(),
        saving: false
    });

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    };

    vm.refineQuery = function() {
        return vm.inModel()
            ? 'home.model.analysis.explorer.query'
            : 'home.segment.explorer.query'
    };

    vm.sort = function(columnName) {
        vm.sortBy = columnName;
        vm.sortDesc = !vm.sortDesc;
        vm.current = 1;
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
        }).finally(function() {
            vm.saving = false;
        });
    };
});
