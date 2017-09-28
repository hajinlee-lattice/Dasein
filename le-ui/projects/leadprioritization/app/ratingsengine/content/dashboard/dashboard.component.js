angular.module('lp.ratingsengine.dashboard', [])
.controller('RatingsEngineDashboard', function($q, $stateParams, $state, 
    RatingsEngineStore, Rating, TimestampIntervalUtility, NumberUtility) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        TimestampIntervalUtility: TimestampIntervalUtility,
        NumberUtility: NumberUtility
    });

    var makeGraph = function(data, params) {
        if(!data) {
            return null;
        }
        var label_key = params.label || 'bucket',
            count_key = params.count || 'count',
            total = 0,
            _graph = {},
            graph = {};
        angular.forEach(data, function(value) {
            total = total + value[count_key];
            _graph[value[label_key]] = value[count_key];
        });
        graph = {
            total: total,
            data: _graph
        }
        return graph;
    }

    vm.init = function() {
        console.log(vm.rating.coverageInfo.bucketCoverageCounts);
        vm.buckets = makeGraph([
            {bucket: 'A', count: 0},
            {bucket: 'A-', count: 753},
            {bucket: 'B', count: 9913},
            {bucket: 'C', count: 32576},
            {bucket: 'E', count: 0},
            {bucket: 'F', count: 0},
        ], {label: 'bucket', count: 'count'});
        //vm.buckets = makeGraph(vm.rating.coverageInfo.bucketCoverageCounts, {label: 'bucket', count: 'count'});
    }

    vm.init();

    // RatingsEngineStore.getRating($stateParams.rating_id).then(function(results){
    //     console.log(results);
    //     vm.ratingsengine = results;
    // });

});
