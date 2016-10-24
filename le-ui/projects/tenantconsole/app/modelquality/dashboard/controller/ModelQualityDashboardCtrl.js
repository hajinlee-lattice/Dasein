angular.module("app.modelquality.controller.ModelQualityDashboardCtrl", [
])
.controller('ModelQualityDashboardCtrl', function ($scope, $state, MeasurementData, $window, $rootScope) {

    var stateChangeStart = $rootScope.$on('$stateChangeStart', function (event, toState, toParams, fromState, fromParams) {
        _window.unbind('resize', bindResize);
    });

    var _window = angular.element($window);

    var bindResize = function () {
        $scope.$broadcast('resize');
        $scope.$digest();
    };

    _window.bind('resize', bindResize);

    $scope.$on('$destroy', function () {
        typeof stateChangeStart === 'function' ? stateChangeStart() : null;
    });


    var data = MeasurementData.resultObj.results[0].series[0],
        columns = data.columns,
        rows = data.values;

    var columnToIndexMap = {};
    columns.forEach(function(column, index) {
        columnToIndexMap[column] = index;
    });

    var timeColumnIndex = columnToIndexMap.time,
        analyticPipelineNameIndex = columnToIndexMap.AnalyticPipelineName,
        analyticTestNameIndex = columnToIndexMap.AnalyticTestName,
        analyticTestTagIndex = columnToIndexMap.AnalyticTestTag,
        dataSetNameIndex = columnToIndexMap.DataSetName,
        pipelineNameIndex = columnToIndexMap.PipelineName,
        samplingNameIndex = columnToIndexMap.SamplingName,
        propDataNameIndex = columnToIndexMap.PropDataConfigName,
        metricsColIndex = {
            RocScore: columnToIndexMap.RocScore,
            Top10PercentLift: columnToIndexMap.Top10PercentLift,
            Top20PercentLift: columnToIndexMap.Top20PercentLift,
            Top30PercentLift: columnToIndexMap.Top30PercentLift
        };

    var lineBuckets = {},
        barBuckets = {};
    rows.forEach(function (row) {

        var at = row[analyticTestNameIndex];
        var ap = row[analyticPipelineNameIndex];
        var p = row[pipelineNameIndex];
        var ds = row[dataSetNameIndex];
        var date = row[timeColumnIndex];

        // if (!at || at === 'null' ||
        //     !ap || ap === 'null' ||
        //     !p  || p === 'null' ||
        //     !ds || ds === 'null') {
        //     return;
        // }

        // line charts
        var key = [at,ap,p,ds].join(':');
        if (!lineBuckets[key]) {
            lineBuckets[key] = {
                description: {
                    analyticTest: at,
                    analyticPipeline: ap,
                    pipeline: p,
                    dataset: ds
                },
                key: key,
                data: {}
            };
        }

        var bucket = lineBuckets[key].data;
        for (var metric in metricsColIndex) {
            if (!bucket[metric]) {
                bucket[metric] = [];
            }

            bucket[metric].push({
                key: metric,
                date: new Date(date),
                value: row[metricsColIndex[metric]]
            });
        }

        // bar charts
        if (!barBuckets[at]) {
            barBuckets[at] = {};
        }

        var atBucket = barBuckets[at];
        if (!atBucket[ds]) {
            atBucket[ds] = {
                dataset: ds,
                description: {
                    analyticTest: at,
                    dataset: ds
                },
                pipelines: {}
            };
        }

        var dsBucket = atBucket[ds];
        var pBucketPipelines = dsBucket.pipelines;
        var pKey = [ap, p].join(':');
        if (!pBucketPipelines[pKey]) {
            pBucketPipelines[pKey] = {};
        }

        var pBucket = pBucketPipelines[pKey];
        if (!pBucket.date || pBucket.date < date) { 
            var value = {};
            for (var m in metricsColIndex) {
                value[m] = row[metricsColIndex[m]];
            }

            pBucket.description = {
                analyticTest: at,
                dataset: ds,
                analyticPipeline: ap,
                pipeline: p
            };
            pBucket.date = date;
            pBucket.value = value;
        }

    });

    $scope.lineCharts = Object.keys(lineBuckets).map(function(key) {
        var bucket = lineBuckets[key];
        return {
            data: {
                data: Object.keys(bucket.data).map(function (metric) {
                    return {
                        key: metric,
                        values: bucket.data[metric]
                    };
                }),
                title: bucket.description.analyticTest
            },
            type: 'line'
        };
    });

    $scope.barCharts = Object.keys(barBuckets).map(function (barBucket) {
        var bucketData = barBuckets[barBucket];

        return {
            data: Object.keys(bucketData).map(function (dataset) {
                var dsBucket = bucketData[dataset];
                return {
                    dataset: dataset,
                    categories: Object.keys(dsBucket.pipelines).map(function(pipeline) {
                        var pipelineData = dsBucket.pipelines[pipeline];
                        return {
                            category: pipelineData.description.pipeline,
                            value: pipelineData.value,
                            description: pipelineData.description
                        };
                    })
                };
            }),
            title: barBucket
        };
    });

});
