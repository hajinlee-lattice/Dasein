angular.module("app.modelquality.controller.ModelQualityDashboardCtrl", [
])
.controller('ModelQualityDashboardCtrl', function ($scope, $state, $window, $rootScope, SelectedPipelineMetrics, ProductionPipelineMetrics) {

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


    var data = SelectedPipelineMetrics.resultObj.results[0].series[0],
        columns = data.columns,
        rows = data.values;

    var columnToIndexMap = {};
    columns.forEach(function(column, index) {
        columnToIndexMap[column] = index;
    });

    var timeColumnIndex = columnToIndexMap.time,
        analyticPipelineNameIndex = columnToIndexMap.AnalyticPipelineName,
        analyticTestNameIndex = columnToIndexMap.AnalyticTestName,
        dataSetNameIndex = columnToIndexMap.DataSetName,
        pipelineNameIndex = columnToIndexMap.PipelineName;

    var metricsColIndex = {};
    metricsColIndex.RocScore = columnToIndexMap.RocScore;
    metricsColIndex.Top10PercentLift = columnToIndexMap.Top10PercentLift;
    metricsColIndex.Top20PercentLift = columnToIndexMap.Top20PercentLift;
    metricsColIndex.Top30PercentLift = columnToIndexMap.Top30PercentLift;

    var barChartBuckets = {};
    rows.forEach(function (row) {

        var at = row[analyticTestNameIndex];
        var ap = row[analyticPipelineNameIndex];
        var p = row[pipelineNameIndex];
        var ds = row[dataSetNameIndex];
        var date = row[timeColumnIndex];

        // bar charts
        if (!barChartBuckets[at]) {
            barChartBuckets[at] = {};
        }

        var atBucket = barChartBuckets[at];
        if (!atBucket[ds]) {
            var newDsBucket = {};
            newDsBucket.dataset = ds;
            newDsBucket.description = {};
            newDsBucket.description.analyticTest = at;
            newDsBucket.description.dataset = ds;
            newDsBucket.pipelines = {};
            atBucket[ds] = newDsBucket;
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

            pBucket.description = {};
            pBucket.description.analyticTest = at;
            pBucket.description.dataset = ds;
            pBucket.description.analyticPipeline = ap;
            pBucket.description.pipeline = p;
            pBucket.date = date;
            pBucket.value = value;
        }

    });

    $scope.barCharts = _.map(barChartBuckets, function (atData, atKey) {
        var chartData = {};
        chartData.title = atKey;
        chartData.data = _.map(atData, function (dsData, dsKey) {
            var groupData = {};
            groupData.dataset = dsKey;
            groupData.categories = _.map(dsData.pipelines, function (pipelineData, pipelineKey) {
                var barData = {};
                barData.category = pipelineData.description.pipeline;
                barData.value = pipelineData.value;
                barData.description = pipelineData.description;

                return barData;
            });

            return groupData;
        });

        return chartData;
    });

    // production line charts
    var series = ProductionPipelineMetrics.resultObj.results[0].series;

    var seriesData = {
        RocScore: [],
        Top10PercentLift: [],
        Top20PercentLift: [],
        Top30PercentLift: []
    };

    series.forEach(function (serie) {
        var ap = serie.tags.AnalyticPipelineName;

        var RocScoreIdx = serie.columns.indexOf('RocScore'),
            Top10PercentLiftIdx = serie.columns.indexOf('Top10PercentLift'),
            Top20PercentLiftIdx = serie.columns.indexOf('Top20PercentLift'),
            Top30PercentLiftIdx = serie.columns.indexOf('Top30PercentLift');

        seriesData.RocScore.push({
            y: serie.values[0][RocScoreIdx],
            x: ap
        });
        seriesData.Top10PercentLift.push({
            y: serie.values[0][Top10PercentLiftIdx],
            x: ap
        });
        seriesData.Top20PercentLift.push({
            y: serie.values[0][Top20PercentLiftIdx],
            x: ap
        });
        seriesData.Top30PercentLift.push({
            y: serie.values[0][Top30PercentLiftIdx],
            x: ap
        });

    });

    var productionChart = {};
    productionChart.description = {};
    productionChart.data = _.map(seriesData, function (values, key) {
        var chartData = {};
        chartData.key = key;
        chartData.values = _.map(values, function (value) {
            return {
                key: key,
                x: value.x,
                y: value.y
            };
        }).sort(function(a,b) { return (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0); });

        return chartData;
    });
    productionChart.title = 'Production';
    $scope.productionChart = productionChart;
});
