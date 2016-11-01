angular.module("app.modelquality.controller.ModelQualityDashboardCtrl", [
])
.controller('ModelQualityDashboardCtrl', function ($scope, $state, $window, SelectedPipelineMetrics, ProductionPipelineMetrics) {

    var _window = angular.element($window);

    var bindResize = function () {
        $scope.$broadcast('resize');
        $scope.$digest();
    };

    _window.bind('resize', bindResize);

    $scope.$on('$destroy', function () {
        _window.unbind('resize', bindResize);
    });


    var data = [],
        columns = [],
        rows = [];
    if (SelectedPipelineMetrics.resultObj.results && SelectedPipelineMetrics.resultObj.results[0].series) {
        data = SelectedPipelineMetrics.resultObj.results[0].series[0];
        columns = data.columns;
        rows = data.values;
    }

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
                barData.category = pipelineData.description.analyticPipeline;
                barData.value = pipelineData.value;
                barData.description = pipelineData.description;

                return barData;
            });

            return groupData;
        });

        return chartData;
    });

    // tag type production line charts
    var series = [];
    if (ProductionPipelineMetrics.resultObj.results && ProductionPipelineMetrics.resultObj.results[0].series) {
        series = ProductionPipelineMetrics.resultObj.results[0].series;
    }

    var tagBuckets = {};
    series.forEach(function (entry) {
        var tagName = entry.tags.AnalyticTestTag;

        if (!tagBuckets[tagName]) {
            tagBuckets[tagName] = [];
        }

        var tagBucket = tagBuckets[tagName];
        tagBucket.push(entry);
    });

    var lineCharts = _.map(tagBuckets, function (tagBucket, tagName) {
        var bucketSeries = {
            RocScore: [],
            Top10PercentLift: [],
            Top20PercentLift: [],
            Top30PercentLift: []
        };

        tagBucket.forEach(function (bucket) {
            var ap = bucket.tags.AnalyticPipelineName;

            var columnToIndexMap = {};
            bucket.columns.forEach(function(column, index) {
                columnToIndexMap[column] = index;
            });

            bucketSeries.RocScore.push({
                key: 'RocScore',
                y: bucket.values[0][columnToIndexMap.RocScore],
                x: ap
            });
            bucketSeries.Top10PercentLift.push({
                key: 'Top10PercentLift',
                y: bucket.values[0][columnToIndexMap.Top10PercentLift],
                x: ap
            });
            bucketSeries.Top20PercentLift.push({
                key: 'Top20PercentLift',
                y: bucket.values[0][columnToIndexMap.Top20PercentLift],
                x: ap
            });
            bucketSeries.Top30PercentLift.push({
                key: 'Top30PercentLift',
                y: bucket.values[0][columnToIndexMap.Top30PercentLift],
                x: ap
            });

        });

        var chart = {};
        chart.data = _.map(bucketSeries, function (values, key) {
            var chartData = {};
            chartData.key = key;
            chartData.values = values.sort(function(a,b) { return (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0); });

            return chartData;
        });
        chart.title = tagName;

        return chart;
    });

    $scope.lineCharts = lineCharts;

    if ($scope.lineCharts.length === 0 && $scope.barCharts.length === 0) {
        $scope.message = 'No Charts to be Displayed';
    }
});
