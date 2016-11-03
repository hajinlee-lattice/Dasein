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

    // tag type selected pipelines bar charts
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
        if (!pBucketPipelines[ap]) {
            pBucketPipelines[ap] = {};
        }

        var pBucket = pBucketPipelines[ap];
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

            _.each(bucketSeries, function (bucketSerie, key) {
                var colIdx = columnToIndexMap[key];

                bucketSerie.push({
                    key: key,
                    y: bucket.values[0][colIdx],
                    x: ap
                });
            });
        });

        var chart = {};
        chart.title = tagName;
        chart.data = _.map(bucketSeries, function (values, key) {
            var chartData = {};
            chartData.key = key;
            chartData.values = values.sort(function(a,b) { return (a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0); });

            return chartData;
        });

        return chart;
    });

    $scope.lineCharts = lineCharts;

    if ($scope.lineCharts.length === 0 && $scope.barCharts.length === 0) {
        $scope.message = 'No Charts to be Displayed';
    }
});
