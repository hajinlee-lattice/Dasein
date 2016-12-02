angular.module("app.modelquality.controller.ModelQualityDashboardCtrl", [
])
.controller('ModelQualityDashboardCtrl', function ($scope, $state, $window, $q, AnalyticTests, InfluxDbService) {

    var _window = angular.element($window);

    var bindResize = function () {
        $scope.$broadcast('resize');
        $scope.$digest();
    };

    _window.bind('resize', bindResize);

    $scope.$on('$destroy', function () {
        _window.unbind('resize', bindResize);
    });

    var analyticTests = AnalyticTests.resultObj;

    var queries = [];
    for (var i = 0; i < analyticTests.length; i++) {
        var at = analyticTests[i];
        var type = at.analytic_test_type;

        var query;
        switch (type) {
            case 'Production':
                query = getProductionQuery(at);
                break;
            case 'SelectedPipelines':
                query = getSelectedPipelinesQuery(at);
                break;
        }

        if (query) {
            queries.push({
                query: query,
                analyticTest: at,
                type: type
            });
        }
    }

    $scope.chartDataPromiseQueries = queries.map(function (query) {
        var promise = InfluxDbService.Query({
            q: query.query,
            db: 'ModelQuality'
        }).then(function (result) {
            var defer = $q.defer();

            if (influxResponseHasData(result)) {
                switch (query.type) {
                    case 'Production':
                        defer.resolve(processLineChart(result));
                        break;
                    case 'SelectedPipelines':
                        defer.resolve(processBarChart(result));
                        break;
                    default:
                        defer.reject({
                            query: query,
                            analyticTest: query.analyticTest,
                            reason: 'Invalid analytic test type'
                        });
                }
            } else {
                defer.reject({
                    query: query,
                    analyticTest: query.analyticTest,
                    reason: 'No data found in InfluxDB'
                });
            }

            return defer.promise;
        });

        return {
            type: query.type,
            title: query.analyticTest.name,
            promise: promise
        };
    });

    if (!$scope.chartDataPromiseQueries.length) {
        $scope.message = 'No Charts to be Displayed';
    }

    function getProductionQuery (analyticTest) {
        var series = [
            'MEAN(RocScore) AS RocScore',
            'MEAN(Top10PercentLift) AS Top10PercentLift',
            'MEAN(Top20PercentLift) AS Top20PercentLift',
            'MEAN(Top30PercentLift) AS Top30PercentLift'
        ];

        var whereClause = "", sep = "";
        var pipelines = analyticTest.analytic_pipeline_names;
        for (var i = 0; i < pipelines.length; i++) {
            whereClause += sep + "AnalyticPipelineName = '" + pipelines[i] + "'";
            sep = " OR ";
        }

        var queries = [];
        for (var j = 0; j < series.length; j++) {
             var q = "SELECT " + series[j] + " FROM ModelingMeasurement WHERE AnalyticTestName = '" + analyticTest.name + "' AND (" + whereClause + ")" + " GROUP BY AnalyticPipelineName";
             queries.push(q);
        }
        return queries.join(";");
    }

    function getSelectedPipelinesQuery (analyticTest) {
        var series = [
            'RocScore',
            'Top10PercentLift',
            'Top20PercentLift',
            'Top30PercentLift'
        ].join(',');

        var columns = [
            'AnalyticPipelineName',
            'AnalyticTestName',
            'DataSetName',
        ].join(',');

        var whereClause = "", sep = "";
        var pipelines = analyticTest.analytic_pipeline_names;
        for (var i = 0; i < pipelines.length; i++) {
            whereClause += sep + "AnalyticPipelineName = '" + pipelines[i] + "'";
            sep = " OR ";
        }

        var query = "SELECT " + series + "," + columns + " FROM ModelingMeasurement WHERE AnalyticTestName = '" + analyticTest.name + "' AND (" + whereClause + ")";
        return query;
    }

    function processBarChart (queryResult) {
        var data = [],
            columns = [],
            rows = [];
        if (queryResult.resultObj.results && queryResult.resultObj.results[0].series) {
            data = queryResult.resultObj.results[0].series[0];
            columns = data.columns;
            rows = data.values;
        }

        var columnToIndexMap = {};
        columns.forEach(function(column, index) {
            columnToIndexMap[column] = index;
        });

        var analyticPipelineNameIndex = columnToIndexMap.AnalyticPipelineName,
            analyticTestNameIndex = columnToIndexMap.AnalyticTestName,
            dataSetNameIndex = columnToIndexMap.DataSetName,
            pipelineNameIndex = columnToIndexMap.PipelineName;

        var metricsColIndex = {};
        metricsColIndex.RocScore = columnToIndexMap.RocScore;
        metricsColIndex.Top10PercentLift = columnToIndexMap.Top10PercentLift;
        metricsColIndex.Top20PercentLift = columnToIndexMap.Top20PercentLift;
        metricsColIndex.Top30PercentLift = columnToIndexMap.Top30PercentLift;

        var barChart = {};
        rows.forEach(function (row) {

            var at = row[analyticTestNameIndex];
            var ap = row[analyticPipelineNameIndex];
            var ds = row[dataSetNameIndex];

            if (!barChart[ds]) {
                var newDsBucket = {};
                newDsBucket.dataset = ds;
                newDsBucket.description = {};
                newDsBucket.description.analyticTest = at;
                newDsBucket.description.dataset = ds;
                newDsBucket.pipelines = {};
                barChart[ds] = newDsBucket;
            }

            var dsBucket = barChart[ds];
            var pBucketPipelines = dsBucket.pipelines;
            if (!pBucketPipelines[ap]) {
                pBucketPipelines[ap] = {};
            }

            var pBucket = pBucketPipelines[ap];
            var value = {};
            for (var m in metricsColIndex) {
                value[m] = row[metricsColIndex[m]];
            }

            pBucket.description = {};
            pBucket.description.analyticTest = at;
            pBucket.description.dataset = ds;
            pBucket.description.analyticPipeline = ap;
            pBucket.value = value;

        });

        return _.map(barChart, function (dsData, dsKey) {
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
    }

    function processLineChart (queryResult) {
        var series = [];
        if (queryResult.resultObj.results) {
            series = queryResult.resultObj.results;
        }

        var lines = series.map(function (series) {
            series = series.series;
            var key = series[0].columns[1];

            var points = series.map(function (data) {
                return {
                    x: data.tags.AnalyticPipelineName,
                    y: data.values[0][1],
                    key: key
                };
            });

            return {
                key: key,
                values: points
            };
        });

        return lines;
    }

    function influxResponseHasData (result) {
        return !!result.resultObj.results[0].series;
    }

});
