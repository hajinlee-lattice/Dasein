angular.module("app.modelquality.controller.ModelQualityDashboardCtrl", [
])
.controller('ModelQualityDashboardCtrl', function ($scope, $state, MeasurementData, $timeout, ModelQualityService) {

    // TODO
        // fetch all metrics
        // bucket by: production, pipeline, dataset, etc
        // create a service to process data, return array of chartdata
        // 1 chart (line or bar) for each set of data

    //NOTE: "boolean" and "null" values are strings
    var data = MeasurementData.resultObj.results[0].series[0];

    var columns = data.columns;
    var rows = data.values;

    var timeColumnIndex = columns.indexOf('time');
    var columnIndexes = {
        RocScore: columns.indexOf('RocScore'),
        Top10PercentLift: columns.indexOf('Top10PercentLift'),
        Top20PercentLift: columns.indexOf('Top20PercentLift'),
        Top30PercentLift: columns.indexOf('Top30PercentLift')
    };

    /*
    series {
        set: [(x,y),...],
    }
    */
    var series = {};

    for (var metric in columnIndexes) {
        series[metric] = [];
    }

    var minDate = new Date(),
        maxDate = new Date(0),
        maxValue = Number.NEGATIVE_INFINITY,
        minValue = 0;

    rows.forEach(function (row) {
        var date = new Date(row[timeColumnIndex]);
        maxDate = Math.max(maxDate, date);
        minDate = Math.min(minDate, date);

        for (var metric in columnIndexes) {
            var value = row[columnIndexes[metric]];
            maxValue = Math.max(maxValue, value);

            series[metric].push({
                key: metric,
                date: date,
                value: value
            });
        }

    });

    series = Object.keys(series).map(function (metric) {
        return {
            key: metric,
            values: series[metric]
        };
    });

    $scope.lineChartData = {
        title: "",
        data: series,
        xExtent: [new Date(minDate), new Date(maxDate)],
        yExtent: [minValue, maxValue]
    };

});
