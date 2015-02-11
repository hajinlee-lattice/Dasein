angular.module('mainApp.appCommon.widgets.ThresholdExplorerWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('ThresholdExplorerWidgetController', function ($scope, ResourceUtility, WidgetFrameworkService) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;

    if (data == null) {
        return;
    }
    
    $scope.chartData = [
        {
            id: "a",
            percent: 0.25
        },
        {
            id: "b",
            percent: 0.30
        },
        {
            id: "c",
            percent: 0.20
        },
        {
            id: "d",
            percent: 0.25
        }
    ];
    
    /*function calculateX(chartData, totalWidth) {
        var counter = 0;
        for (var i=0;i<chartData.length;i++) {
            var chartDataWidth = totalWidth * chartData[i].percent;
            if (counter === 0) {
                chartData[i].x = 0;
                counter = chartDataWidth;
            } else {
                chartData[i].x = counter + 1;
                counter = counter + chartDataWidth + 1;
            }
        }
    }

    //TODO:pierce Do stuff
    var width = $("#thresholdExplorerChart").width();
    calculateX(chartData, width);
    
    var svg = d3.select(#thresholdExplorerChart)
        .append("svg")
        .datum(chartData)
        .attr("width", "100%")
        .attr("height", "400");
    
    svg.selectAll("rect")
        .data(chartData)
    .enter().append("rect")
        .style("fill", "#487bba")
        .attr("height", "380")
        .attr("x", function(d) {
            return d.x; 
        })
        .attr("width", function (d) {
            var barWidth = width * d.percent;
            return barWidth > 2 ? barWidth : 2; // so we can see really tiny bars
        });*/
})

.directive('thresholdExplorerWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});