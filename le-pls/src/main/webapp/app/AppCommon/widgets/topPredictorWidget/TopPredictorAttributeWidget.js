angular.module('mainApp.appCommon.widgets.TopPredictorAttributeWidget', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.controller('TopPredictorAttributeWidgetController', function ($scope, ResourceUtility) {
    var data = $scope.data;
    $scope.attributeName = data.name;
    $scope.attributeDescription = data.description;
    $scope.attributeColor = data.color;
    
    function setHoverPosition(xPos) {
        var donutChartSvg = $(".js-top-predictor-donut > svg");
        var donutChartLocation = donutChartSvg.offset();
        var attributeHover = $(".attribute-hover");
        $("#topPredictorAttributeHover").css("top", donutChartLocation.top - 50);
        
        if (xPos > 0) {
            $("#topPredictorAttributeHover").css("left", donutChartLocation.left + donutChartSvg.width());
            attributeHover.removeClass("attribute-hover-left-arrow");
            attributeHover.addClass("attribute-hover-right-arrow");
        } else {
            $("#topPredictorAttributeHover").css("left", donutChartLocation.left - 510);
            attributeHover.removeClass("attribute-hover-right-arrow");
            attributeHover.addClass("attribute-hover-left-arrow");
        }
    }
    setHoverPosition($scope.mouseX);
    
    var chartData = data.elementList;
    var i;
    
    var liftValues = [];
    for (i = 0; i < data.elementList.length; i++) {
        liftValues.push(parseFloat(data.elementList[i].lift));
    }
    
    var bucketNames = [];
    for (i = 0; i < data.elementList.length; i++) {
        bucketNames.push(data.elementList[i].name);
    }
    
    var percentLeads = [];
    for (i = 0; i < data.elementList.length; i++) {
        percentLeads.push(data.elementList[i].percentTotal);
    }
    
    var chart,
        width = 200,
        left_width = 101,
        bar_height = 24,
        height = bar_height * bucketNames.length,
        gap = 10;
    
    var x = d3.scale.linear()
        .domain([0, d3.max(liftValues) + 1])
        .range([0, width]);
    
    var y = d3.scale.ordinal()
        .domain(liftValues)
        .rangeBands([0, (bar_height + 2 * gap) * liftValues.length]);
    
    var maxTicks = d3.max(liftValues) > 5 ? 5 : d3.max(liftValues);
    var xTicks = x.ticks(maxTicks + 1);
    chart = d3.select("#attributeChart") 
      .append('svg')
      .attr('class', 'chart')
      .attr('width', left_width + width + 40)
      .attr('height', (bar_height + gap * 2) * bucketNames.length + 80)
      .append("g")
      .attr("transform", "translate(0, 20)");
      
    // These are the background ticks
    chart.selectAll("line")
        .data(xTicks)
        .enter().append("line")
        .attr("x1", function(d) { return x(d) + left_width; })
        .attr("x2", function(d) { return x(d) + left_width; })
        .attr("y1", function(d, i) {
            if (i === 0 || i === xTicks.length-1) {
                return 0;
            } else {
                return 20;
            }
        })
        .attr("y2", (bar_height + gap * 2) * bucketNames.length + 20);
        
    // These are the background tick labels
    chart.selectAll(".rule")
        .data(xTicks)
        .enter().append("text")
        .attr("class", "rule")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", (bar_height + gap * 2) * bucketNames.length + 40)
        .attr("dy", -6)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "middle")
        .style("fill", "#666666")
        .text(function(d) { return d + "x"; } );
    
    // This is the lift label at the bottom of the chart
    var liftText = ResourceUtility.getString("TOP_PREDICTORS_HOVER_CHART_LIFT_LABEL").toUpperCase();
    chart.append("text")
        .attr("x", function () {
            return left_width + (width/2) - 20;
        })
        .attr("y", (bar_height + gap * 2) * bucketNames.length + 55)
        .attr("dy", ".36em")
        .attr("font-size", "10px")
        .style("fill", "#999999")
        .text(liftText);
    
    // These are the bars
    chart.selectAll("rect")
        .data(liftValues)
        .enter().append("rect")
        .attr("x", left_width)
        .attr("y", function(d) { 
            return y(d) + gap + 20; 
        })
        .attr("width", x)
        .attr("height", bar_height)
        .style("fill", data.color)
        .attr('opacity', function(d) {
            if (d > 1) {
                return 0.9;
            } else {
                return 0.4;
            }
            
        });
        
    // These are the lift numbers to the right of the chart
    chart.selectAll("text.lift")
        .data(liftValues)
        .enter().append("text")
        .attr("x", 400)
        .attr("y", function(d, i) {
            return (i * (bar_height + 2 * gap)) + 42; 
        })
        .attr("dx", -5)
        .attr("dy", ".36em")
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "black")
        .text(function(d) { return d + "x"; } );
        
    // This is the lift label to the right of the chart
    chart.append("text")
        .attr("x", 375)
        .attr("y", 5)
        .attr("dy", ".36em")
        .attr("font-size", "10px")
        .style("fill", "#999999")
        .text(liftText);
    
    // These are the percent numbers to the right of the chart
    chart.selectAll("text.percentLeads")
        .data(percentLeads)
        .enter().append("text")
        .attr("x", 450)
        .attr("y", function(d, i) { 
            return (i * (bar_height + 2 * gap)) + 42; 
        })
        .attr("dx", -5)
        .attr("dy", ".36em")
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "black")
        .text(function(d) { return d + "%"; } );
    
    // This is the %Leads label to the right of the chart
    var leadsText = ResourceUtility.getString("TOP_PREDICTORS_HOVER_CHART_LEADS_LABEL").toUpperCase();
    chart.append("text")
        .attr("x", 410)
        .attr("y", 5)
        .attr("dy", ".36em")
        .attr("font-size", "10px")
        .style("fill", "#999999")
        .text(leadsText);
        
    // These are the bucket names to the left of the chart
    chart.selectAll("text.name")
        .data(bucketNames)
        .enter().append("text")
        .attr("x", left_width - 5)
        .attr("y", function(d, i) {
            return (i * (bar_height + 2 * gap)) + 42; 
        })
        .attr("dy", ".36em")
        .attr("text-anchor", "end")
        .style("fill", "black")
        .text(String);
        
})

.directive('topPredictorAttributeWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});