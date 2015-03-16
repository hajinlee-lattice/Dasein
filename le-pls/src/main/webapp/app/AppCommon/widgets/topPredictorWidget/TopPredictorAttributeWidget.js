angular.module('mainApp.appCommon.widgets.TopPredictorAttributeWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.services.TopPredictorService'
])

.controller('TopPredictorAttributeWidgetController', function ($scope, _, ResourceUtility, TopPredictorService) {
    var data = $scope.data;
    $scope.attributeName = data.name;
    $scope.attributeFullDescription = data.description;
    $scope.attributeDescription = data.description;
    if ($scope.attributeDescription != null && $scope.attributeDescription.length > 110) {
       $scope.attributeDescription = $scope.attributeDescription.substring(0, 110) + "...";
    }
    $scope.attributeColor = data.color;
    
    function setHoverPosition(xPos) {
        var donutChartSvg = $(".js-top-predictor-donut > svg");
        var donutChartLocation = donutChartSvg.offset();
        var attributeHover = $(".attribute-hover");
        $("#topPredictorAttributeHover").css("top", donutChartLocation.top - 30);
        
        if (xPos > 0) {
            $("#topPredictorAttributeHover").css("left", donutChartLocation.left + donutChartSvg.width() - 10);
            attributeHover.removeClass("attribute-hover-left-arrow");
            attributeHover.addClass("attribute-hover-right-arrow");
        } else {
            $("#topPredictorAttributeHover").css("left", donutChartLocation.left - 340);
            attributeHover.removeClass("attribute-hover-right-arrow");
            attributeHover.addClass("attribute-hover-left-arrow");
        }
        
        $("#topPredictorAttributeHover").show();
    }
    setHoverPosition($scope.mouseX);
    var chartData = data.elementList;
    var liftValues = _.map(chartData, function(d){ return parseFloat(d.lift.toPrecision(2)); });
    var bucketNames = _.map(chartData, "name");
    var percentLeads = _.map(chartData, function(d){ return parseInt(d.percentTotal); });
    
    var chart,
        width = 150,
        left_width = 101,
        barHeight = 20,
        gap = 6,
        baseHeight = (barHeight + gap * 2) * chartData.length,
        labelSize = "10px",
        fontSize = "12px",
        commonDy = "0em",
        labelDx = 0;
    
    var x = d3.scale.linear()
        .domain([0, d3.max(liftValues) + 1])
        .range([0, width]);
    
    var maxTicks = d3.max(liftValues) > 5 ? 5 : d3.max(liftValues);
    var xTicks = x.ticks(maxTicks + 1);
    chart = d3.select("#attributeChart") 
      .append('svg')
      .attr('class', 'chart')
      .attr('width', left_width + width + 40)
      .attr('height', baseHeight + 80)
      .append("g")
      .attr("transform", "translate(0, 0)");
    
    // These are the background bars that alternate
    chart.selectAll("rect.background")
        .data(liftValues)
        .enter().append("rect")
        .attr("display", function(d, i) { 
            return i % 2 == 1 ? "none" : null; 
        }) 
        .attr("x", left_width)
        .attr("y", function(d, i) { 
            return (i * (barHeight + 2 * gap)) + 24; 
        })
        .attr("width", function (d) {
            return width + 80;
        })
        .attr("height", barHeight + 8)
        .style("fill", "#EEF3F7")
        .attr('opacity', 0.7);
        
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
        .attr("y2", baseHeight + 20);
        
    // These are the background tick labels
    chart.selectAll(".rule")
        .data(xTicks)
        .enter().append("text")
        .attr("class", "rule")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", baseHeight + 40)
        .attr("dx", function(d) {
            if (d === 0) {
                return 0;
            } else {
                return labelDx;
            }
        })
        .attr("dy", -6)
        .attr("font-weight", "semi-bold")
        .attr("font-size", "10px")
        .attr("text-anchor", "middle")
        .style("fill", "#666666")
        .text(function(d) {
            if (d === 0) {
                return d;
            } else if (d === 1) {
                return ""; 
            } else {
                return d; 
            }
        });
    
    // This is the lift label at the bottom of the chart
    var liftText = ResourceUtility.getString("TOP_PREDICTORS_HOVER_CHART_LIFT_LABEL").toUpperCase();
    chart.append("text")
        .attr("x", function () {
            return left_width + (width/2) - 20;
        })
        .attr("y", baseHeight + 55)
        .attr("dy", commonDy)
        .attr("font-size", labelSize)
        .style("fill", "#999999")
        .text(liftText);
        
    // These are the bars
    chart.selectAll("rect.bar")
        .data(liftValues)
        .enter().append("rect")
        .attr("x", left_width)
        .attr("y", function(d, i) { 
            return (i * (barHeight + 2 * gap)) + 28; 
        })
        .attr("width", x)
        .attr("height", barHeight)
        .style("fill", data.color)
        .attr('opacity', function(d) {
            if (d > 1) {
                return 0.9;
            } else {
                return 0.4;
            }
            
        });
    
    // This is the 1x line
    chart.selectAll("line.baselineLift")
        .data([1])
        .enter().append("line")
        .attr("x1", function(d) { return x(d) + left_width; })
        .attr("x2", function(d) { return x(d) + left_width; })
        .attr("y1", 20)
        .attr("y2", baseHeight + 20)
        .style("fill", "#BBBBBB")
        .attr('opacity', 1);
        
    // This is the 1x line label at the bottom
    chart.selectAll(".baselineLiftBottom")
        .data([1])
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", baseHeight + 40)
        .attr("dy", -6)
        .attr("dx", labelDx)
        .attr("font-weight", "semi-bold")
        .attr("font-size", "10px")
        .attr("text-anchor", "middle")
        .style("fill", "#666666")
        .text(function(d) { return d; } );
    
    // This is the 1x line label at the top    
    chart.selectAll(".baselineLiftTop")
        .data([1])
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", 20)
        .attr("dy", -6)
        .attr("dx", labelDx)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "middle")
        .style("fill", "#666666")
        .text(function(d) { return d + "x"; } );
        
    // These are the lift numbers to the right of the chart
    chart.selectAll("text.lift")
        .data(liftValues)
        .enter().append("text")
        .attr("x", width + 135)
        .attr("y", function(d, i) {
            return (i * (barHeight + 2 * gap)) + 42; 
        })
        .attr("dx", -5)
        .attr("dy", commonDy)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "black")
        .text(function(d) { return d + "x"; } );
        
    // This is the lift label to the right of the chart
    chart.append("text")
        .attr("x", width + 110)
        .attr("y", 5)
        .attr("dy", "0.36em")
        .attr("font-size", labelSize)
        .style("fill", "#999999")
        .text(liftText);
    
    // These are the percent numbers to the right of the chart
    chart.selectAll("text.percentLeads")
        .data(percentLeads)
        .enter().append("text")
        .attr("x", width + 180)
        .attr("y", function(d, i) { 
            return (i * (barHeight + 2 * gap)) + 42; 
        })
        .attr("dx", -5)
        .attr("dy", commonDy)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "black")
        .text(function(d) { return d + "%"; } );
    
    // This is the %Leads label to the right of the chart
    var leadsText = ResourceUtility.getString("TOP_PREDICTORS_HOVER_CHART_LEADS_LABEL").toUpperCase();
    chart.append("text")
        .attr("x", width + 142)
        .attr("y", 5)
        .attr("dy", "0.36em")
        .attr("font-size", labelSize)
        .style("fill", "#999999")
        .text(leadsText);
        
    // These are the bucket names to the left of the chart
    chart.selectAll("text.name")
        .data(bucketNames)
        .enter().append("text")
        .attr("x", left_width - 5)
        .attr("y", function(d, i) {
            return (i * (barHeight + 2 * gap)) + 42; 
        })
        .attr("dy", commonDy)
        .attr("font-weight", 600)
        .attr("font-size", fontSize)
        .attr("text-anchor", "end")
        .style("fill", "black")
        .text(String)
        .call(wrap, left_width - 20);
    
    function wrap(text, width) {
        text.each(function() {
            var text = d3.select(this),
                words = text.text().split(/\s+/).reverse(),
                wordLength = words.length,
                word,
                line = [],
                lineNumber = 0,
                lineHeight = 1.1, // ems
                y = text.attr("y"),
                dy = parseFloat(text.attr("dy")),
                tspan = text.text(null).append("tspan").attr("x", left_width - 5).attr("y", y).attr("dy", dy + "em");
            for (var i = 0; i < wordLength; i++) {
                word = words.pop();
                line.push(word);
                tspan.text(line.join(" "));
                if (tspan.node().getComputedTextLength() > width) {
                    dy = -0.36;
                    tspan.attr("dy", dy + "em");
                    line.pop();
                    tspan.text(line.join(" "));
                    line = [word];
                    tspan = text.append("tspan").attr("x", left_width - 5).attr("y", y).attr("dy", ++lineNumber * lineHeight + dy + "em").text(word);
                }
            }
        });
    }
        
})

.directive('topPredictorAttributeWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});