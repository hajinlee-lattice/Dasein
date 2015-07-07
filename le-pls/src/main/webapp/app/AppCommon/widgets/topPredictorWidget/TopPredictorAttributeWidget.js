angular.module('mainApp.appCommon.widgets.TopPredictorAttributeWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.services.TopPredictorService',
    'ngSanitize'
])

.controller('TopPredictorAttributeWidgetController', function ($scope, _, $sce, $filter, ResourceUtility, TopPredictorService) {
    var data = $scope.data;
    $scope.attributeName = data.name;
    $scope.attributeFullDescription = data.description;
    $scope.attributeDescription = data.description;
    /* no longer needed, truncation happens further down.
    if ($scope.attributeDescription != null && $scope.attributeDescription.length > 110) {
        $scope.attributeDescription = $scope.attributeDescription.substring(0, 110) + "&hellip;";
    }
    */
    $scope.attributeColor = data.color;

    var chartData = data.elementList;
    var liftValues = _.map(chartData, function(d){ return d.lift; });
    var bucketNames = _.map(chartData, "name");
    var percentLeads = _.map(chartData, function(d){ return TopPredictorService.FormatPercent(d.percentTotal); });
    percentLeads = TopPredictorService.SumToOne(percentLeads);
    
    $scope.generateAttributeName = function() { 
        return $sce.trustAsHtml('<h4 style="border-left: ' + $scope.attributeColor + 
    		' solid 6px;" title="' + $scope.attributeName + '">' + $scope.attributeName + '</h4>' + '<p title="' +  
    		$scope.attributeDescription + '">' + decodeURIComponent(escape($scope.attributeDescription)) + '</p>');
    };

    var chart,
        width = 145,
        left_width = 101,
        chartWidth = left_width + width + 45,
        barHeight = 20,
        gap = 6,
        baseHeight = (barHeight + gap * 2) * chartData.length - gap * 2,
        titleHeight = 80,
        labelHeight = 70,
        chartHeight = baseHeight + labelHeight + titleHeight,
        labelSize = "10px",
        fontSize = "12px",
        commonDy = "0em",
        labelDx = 0;

    function setHoverPosition(xPos, chartHeight) {
        var attributeHover = $(".attribute-hover"),
            hoverElem = $("#topPredictorAttributeHover"),
            donut = document.getElementById('chart'),
            path = $scope.selectedPath[0][0],
            donutRect = donut.getBoundingClientRect(),
            pathRect = path.getBoundingClientRect(),
            center = donutRect.left + (donutRect.width >> 1),
            top = (pathRect.top - donutRect.top + (pathRect.height >> 1)) / donutRect.height;
        
        // Adjust height of tail pseudo-element to anchor to path element
        // FIXME - Find a better way to adjust pseudo-element when supported
        document.styleSheets[0].addRule(
            '.attribute-hover-left-arrow>div:after,' +
            '.attribute-hover-left-arrow>div:before,' +
            '.attribute-hover-right-arrow>div:after,' +
            '.attribute-hover-right-arrow>div:before',
            'top: ' + top * 100 + '% !important;' // !important to override previous
        );

        hoverElem.css("top", donutRect.top + (donutRect.height >> 1));

        if (xPos > 0) {
            hoverElem.css("left", center + width - 5);
            attributeHover.removeClass("attribute-hover-left-arrow");
            attributeHover.addClass("attribute-hover-right-arrow");
        } else {
            hoverElem.css("left", center - width + 15 - 360);
            attributeHover.removeClass("attribute-hover-right-arrow");
            attributeHover.addClass("attribute-hover-left-arrow");
        }

        /* 
            this code truncates the description.  It's a bit hacky,
            but there is no real good way of doing it.  The opacity
            changes are there to hide the box until the next frame to
            avoid HTML reflow when description gets adjusted.
        */
        hoverElem[0].style.opacity = 0;
        hoverElem.show();

        setTimeout(function() {
            var description = $('.attribute-hover-header p')[0],
                height = description.offsetHeight;

            if (height > 28) { // determine > 2 lines, line-height 14.
                $(description).addClass('truncate_2lines');
            }

            hoverElem[0].style.opacity = 1;
        }, 0);
    }

    setHoverPosition($scope.mouseX, chartHeight);

    var maxTick = _.max([_.max(liftValues), 1.5]);
    var xTicks = TopPredictorService.createTicks(maxTick, 5);
    var x = d3.scale.linear()
        .domain([0, xTicks[xTicks.length - 1]])
        .range([0, width - 5]);

    chart = d3.select("#attributeChart")
        .append('svg')
        .attr('class', 'chart')
        .attr('width', chartWidth)
        .attr('height', baseHeight + labelHeight)
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
        .attr("width", function () {
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
                return 15;
            }
        })
        .attr("y2", baseHeight + 32);

    // These are the background tick labels
    chart.selectAll(".rule")
        .data(xTicks)
        .enter().append("text")
        .attr("class", "rule")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", baseHeight + 50)
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
        .attr("y", baseHeight + 60)
        .attr("dy", commonDy)
        .attr("font-size", labelSize)
        .style("fill", "#999")
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
                return 0.8;
            } else {
                return 0.4;
            }

        });
    
    // These are the "borders" that accompany each bar
    chart.selectAll("rect.barBorder")
        .data(liftValues)
        .enter().append("rect")
        .attr("x", left_width)
        .attr("y", function(d, i) {
            return (i * (barHeight + 2 * gap)) + (28 + barHeight);
        })
        .attr("width", function (d) {
            var barWidth = x(d);
            return barWidth-0.4;
        })
        .attr("height", 1)
        .style("fill", data.color)
        .attr('opacity', function(d) {
            if (d > 1) {
                return 1;
            } else {
                return 0.6;
            }

        });
        

    // This is the 1x line
    chart.selectAll("line.baselineLift")
        .data([1])
        .enter().append("line")
        .attr("x1", function(d) { return x(d) + left_width; })
        .attr("x2", function(d) { return x(d) + left_width; })
        .attr("y1", 15)
        .attr("y2", baseHeight + 32)
        .style("fill", "#BBB")
        .attr('opacity', 1);

    // This is the 1x line label at the bottom
    chart.selectAll(".baselineLiftBottom")
        .data([1])
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", baseHeight + 50)
        .attr("dy", -6)
        .attr("dx", labelDx)
        .attr("font-weight", "semi-bold")
        .attr("font-size", "10px")
        .attr("text-anchor", "middle")
        .style("fill", "#666")
        .text(function(d) { return d; } );

    // This is the 1x line label at the top
    chart.selectAll(".baselineLiftTop")
        .data([1])
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", 15)
        .attr("dy", -6)
        .attr("dx", labelDx)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "middle")
        .style("fill", "#666")
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
        .style("fill", "#666")
        .text(function(d) { return $filter('number')(d, 1) + "x"; } );

    // This is the lift label to the right of the chart
    chart.append("text")
        .attr("x", width + 110)
        .attr("y", 5)
        .attr("dy", "0.36em")
        .attr("font-size", labelSize)
        .style("fill", "#999")
        .text(liftText);

    // These are the percent numbers to the right of the chart
    chart.selectAll("text.percentLeads")
        .data(percentLeads)
        .enter().append("text")
        .attr("x", width + 177)
        .attr("y", function(d, i) {
            return (i * (barHeight + 2 * gap)) + 42;
        })
        .attr("dx", -5)
        .attr("dy", commonDy)
        .attr("font-weight", "semi-bold")
        .attr("font-size", fontSize)
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "#666")
        .text(function(d) { return d + "%"; } );

    // This is the %Leads label to the right of the chart
    var leadsText = ResourceUtility.getString("TOP_PREDICTORS_HOVER_CHART_LEADS_LABEL").toUpperCase();
    chart.append("text")
        .attr("x", width + 142)
        .attr("y", 5)
        .attr("dy", "0.36em")
        .attr("font-size", labelSize)
        .style("fill", "#999")
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
        .attr("font-weight", "semi-bold")
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
                lineHeight = 1.05, // ems
                y = text.attr("y"),
                dy = parseFloat(text.attr("dy")),
                tspan = text.text(null).append("tspan").attr("x", left_width - 5).attr("y", y).attr("dy", dy + "em");
            for (var i = 0; i < wordLength; i++) {
                if (lineNumber >= 1) { break; }
                word = words.pop();
                line.push(word);
                tspan.text(line.join(" "));
                if (tspan.node().getComputedTextLength() > width) {
                    dy = -0.5;
                    tspan.attr("dy", dy + "em");
                    line.pop();
                    tspan.text(line.join(" ").substring(0, 18));
                    line = [word];
                    tspan = text.append("tspan").attr("x", left_width - 5).attr("y", y).attr("dy", ++lineNumber * lineHeight + dy + "em").text(word);
                }
            }
        });
    }

    $scope.showtitle = true;
})

.directive('topPredictorAttributeWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidgetTemplate.html'
    };
});