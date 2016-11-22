angular.module('app.modelquality.directive.ModelQualityLineChart', [
])
.directive('modelQualityLineChart', function ($window, $timeout) {
    return {
        restrict: 'AE',
        scope: {
            promise: '=',
            title: '='
        },
        link: function (scope, element, attr, ModelQualityLineChartVm) {

            var chartId = new Date().getTime();

            var container = element[0];
            $(container).empty();
            var d3container = d3.select(container);

            var tooltipTimer = null;
            var tooltip = d3container
                .append("div")
                .attr("class", "chart-tooltip")
                .style("opacity", 0);

            var title = d3container.append("div")
                .attr("class", "chart-title")
                .text(scope.title);

            var margin = {top: 20, right: 20, bottom: 40, left: 40},
                width = container.clientWidth - margin.left - margin.right,
                height = container.clientHeight - margin.top - margin.bottom;

            var svg = d3container.append("svg")
                .attr("width", container.clientWidth)
                .attr("height", container.clientHeight);

            svg.append("defs").append("clipPath")
                .attr("id", "clip-" + chartId)
                .append("rect")
                .attr("width", width)
                .attr("height", height);

            var x = d3.scalePoint().range([0, width]),
                y = d3.scaleLinear().range([height, 0]),
                color = d3.scaleOrdinal(d3.schemeCategory20);

            var xAxis = d3.axisBottom(x).tickValues([]),
                yAxis = d3.axisLeft(y);

            var line = d3.line()
                .curve(d3.curveLinear)
                .x(function(d) { return x(d.x); })
                .y(function(d) { return y(d.y); });

            var chart = svg.append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            var hoverVerticalLine = chart.append("line")
                .attr("class", "chart-hover-line")
                .attr("x1", 0)
                .attr("y1", 0)
                .attr("x2", 0)
                .attr("y2", height);

            var focus = chart.append("rect")
                .attr("class", "chart-focus")
                .attr("width", width)
                .attr("height", height)
                .style("fill", "transparent");

            var tooltipXPosWrap = function (width) {
                return function (mouseX) {
                    if (mouseX + 230 > width) {
                        return mouseX - 220 + "px";
                    } else {
                        return mouseX + 75 + "px";
                    }
                };
            };
            var tooltipXPos = tooltipXPosWrap(width);
            var hideTooltip = function () {
                tooltip.style("opacity", 0);
            };

            var showTooltip = function () {
                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);
            };

            var render = function () {
                var seriesData = scope.data;
                var extents = getExtents(seriesData);

                y.domain(extents.yExtent);
                x.domain(extents.xDomain);

                color.domain(seriesData.map(function(c) { return c.key; }));
                // should not need to remove, use .enter() or .update()
                chart.selectAll("g").remove();

                var legendRectSize = 12,
                    legendSpacing = 4,
                    charSize = 8,
                    vert = height + margin.bottom,
                    prevOffset = 0;

                var legend = svg.selectAll('.legend')
                    .data(color.domain())
                    .enter()
                    .append('g')
                    .attr('class', 'legend');

                legend.append('rect')
                    .attr("width", legendRectSize)
                    .attr("height", legendRectSize/2)
                    .style("fill", color)
                    .style("stroke", color);

                legend.append("text")
                    .attr("x", legendRectSize + legendSpacing)
                    .attr("dy", ".35em")
                    .attr("text-anchor", "start")
                    .attr("dominant-baseline", "middle")
                    .text(function(d) { return d.toUpperCase(); });

                legend.each(function(d, i) {
                    var self = d3.select(this);
                    self.attr("transform", "translate(" + prevOffset + "," + vert + ")");

                    prevOffset += self.node().getBBox().width + legendSpacing;
                });

                var series = chart.selectAll(".series")
                    .data(seriesData)
                    .enter().append("g")
                    .attr("class", "series");

                series.append("path")
                    .attr("class", "line")
                    .attr("clip-path", "url(#clip-" + chartId + ")")
                    .attr("d", function (d) { return line(d.values); })
                    .style("stroke", function(d) { return color(d.key); });

                var dots = series.append("g").selectAll("circle")
                    .data(function (d) { return d.values; })
                    .enter()
                    .append("circle")
                    .attr("cx", function(d,i) { return x(d.x); })
                    .attr("cy", function(d,i) { return y(d.y); })
                    .attr("fill", function (d,i) { return color(d.key); });

                chart.append("g")
                    .attr("class", "axis axis--x")
                    .attr("transform", "translate(0," + height + ")")
                    .call(xAxis);

                chart.append("g")
                    .attr("class", "axis axis--y")
                    .call(yAxis);

                focus.on("mouseenter", function() {
                    hoverVerticalLine.style("display", null);
                    showTooltip();
                }).on("mouseleave", function() {
                    hideTooltip();
                    hoverVerticalLine.style("display", "none");
                    dots.style("r", "0px");
                }).on("mousemove", function () {
                    var mouse = d3.mouse(this);
                    var xPos = mouse[0];
                    var tippedPos = -1;
                    var domain = x.domain();

                    for (var i = 1; i < domain.length; i++) {
                        var left = x(domain[i-1]);
                        var right = x(domain[i]);

                        if (left < xPos && right >= xPos) {
                            tippedPos = (xPos - left) < (right - xPos) ? i - 1 : i;
                            break;
                        }
                    }

                    $timeout.cancel(tooltipTimer);
                    tooltipTimer = $timeout(hideTooltip, 1500);
                    var tippedKey = domain[tippedPos];
                    var translateXBy = x(tippedKey);

                    hoverVerticalLine.attr("transform", "translate("+ translateXBy + ", 0)");

                    var tooltipData = {};
                    tooltipData.AnalyticPipeline = tippedKey;

                    dots.style("r", function (d, i) {
                        if (d.x === tippedKey) {
                            tooltipData[d.key] = d.y;
                            return '3px';
                        } else {
                            return '0px';
                        }
                    });

                    var template = '';

                    for (var key in tooltipData) {
                        template += key + ': ' + tooltipData[key] + '<br>';
                    }

                    tooltip.html(template)
                        .style("left", function () {
                            return tooltipXPos(xPos);
                        }).style("top", mouse[1] + "px");
                });

            };

            var resize = function () {

                width = container.clientWidth - margin.left - margin.right;
                height = container.clientHeight - margin.top - margin.bottom;

                svg.attr("width", container.clientWidth)
                    .attr("height", container.clientHeight);

                focus.attr("width", width);

                x.range([0, width]);
                xAxis.scale(x);

                svg.select("clipPath rect")
                    .attr("width", width)
                    .attr("height", height);

                tooltipX = tooltipXPosWrap(width);

                render();
            };

            var getExtents = function (series) {
                var minValue = 0,
                    maxValue = Number.NEGATIVE_INFINITY;

                var xDomain = {};

                series.forEach(function (serie) {
                    serie.values.forEach(function (d) {
                        maxValue = Math.max(maxValue, d.y);
                        xDomain[d.x] = d.x;
                    });
                });

                return {
                    xDomain: _.map(xDomain, function (x) {
                        return x;
                    }).sort(function(a,b) { return (a > b) ? 1 : ((a < b) ? -1 : 0); }),
                    yExtent: [minValue, maxValue * 1.10]
                };
            };

            scope.promise.then(function(result) {
                scope.data = result;
                render();
            });

            scope.$on('resize', function () {
                resize();
            });
        },
        controller: 'ModelQualityLineChartCtrl',
        controllerAs: 'ModelQualityLineChartVm'
    };
})
.controller('ModelQualityLineChartCtrl', function ($scope) {

});
