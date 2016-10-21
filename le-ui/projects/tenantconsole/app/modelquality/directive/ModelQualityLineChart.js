angular.module('app.modelquality.directive.ModelQualityLineChart', [
])
.directive('modelQualityLineChart', function ($window, $timeout) {
    return {
        restrict: 'AE',
        scope: {
            data: '='
        },
        link: function (scope, element, attr, ModelQualityLineChartVm) {
            scope.$on('resize', function () {
                resize();
            });

            scope.$on('$destroy', function () {
                $timeout.cancel(tooltipTimer);
            });

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
                .text(scope.data.title);

            var margin = {top: 20, right: 0, bottom: 40, left: 40},
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

            var x = d3.scaleTime().range([0, width]),
                x2 = d3.scaleTime().range([0, width]), // copy for zoom
                y = d3.scaleLinear().range([height, 0]),
                color = d3.scaleOrdinal(d3.schemeCategory20);

            var xAxis = d3.axisBottom(x),
                yAxis = d3.axisLeft(y);

            var line = d3.line()
                .curve(d3.curveLinear)
                .x(function(d) { return x(d.date); })
                .y(function(d) { return y(d.value); });

            var zoomed = function() {
                var t = d3.event.transform;
                x.domain(t.rescaleX(x2).domain());

                chart.selectAll(".line").attr("d", function (d) { return line(d.values); });
                chart.select(".axis--x").call(xAxis);
                chart.selectAll(".focus").attr("transform", "translate(" + d3.event.transform.x+",0) scale(" + d3.event.transform.k + ",1)");
                chart.selectAll("circle")
                    .attr("cx", function(d,i) { return x(d.date); })
                    .attr("cy", function(d,i) { return y(d.value); });
            };

            var zoom = d3.zoom()
                .scaleExtent([1, 365]) // 365x zoom down to hourly
                .translateExtent([[0, 0], [width, 0]])
                .extent([[0, 0], [width, height]])
                .on("zoom", zoomed);

            svg.call(zoom);

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

            var bisectDate = d3.bisector(function(d) { return d; }).left;

            var tooltipXDecorator = function (width) {
                return function (mouseX) {
                    if (mouseX + 230 > width) {
                        return mouseX - 220 + "px";
                    } else {
                        return mouseX + 75 + "px";
                    }
                };
            };
            var tooltipX = tooltipXDecorator(width);
            var hideTooltip = function () {
                tooltip.style("opacity", 0);
            };

            var showTooltip = function () {
                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);
            };

            var render = function () {
                var seriesData = scope.data.data;
                var extents = getExtents(seriesData);

                x.domain(extents.xExtent);
                y.domain(extents.yExtent);

                color.domain(seriesData.map(function(c) { return c.key; }));
                x2.domain(x.domain());
                // should not need to remove, use .enter() or .update()
                chart.selectAll("g").remove(); // empty all except zoom

                var legendRectSize = 18, legendSpacing = 4, charSize = 8;
                var prevOffset = 0;
                var legend = svg.selectAll('.legend')
                    .data(color.domain())
                    .enter()
                    .append('g')
                    .attr('class', 'legend')
                    .attr('transform', function (d, i) {
                        var offset = d.length * charSize + legendSpacing * 2;
                        var horz = prevOffset + offset;
                        var vert = height + margin.bottom;
                        prevOffset = horz + legendRectSize;

                        return 'translate(' + horz + ',' + vert + ')';
                    });

                legend.append('rect')
                    .attr('width', legendRectSize)
                    .attr('height', legendRectSize)
                    .style('fill', color)
                    .style('stroke', color);

                legend.append("text")
                    .attr("x", -legendSpacing)
                    .attr("y", 9)
                    .attr("dy", ".35em")
                    .attr("text-anchor", "end")
                    .text(function(d) { return d.toUpperCase(); });

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
                    .attr("cx", function(d,i) { return x(d.date); })
                    .attr("cy", function(d,i) { return y(d.value); })
                    .attr("fill", function (d,i) { return color(d.key); });

                chart.append("g")
                    .attr("class", "axis axis--x")
                    .attr("transform", "translate(0," + height + ")")
                    .call(xAxis);

                chart.append("g")
                    .attr("class", "axis axis--y")
                    .call(yAxis);

                var dateMap = seriesData.reduce(function (set, cur) {
                    cur.values.forEach(function(val) {
                        set[val.date] = val.date;
                    });
                    return set;
                }, {});

                var dateSet = Object.keys(dateMap).map(function (key) {
                    return dateMap[key];
                }).sort(function(a, b) {
                    if (!(a instanceof Date) || !(b instanceof Date)) {
                        return new Date(a) - new Date(b);
                    } else {
                        return a - b;
                    }
                });

                focus.on("mouseenter", function() {
                    hoverVerticalLine.style("display", null);
                    showTooltip();
                })
                .on("mouseleave", function() {
                    hideTooltip();
                    hoverVerticalLine.style("display", "none");
                    dots.style("r", "0px");
                })
                .on("mousemove", function () {
                    // edges not triggering mouseout
                    // workaround for closing tooltip
                    $timeout.cancel(tooltipTimer);
                    tooltipTimer = $timeout(hideTooltip, 1500);

                    var mouse = d3.mouse(this);
                    var mouseDate = x.invert(mouse[0]);
                    var i = bisectDate(dateSet, mouseDate);
                    var date = dateSet[i];
                    var translateXBy = x(date);
                    if (!isNaN(translateXBy)) {
                        hoverVerticalLine.attr("transform", "translate("+ translateXBy + ", 0)");
                    }

                    var tooltipData = {};
                    var showTooltip = false;

                    dots.style("r", function (d, i) {
                        if (date - d.date === 0) {
                            showTooltip = true;
                            tooltipData[d.key] = d.value;
                            return '3px';
                        } else {
                            return '0px';
                        }
                    });

                    if (showTooltip) {
                        var template = date.toLocaleString() + '<br>';

                        for (var key in tooltipData) {
                            template += key + ': ' + tooltipData[key] + '<br>';
                        }

                        tooltip.html(template)
                            .style("left", function () {
                                return tooltipX(mouse[0]);
                            })
                            .style("top", mouse[1] + "px");
                    }
                });

                // initial zoom and pan
                var s = 13;
                var t = d3.zoomIdentity
                    .scale(s)
                    .translate(-width*(s-1)/s, 0);
                svg.transition()
                    .duration(750).call(zoom.transform, t);
            };

            scope.$watch('data', function (newData, oldData) {
                render();
            }, true);

            var ignoreFirst = true;
            var resize = function () {
                if (ignoreFirst) {
                    ignoreFirst = false;
                    return;
                }

                width = container.clientWidth - margin.left - margin.right;
                height = container.clientHeight - margin.top - margin.bottom;

                svg.attr("width", container.clientWidth)
                    .attr("height", container.clientHeight);

                svg.select(".zoom")
                    .attr("width", width)
                    .attr("height", height);

                x = d3.scaleTime().range([0, width]);
                x2 = d3.scaleTime().range([0, width]);

                xAxis.scale(x);

                svg.select("clipPath rect")
                    .attr("width", width)
                    .attr("height", height);

                zoom.translateExtent([[0, 0], [width, 0]])
                    .extent([[0, 0], [width, height]]);

                tooltipX = tooltipXDecorator(width);

                render();
            };

            var getExtents = function (series) {
                var minDate = new Date(),
                    maxDate = new Date(0),
                    maxValue = Number.NEGATIVE_INFINITY,
                    minValue = 0;

                series.forEach(function (serie) {
                    serie.values.forEach(function (d) {
                        maxDate = Math.max(maxDate, d.date);
                        minDate = Math.min(minDate, d.date);
                        maxValue = Math.max(maxValue, d.value);
                        //minValue = Math.min(minValue, d.value);
                    });
                });

                return {
                    xExtent: [new Date(minDate), new Date(maxDate)],
                    yExtent: [minValue, maxValue * 1.05]
                };
            };

        },
        controller: 'ModelQualityLineChartCtrl',
        controllerAs: 'ModelQualityLineChartVm'
    };
})
.controller('ModelQualityLineChartCtrl', function ($scope) {

});
