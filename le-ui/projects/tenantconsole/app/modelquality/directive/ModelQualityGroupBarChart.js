angular.module('app.modelquality.directive.ModelQualityGroupBarChart', [
])
.directive('modelQualityGroupBarChart', function ($timeout) {
    return {
        restrict: 'AE',
        scope: {
            data: '='
        },
        link: function (scope, element, attr, ModelQualityGroupBarChartVm) {
            scope.$on('resize', function () {
                resize();
            });

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

            var options = ['RocScore', 'Top10PercentLift','Top20PercentLift','Top30PercentLift'];

            var dropdown = d3container.append("select")
                .attr("class", "chart-metric-menu")
                .attr("name", "metric")
                .on("change", function (_, i, el) {
                    key = options[el[i].selectedIndex];
                    changeKey();
                });

            dropdown.selectAll("option")
                .data(options)
                .enter()
                .append("option")
                .attr("value", function (d) { return d; })
                .text(function (d) { return d; });

            var key = options[0];

            var margin = {top: 20, right: 150, bottom: 20, left: 40},
                width = container.clientWidth - margin.left - margin.right,
                height = container.clientHeight - margin.top - margin.bottom;

            var svg = d3container.append("svg")
                .attr("width", container.clientWidth)
                .attr("height", container.clientHeight);

            var chart = svg.append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            var x0 = d3.scaleBand().rangeRound([0, width]).padding(0.5).align(0.5),
                x1 = d3.scaleBand(),
                y = d3.scaleLinear().range([height, 0]);

            var color = d3.scaleOrdinal(d3.schemeCategory20);
            var xAxis = d3.axisBottom(x0).tickValues([]);
                yAxis = d3.axisLeft(y);

            var hideTooltip = function () {
                tooltip.style("opacity", 0);
            };

            var showTooltip = function () {
                tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);
            };

            var data, groups, bars;
            var render = function () {
                data = scope.data.data;
                var categories = scope.data.data[0].categories.map(function(set) {
                    return set.category;
                });

                x0.domain(data.map(function(d) { return d.dataset; }));
                x1.domain(categories).rangeRound([0, x0.bandwidth()]);
                y.domain([0, d3.max(data, function(d) {
                    return d3.max(d.categories, function(d) {
                        return d.value[key];
                    });
                })]);
                color.domain(categories);

                chart.selectAll("g").remove();

                var legendRectSize = 18, legendSpacing = 4, charSize = 8;
                var prevOffset = 0;
                var legend = svg.selectAll('.legend')
                    .data(color.domain())
                    .enter()
                    .append('g')
                    .attr('class', 'legend')
                    .attr('transform', function (d, i) {
                        var legendHeight = legendRectSize + legendSpacing;
                        var offset =  legendHeight * color.domain().length / 2;
                        var horz = -2 * legendRectSize;
                        var vert = height  - i * legendHeight - offset;
                        return 'translate(' + (width + margin.right) + ',' + (vert + margin.bottom)  + ')';
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

                chart.append("g")
                    .attr("class", "x axis")
                    .attr("transform", "translate(0," + height + ")")
                    .call(xAxis);

                chart.append("g")
                    .attr("class", "y axis")
                    .call(yAxis);

                groups = chart.selectAll(".category")
                    .data(data)
                    .enter().append("g")
                    .attr("class", "category")
                    .attr("transform", function(d) { return "translate(" + x0(d.dataset) + ",0)"; });

                bars = groups.selectAll("rect")
                    .data(function(d) { return d.categories; })
                    .enter().append("rect")
                    .attr("x", function(d) { return x1(d.category); })
                    .attr("y", function(d) { return y(d.value[key]); })
                    .attr("width", x1.bandwidth())
                    .attr("height", function(d) {
                        return height - y(d.value[key]); })
                    .style("fill", function(d) {
                        return color(d.category); });

                bars.on("mouseenter", function(d,i) {
                    var template = '';
                    template += 'Dataset: ' + d.description.dataset + '<br>';
                    template += 'Pipeline: ' + d.description.pipeline + '<br>';

                    template += Object.keys(d.value).map(function (metric) {
                        return metric + ': ' + d.value[metric];
                    }).join('<br>');

                    tooltip.html(template)
                        .style("right", "0px")
                        .style("top", "0px");

                    showTooltip();
                    $timeout.cancel(tooltipTimer);
                    tooltipTimer = $timeout(hideTooltip, 1500);
                })
                .on("mouseleave", function() {
                    hideTooltip();
                })
                .on("mousemove", function () {
                    $timeout.cancel(tooltipTimer);
                    tooltipTimer = $timeout(hideTooltip, 1500);
                });

            };

            var changeKey = function () {
                y.domain([0, d3.max(data, function(d) {
                    return d3.max(d.categories, function(d) {
                        return d.value[key];
                    });
                })]);
                yAxis = d3.axisLeft(y);

                chart.select('.y.axis').transition().duration(200).call(yAxis);
                bars.transition().duration(200)
                    .attr("y", function(d) { return y(d.value[key]); })
                    .attr("height", function(d) {
                        return height - y(d.value[key]);
                    });
            };

            var _skipFirst = true;
            var resize = function () {
                if (_skipFirst) {
                    _skipFirst = false;
                    return;
                }
                width = container.clientWidth - margin.left - margin.right;
                height = container.clientHeight - margin.top - margin.bottom;

                svg.attr("width", container.clientWidth)
                    .attr("height", container.clientHeight);

                x0 = d3.scaleBand().rangeRound([0, width]).padding(0.5).align(0.5);
                x1 = d3.scaleBand();
                xAxis.scale(x0);
                render();
            };

            scope.$watch('data', function (newData, oldData) {
                render();
            });

        },
        controller: 'ModelQualityGroupBarChartCtrl',
        controllerAs: 'ModelQualityGroupBarChartVm'
    };
})
.controller('ModelQualityGroupBarChartCtrl', function ($scope) {

});
