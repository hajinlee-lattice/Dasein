angular.module('app.modelquality.directive.ModelQualityGroupBarChart', [
])
.directive('modelQualityGroupBarChart', function ($timeout) {
    return {
        restrict: 'AE',
        scope: {
            data: '='
        },
        link: function (scope, element, attr, ModelQualityGroupBarChartVm) {

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

            var options = ['RocScore', 'Top10PercentLift', 'Top20PercentLift','Top30PercentLift'];

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

            var margin = {top: 20, right: 20, bottom: 40, left: 40},
                width = container.clientWidth - margin.left - margin.right,
                height = container.clientHeight - margin.top - margin.bottom;

            var svg = d3container.append("svg")
                .attr("width", container.clientWidth)
                .attr("height", container.clientHeight);

            var chart = svg.append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            var x0 = d3.scaleBand().rangeRound([0, width]).padding(1/3).align(0.5),
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

                var legendRectSize = 12,
                    legendSpacing = 4,
                    charSize = 8,
                    vert = height + margin.bottom,
                    prevOffset = 0;

                var legend = svg.selectAll(".legend")
                    .data(color.domain())
                    .enter()
                    .append("g")
                    .attr("class", "legend");

                legend.append("rect")
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
                        return height - y(d.value[key]);
                    })
                    .style("fill", function(d) {
                        return color(d.category);
                    });

                bars.on("mouseenter", function(d,i) {
                    var template = '';
                    template += 'Dataset: ' + d.description.dataset + '<br>';
                    template += 'Pipeline: ' + d.description.pipeline + '<br>';

                    template += _.map(d.value, function (value, metric) {
                        return metric + ': ' + value;
                    }).join('<br>');

                    tooltip.html(template);

                    var tooltipWidth = tooltip.nodes()[0].offsetWidth;
                    if (d3.event.offsetX < tooltipWidth || d3.event.offsetX < 250) {
                        tooltip.style("right", width - (d3.event.offsetX + tooltipWidth) + "px");

                    } else {
                        tooltip.style("right", width - d3.event.offsetX + 30 + "px");
                    }

                    showTooltip();

                    $timeout.cancel(tooltipTimer);
                    tooltipTimer = $timeout(hideTooltip, 1500);
                })
                .on("mouseleave", function() {
                    hideTooltip();
                })
                .on("mousemove", function () {
                    tooltip.style("top", d3.event.offsetY + "px");

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
                yAxis.scale(y);

                chart.select('.y.axis').transition().duration(200).call(yAxis);
                bars.transition().duration(200)
                    .attr("y", function(d) { return y(d.value[key]); })
                    .attr("height", function(d) {
                        return height - y(d.value[key]);
                    });
            };

            var resize = function () {
                width = container.clientWidth - margin.left - margin.right;
                height = container.clientHeight - margin.top - margin.bottom;

                svg.attr("width", container.clientWidth)
                    .attr("height", container.clientHeight);

                x0.rangeRound([0, width]);
                xAxis.scale(x0);

                render();
            };

            render();

            scope.$on('resize', function () {
                resize();
            });

        },
        controller: 'ModelQualityGroupBarChartCtrl',
        controllerAs: 'ModelQualityGroupBarChartVm'
    };
})
.controller('ModelQualityGroupBarChartCtrl', function ($scope) {

});
