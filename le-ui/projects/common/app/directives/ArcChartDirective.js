angular.module('mainApp.appCommon.directives.charts.ArcChartDirective', [])
.directive('arcChart', function ($window) {
    return {
        restrict: 'EA',
        scope: {},
        link: function (scope, element, attrs) {
            var chartSize = attrs.chartSize;
            var color = attrs.chartColor;
            var value = attrs.chartValue;
            var total = attrs.chartTotal;
            var label = attrs.chartLabel;
            // Browser onresize event
            window.onresize = function() {
                scope.$apply();
            };
            
            // Watch for resize event
            scope.$watch(function () {
                return angular.element($window)[0].innerWidth;
            }, function () {
                scope.render();
            });
            
            scope.render = function () {
                
                var twoPi = 2 * Math.PI;
                $(element[0]).empty();
                var svg = d3.select(element[0]).append("svg")
                    .attr("width", chartSize)
                    .attr("height", chartSize)
                    .append("g")
                    .attr("transform", "translate(" + chartSize / 2 + "," + chartSize / 2 + ")");
                var arc = d3.svg.arc()
                    .startAngle(0)
                    .innerRadius((chartSize/2)-2)
                    .outerRadius(chartSize/2);
                
                // Add the background arc, from 0 to 100% (τ).
                var background = svg.append("path")
                    .datum({endAngle: twoPi})
                    .style("fill", "#DDDDDD")
                    .attr("d", arc);
                
                // Add the foreground arc
                var foreground = svg.append("path")
                    .datum({endAngle: 0})
                    .style("fill", color)
                    .attr("d", arc);
                
                function arcTween(transition, newAngle) {
                    transition.attrTween("d", function(d) {
                        var interpolate = d3.interpolate(d.endAngle, newAngle);
                        return function(t) {
                            d.endAngle = interpolate(t);
                            return arc(d);
                        };
                    });
                }
                
                foreground
                    .transition()
                    .duration(1000)
                    .call(arcTween, (value/total) * twoPi);
                
                // Add value to the middle of the arc
                svg.append("text")
                    .attr("class", "arc-chart-value")
                    .style("fill", "#555555")
                    .attr("font-size", "12px")
                    .attr("dx", "-0.55em")
                    .attr("dy", ".37em")
                    .text(value);
            };
        }
    };
});