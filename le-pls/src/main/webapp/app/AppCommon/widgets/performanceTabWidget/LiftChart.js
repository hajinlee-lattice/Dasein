angular.module('mainApp.appCommon.widgets.performanceTab.LiftChart', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.directive('liftChart', function () {
    return {
        template: '<div id="liftChart" class="lift-chart"></div>',
        scope: {data: "="},
        controller: function ($scope, $filter, ResourceUtility) {
            if ($scope.data == null) {
                return;
            }

            var data = $scope.data;
            var targetSet = true;
            //==================================================
            // Specify Dimensions
            //==================================================
            var width = 840,
                margin = {top: 5, right: 0, bottom: 69, left: 936 - width},
                height = 363 - margin.top - margin.bottom;

            // iterators
            var i;

            //==================================================
            // Max Lift and Lift Ticks
            //==================================================
            var maxLift, nTicks;

            maxLift = d3.max(data) * 1.1;   // 10% extra height for the lift label of the highest bar
            var halfIntegerTicks = maxLift <= 5.5; // show half integer ticks only if max lift is <= 5.0

            if (halfIntegerTicks) {
                nTicks = Math.ceil(maxLift * 2.0);
                maxLift = nTicks * 0.5;
            } else {
                nTicks = Math.ceil(maxLift);
                maxLift = nTicks;
            }

            //==================================================
            // Define Axes
            //==================================================
            var x = d3.scale.linear().range([0, width]);
            var y = d3.scale.linear().range([height, 0]);
            var yAxis = d3.svg.axis()
                .scale(y)
                .ticks(nTicks)
                .innerTickSize(3)
                .orient("left");

            //==================================================
            // Specify Tick Formats
            //==================================================
            if (halfIntegerTicks) {
                yAxis.tickFormat(function(d) {
                    return (d * 2) % 1 === 0 ? $filter("number")(d, 1) : "";
                });
            } else {
                yAxis.tickFormat(function(d) {
                    return d % 1 === 0 ? $filter("number")(d, 1) : "";
                });
            }


            //==================================================
            // Define Domains
            //==================================================
            x.domain([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            y.domain([0, maxLift]);

            //==================================================
            // Append Primary
            //==================================================
            var svg = d3.select("#liftChart").append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            //==================================================
            // Append Grey Background
            //==================================================
            svg.append("rect")
                .attr("id", "upper-gradient")
                .attr("x", 0)
                .attr("y", 0)
                .attr("width", width)
                .attr("height", height)
                .attr("fill", "#f2f4f8");

            //==================================================
            // Append Grid Lines and x Ticks
            //==================================================
            for (i = 1; i < 10; i++) {
                var xLoc = x(i * 0.1);
                svg.append("line")
                    .attr("x1", xLoc)
                    .attr("y1", "0")
                    .attr("x2", xLoc)
                    .attr("y2", height)
                    .attr("stroke-width", "1")
                    .style("stroke", "white");
            }

            for (i = 0; i < 10; i++) {
                svg.append("text")
                    .attr("x", x(i * 0.1 + 0.05))
                    .attr("y", height + 15)
                    .style("text-anchor", "middle")
                    .style("font-size", "9px")
                    .style("font-family", "ProximaNova-Semibold")
                    .style("fill", "#aaa")
                    .text(bucketLabel(i));
            }

            function bucketLabel(i) {
                var start = 91 - (i * 10);
                var end = 100 - (i * 10);
                return String(start) + " - " + String(end);
            }

            //==================================================
            // Append Axes
            //==================================================
            svg.append("g")
                .attr("class", "y axis")
                .attr("transform", "translate(-3, 0)")
                .style("font-size", "9px")
                .style("font-weight", "700")
                .style("fill", "#999")
                .call(yAxis);

            svg.append("text")
                .attr("transform", "translate(" + (-55) + ", " + (height / 2) + ") rotate(-90)")
                .style("text-anchor", "middle")
                .style("font-size", "12px")
                .style("font-family", "ProximaNova-Semibold")
                .style("fill", "#aaa")
                .text(ResourceUtility.getString("LIFT_CHART_Y_AXIS_LABEL"));

            svg.append("text")
                .attr("transform", "translate(" + x(0.5) + ", " + (height + 58) + ")")
                .style("text-anchor", "middle")
                .style("font-size", "12px")
                .style("font-family", "ProximaNova-Semibold")
                .style("fill", "#aaa")
                .text(ResourceUtility.getString("LIFT_CHART_X_AXIS_LABEL"));

            //==================================================
            // Apply Tick Coloring
            //==================================================
            d3.selectAll(".axis g.tick line").style("stroke", "#999");

            //==================================================
            // Hide Axis Paths
            //==================================================
            d3.selectAll(".axis path").style("display", "none");


            //==================================================
            // Append Bars
            //==================================================

            data.forEach(function(lift, index){
                drawBar(
                    x(0.1 * (index + 0.5)),
                    lift,
                    lift >= 1.0 ? '#477cba' : '#f6b300',
                    lift >= 1.0 ? '#4376b1' : '#e9aa00'
                );
            });

            function drawBar(xCent, lift, majorColor, minorColor) {
                var yTop = y(lift);
                svg.append("rect")
                    .attr("x", xCent - 33)
                    .attr("y", yTop)
                    .attr("width", 59)
                    .attr("height",  y(0) - yTop)
                    .attr("fill", majorColor);
                svg.append("rect")
                    .attr("x", xCent + 26)
                    .attr("y", yTop)
                    .attr("width", 7)
                    .attr("height", y(0) - yTop)
                    .attr("fill", minorColor);
                svg.append("text")
                    .attr("x", xCent)
                    .attr("y", yTop - 5)
                    .style("text-anchor", "middle")
                    .style("font-size", "16px")
                    .style("font-family", "ProximaNova-Bold")
                    .style("fill", "#333333")
                    .text(lift2str(lift));
            }

            //==================================================
            // Append Unity Lift Level
            //==================================================
            drawLiftLevel(1);

            function drawLiftLevel(lift) {
                var yLoc = y(lift);
                //==================================================
                // Level Grid Line
                //==================================================
                svg.append("line")
                    .attr("x1", 0)
                    .attr("y1", yLoc)
                    .attr("x2", x(1))
                    .attr("y2", yLoc)
                    .attr("stroke-width", "1")
                    .style("stroke-dasharray", ("2, 2"))
                    .style("stroke", "#bbbbbb");

                //==================================================
                // Append Level Marker
                //==================================================
                var points = [
                    {"x": -3, "y": yLoc},
                    {"x": -8, "y": yLoc + 7},
                    {"x": -23, "y": yLoc + 7},
                    {"x": -24, "y": yLoc + 6},
                    {"x": -24, "y": yLoc - 6},
                    {"x": -23, "y": yLoc - 7},
                    {"x": -8, "y":  yLoc - 7}
                ];

                function points2str(pts) {
                    return pts.map(function(pt){
                        return [String(pt.x), String(pt.y)].join(",");
                    }).join(" ");
                }

                svg.append("polygon")
                    .attr("points", points2str(points))
                    .attr("fill","#666");

                svg.append("text")
                    .attr("x", -15)
                    .attr("y", yLoc + 3.5)
                    .style("text-anchor", "middle")
                    .style("font-size", "11px")
                    .style("font-family", "ProximaNova-Bold")
                    .style("fill", "white")
                    .text($filter("number")(lift, 1));
            }

            function lift2str(lift) {
                return $filter("number")(lift, 1) + "x";
            }
        }
    };
});