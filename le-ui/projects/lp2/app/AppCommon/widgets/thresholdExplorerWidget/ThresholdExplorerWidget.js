angular.module('mainApp.appCommon.widgets.ThresholdExplorerWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('ThresholdExplorerWidgetController', function ($scope, ResourceUtility, WidgetFrameworkService) {
    var widgetConfig = $scope.widgetConfig;
    var modelSummary = $scope.data;

    if (modelSummary == null) {
        return;
    }

    var data = modelSummary.ThresholdChartData;
    var targetSet = true;
    //==================================================
    // Specify Dimensions
    //==================================================
    var margin = {top: 47, right: 150, bottom: 89, left: 204},
        width = 1154 - margin.left - margin.right,
        height = 336 - margin.top - margin.bottom;

    //==================================================
    // Define Axes
    //==================================================
    var x = d3.scale.linear()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .ticks(100)
        .innerTickSize(3)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .ticks(100)
        .innerTickSize(3)
        .orient("left");

    //==================================================
    // Specify Tick Formats
    //==================================================
    xAxis.tickFormat(function(d) {
        return d % 10 === 0 ? d : "";
    });
    
    yAxis.tickFormat(function(d) {
        return d % 10 === 0 ? d : "";
    });

    //==================================================
    // Define Domains
    //==================================================
    x.domain(d3.extent(data, function(d) { return d.leads; }));
    y.domain(d3.extent(data, function(d) { return d.conversions; }));

    //==================================================
    // Define Line/Area
    //==================================================
    var line = d3.svg.line()
        .x(function(d) { return x(d.leads); })
        .y(function(d) { return y(d.conversions); });

    var	lowerArea = d3.svg.area()
        .x(function(d) { return x(d.leads); })
        .y0(height)
        .y1(function(d) { return y(d.conversions); });

    var upperArea = d3.svg.area()
        .x(function(d) { return x(d.leads); })
        .y0(-1)
        .y1(function(d) { return y(d.conversions); });

    //==================================================
    // Append Primary
    //==================================================
    var svg = d3.select("#thresholdExplorerChart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .style("cursor", "pointer")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    //==================================================
    // Append Lower Area (With Gradient)
    //==================================================
    svg.append("linearGradient")
        .attr("id", "lower-gradient")
        .attr("x1", "0%")
        .attr("y1", "0%")
        .attr("x2", "0%")
        .attr("y2", "100%")
        .attr("spreadMethod", "pad")
      .selectAll("stop")
        .data([{offset: "0%", color: "#6aa9ff", opacity: 0.6},
               {offset: "100%", color: "#6aa9ff", opacity: 0.1}])
      .enter().append("stop")
        .attr("offset", function(d) { return d.offset; })
        .attr("stop-color", function(d) { return d.color; })
        .attr("stop-opacity", function(d) { return d.opacity; });

    svg.append("path")
        .datum(data)
        .style("fill", "url(#lower-gradient)")
        .attr("d", lowerArea);

    //==================================================
    // Append Upper Area (With Gradient)
    //==================================================
    svg.append("linearGradient")
        .attr("id", "upper-gradient")
        .attr("x1", "0%")
        .attr("y1", "0%")
        .attr("x2", "100%")
        .attr("y2", "100%")
        .attr("spreadMethod", "pad")
      .selectAll("stop")
        .data([{offset: "0%", color: "#f2f4f8", opacity: 1.0},
               {offset: "100%", color: "#f2f4f8", opacity: 1.0}])
      .enter().append("stop")
        .attr("offset", function(d) { return d.offset; })
        .attr("stop-color", function(d) { return d.color; })
        .attr("stop-opacity", function(d) { return d.opacity; });

    svg.append("path")
        .datum(data)
        .style("fill", "url(#upper-gradient)")
        .attr("d", upperArea);

    //==================================================
    // Append Axes
    //==================================================
    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + (height + 3) + ")")
        .style("font-size", "9px")
        .style("font-weight", "700")
        .style("fill", "#999")
        .call(xAxis)
      .append("text")
        .attr("transform", "translate(" + (width / 2) + " , 77)")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .style("font-family", "ProximaNova-Semibold")
        .style("fill", "#aaa")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_X_AXIS_LABEL"));

    svg.append("g")
        .attr("class", "y axis")
        .attr("transform", "translate(-3, 0)")
        .style("font-size", "9px")
        .style("font-weight", "700")
        .style("fill", "#999")
        .call(yAxis)
      .append("text")
        .attr("transform", "translate(" + (-100) + ", " + (height / 2) + ") rotate(-90)")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .style("font-family", "ProximaNova-Semibold")
        .style("fill", "#aaa")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_Y_AXIS_LABEL"));

    //==================================================
    // Apply Tick Coloring
    //==================================================
    d3.selectAll(".axis g.tick line").style("stroke", function(d) {
        return d % 10 === 0 ? "#666" : "#ddd";
    });

    //==================================================
    // Hide Axis Paths
    //==================================================
    d3.selectAll(".axis path").style("display", "none");

    //==================================================
    // Append Grid Lines
    //==================================================
    for (var i = 1; i < 10; i++) {
        var xLoc = x(i * 10);
        svg.append("line")
            .attr("x1", xLoc)
            .attr("y1", "0")
            .attr("x2", xLoc)
            .attr("y2", height)
            .attr("stroke-width", "1")
            .style("stroke", "white");
    }

    //==================================================
    // Append Data Line
    //==================================================
    svg.append("path")
        .datum(data)
        .style("fill", "none")
        .style("stroke", "#2579ad")
        .style("stroke-width", "2")
        .attr("d", line);

    //==================================================
    // Append Borders
    //==================================================
    svg.append("line")
        .attr("x1", "0")
        .attr("y1", "0")
        .attr("x2", "0")
        .attr("y2", height)
        .style("stroke", "#ccc");

    svg.append("line")
        .attr("x1", width)
        .attr("y1", "0")
        .attr("x2", width)
        .attr("y2", height)
        .style("stroke", "#ccc");

    //==================================================
    // Append Info Elements
    //==================================================
    var infoElements = svg.append("g");

    infoElements.append("line")
        .attr("class", "x")
        .attr("x1", -34)
        .attr("y1", "0")
        .attr("x2", width)
        .attr("y2", "0")
        .attr("stroke-dasharray", "2 2")
        .style("stroke", "#666");

    infoElements.append("line")
        .attr("class", "y")
        .attr("x1", "0")
        .attr("y1", "0")
        .attr("x2", "0")
        .attr("y2", height + 62)
        .attr("stroke-dasharray", "2 2")
        .style("stroke", "#666");

    infoElements.append("circle")
        .attr("class", "y")
        .style("fill", "white")
        .style("stroke", "#2579ad")
        .style("stroke-width", "2")
        .attr("r", 6.5);

    infoElements.append("text")
        .attr("class", "rltext")
        .style("text-anchor", "start")
        .style("fill", "#333")
        .style("font-size", "11px")
        .style("font-family", "ProximaNova-Bold")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_LIFT_LABEL"));

    infoElements.append("text")
        .attr("class", "rtext")
        .style("text-anchor", "start")
        .style("fill", "#333")
        .style("font-size", "18px")
        .style("font-family", "ProximaNova-Bold");

    infoElements.append("text")
        .attr("class", "lltext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#333")
        .style("font-size", "11px")
        .style("font-family", "ProximaNova-Bold")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_LIFT_LABEL"));

    infoElements.append("text")
        .attr("class", "ltext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#333")
        .style("font-size", "18px")
        .style("font-family", "ProximaNova-Bold");

    infoElements.append("text")
        .attr("class", "xltext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#333")
        .style("font-size", "11px")
        .style("font-family", "ProximaNova-Bold")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_CONVERSION_LABEL"));

    infoElements.append("text")
        .attr("class", "xtext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#477cba")
        .style("font-size", "18px")
        .style("font-family", "ProximaNova-Bold");

    infoElements.append("text")
        .attr("class", "lyltext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#333")
        .style("font-size", "11px")
        .style("font-family", "ProximaNova-Bold")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_LEADS_LABEL"));

    infoElements.append("text")
        .attr("class", "lytext")
        .style("text-anchor", "end")
        .style("startOffset", "100%")
        .style("fill", "#477cba")
        .style("font-size", "18px")
        .style("font-family", "ProximaNova-Bold");

    infoElements.append("text")
        .attr("class", "ryltext")
        .style("text-anchor", "start")
        .style("fill", "#333")
        .style("font-size", "11px")
        .style("font-family", "ProximaNova-Bold")
        .text(ResourceUtility.getString("THRESHOLD_EXPLORER_SCORE_LABEL"));

    infoElements.append("text")
        .attr("class", "rytext")
        .style("text-anchor", "start")
        .style("fill", "#477cba")
        .style("font-size", "18px")
        .style("font-family", "ProximaNova-Bold");

    infoElements.append("polygon")
        .attr("class", "rarrow")
        .attr("points", "0,0, 0,6, 4,3")
        .style("fill", "#447bbc")
        .attr("stroke", "#447bbc");

    infoElements.append("polygon")
        .attr("class", "larrow")
        .attr("points", "4,0, 4,6, 0,3")
        .style("fill", "#447bbc")
        .attr("stroke", "#447bbc");

    infoElements.append("circle")
        .attr("class", "xball")
        .style("fill", "#447bbc")
        .attr("stroke", "#447bbc")
        .attr("r", 2.0);

    infoElements.append("circle")
        .attr("class", "yball")
        .style("fill", "#447bbc")
        .attr("stroke", "#447bbc")
        .attr("r", 2.0);

    infoElements.append("line")
        .attr("class", "d")
        .attr("x1", "0")
        .attr("y1", "0")
        .attr("x2", "0")
        .attr("y2", "25")
        .style("stroke", "#c6cbd1");

    //==================================================
    // Specify Info Element Update
    //==================================================
    function updateInfoElements(d)
    {
        infoElements.select("circle.y")
            .attr("transform", "translate(" +
                    x(d.leads) + "," +
                    y(d.conversions) + ")");

        infoElements.select("line.x")
            .attr("transform", "translate(" +
                    "0" + "," +
                    y(d.conversions) + ")");

        infoElements.select("line.y")
            .attr("transform", "translate(" +
                    x(d.leads) + "," +
                    "-40" + ")");

        infoElements.select("line.d")
            .attr("transform", "translate(" +
                    x(d.leads) + "," +
                    (height + 30) + ")");

        infoElements.select("text.rltext")
            .attr("transform", "translate(" +
                    (x(d.leads) + 15) + "," +
                    "-30" + ")");

        infoElements.select("text.rtext")
            .text((d.rightLift !== 0 ? d.rightLift.toFixed(2) : 0) + "x")
            .attr("transform", "translate(" +
                    (x(d.leads) + 14) + "," +
                    "-13" + ")");

        infoElements.select("text.lltext")
            .attr("transform", "translate(" +
                    (x(d.leads) - 16) + "," +
                    "-30" + ")");

        infoElements.select("text.ltext")
            .text(d.leftLift.toFixed(2) + "x")
            .attr("transform", "translate(" +
                    (x(d.leads) - 16) + "," +
                    "-13" + ")");

        infoElements.select("polygon.larrow")
        	.attr("transform", "translate(" +
                	(x(d.leads) - 10) + "," +
					"-30" + ")");

        infoElements.select("polygon.rarrow")
        	.attr("transform", "translate(" +
                	(x(d.leads) + 5) + "," +
					"-30" + ")");

        infoElements.select("circle.xball")
        	.attr("transform", "translate(" +
        	        "-36" + "," +
                	y(d.conversions) + ")");

        infoElements.select("circle.yball")
	        .attr("transform", "translate(" +
                	x(d.leads) + "," +
                	(height + 25) + ")");

        infoElements.select("text.xltext")
            .attr("transform", "translate(" +
                    (-40) + "," +
                    (y(d.conversions) - 3) + ")");

        infoElements.select("text.xtext")
            .text(d.conversions.toFixed(0) + "%")
            .attr("transform", "translate(" +
                    (-40) + "," +
                    (y(d.conversions) + 14) + ")");

        infoElements.select("text.lyltext")
            .attr("transform", "translate(" +
                    (x(d.leads) - 10) + "," +
                    (height + 38) + ")");

        infoElements.select("text.lytext")
            .text(d.leads + "%")
            .attr("transform", "translate(" +
                    (x(d.leads) - 10) + "," +
                    (height + 55) + ")");

        infoElements.select("text.ryltext")
            .attr("transform", "translate(" +
                    (x(d.leads) + 9) + "," +
                    (height + 38) + ")");

        infoElements.select("text.rytext")
            .text("> " + (d.score - 1))
            .attr("transform", "translate(" +
                    (x(d.leads) + 9) + "," +
                    (height + 55) + ")");
    }

    //==================================================
    // Specify Default Info Elements Location
    //==================================================
    function setDefaultInfoElements() {
        updateInfoElements(data[20]);
    }

    //==================================================
    // Update Info Elements
    //==================================================
    setDefaultInfoElements();

    //==================================================
    // Append Capture Area
    //==================================================
    svg.append("rect")
        .attr("x", -10)
        .attr("y", -10)
        .attr("width", width + 20)
        .attr("height", height + 20)
        .style("fill", "none")
        .style("pointer-events", "all")
        .on("mousemove", captureMouseMove)
        .on("mousedown", captureMouseDown);

    //==================================================
    // Capture Area: MouseMove
    //==================================================
    var bisectLeads = d3.bisector(function(d) { return d.leads; }).left;
    function captureMouseMove() {
        if (targetSet) {
            return;
        }
        var x0 = x.invert(d3.mouse(this)[0]),
            i = bisectLeads(data, x0, 1),
            d0 = data[i - 1],
            d1 = data[i],
            d = x0 - d0.leads > d1.leads - x0 ? d1 : d0;

        if (d.leads === 0) {
            updateInfoElements(data[1]);
        } else {
            updateInfoElements(d);
        }
    }
    
    //==================================================
    // Capture Area: MouseDown
    //==================================================
    function captureMouseDown() {
        targetSet = !targetSet;
        var x0 = x.invert(d3.mouse(this)[0]),
            i = bisectLeads(data, x0, 1),
            d0 = data[i - 1],
            d1 = data[i],
            d = x0 - d0.leads > d1.leads - x0 ? d1 : d0;

        if (d.leads === 0) {
            updateInfoElements(data[1]);
        } else {
            updateInfoElements(d);
        }
    }
})

.directive('thresholdExplorerWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});