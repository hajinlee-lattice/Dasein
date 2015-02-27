angular.module('mainApp.appCommon.widgets.TopPredictorAttributeWidget', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.controller('TopPredictorAttributeWidgetController', function ($scope, ResourceUtility) {
    var data = $scope.data;
    $scope.attributeName = data.name;
    $scope.attributeDescription = data.description;
    $scope.attributeColor = data.color;
    
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
    
    var chart,
        width = 350,
        left_width = 140,
        bar_height = 24,
        height = bar_height * bucketNames.length,
        gap = 5;
    
    var x = d3.scale.linear()
        .domain([0, d3.max(liftValues)])
        .range([0, width]);
    
    var y = d3.scale.ordinal()
        .domain(liftValues)
        .rangeBands([0, (bar_height + 2 * gap) * liftValues.length]);
    
    var nameY = d3.scale.ordinal()
            .domain(bucketNames)
            .rangeBands([0, (bar_height + 2 * gap) * bucketNames.length]);
    
    chart = d3.select("#attributeChart") 
      .append('svg')
      .attr('class', 'chart')
      .attr('width', left_width + width + 40)
      .attr('height', (bar_height + gap * 2) * bucketNames.length + 30)
      .append("g")
      .attr("transform", "translate(10, 20)");
      
    chart.selectAll("line")
        .data(x.ticks(d3.max(liftValues)))
        .enter().append("line")
        .attr("x1", function(d) { return x(d) + left_width; })
        .attr("x2", function(d) { return x(d) + left_width; })
        .attr("y1", 0)
        .attr("y2", (bar_height + gap * 2) * bucketNames.length);

    chart.selectAll(".rule")
        .data(x.ticks(d3.max(liftValues)))
        .enter().append("text")
        .attr("class", "rule")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", 0)
        .attr("dy", -6)
        .attr("text-anchor", "middle")
        .attr("font-size", 10)
        .text(String);
    
    chart.selectAll("rect")
        .data(liftValues)
        .enter().append("rect")
        .attr("x", left_width)
        .attr("y", function(d) { return y(d) + gap; })
        .attr("width", x)
        .attr("height", bar_height)
        .style("fill", data.color)
        .attr('opacity', function(d) {
            if (d > 1) {
                return 0.7;
            } else {
                return 0.4;
            }
            
        });
   
    chart.selectAll("text.lift")
        .data(liftValues)
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", function(d){ return y(d) + y.rangeBand()/2; } )
        .attr("dx", -5)
        .attr("dy", ".36em")
        .attr("text-anchor", "end")
        .attr("class", "lift")
        .style("fill", "black")
        .text(String);
  
    chart.selectAll("text.name")
        .data(bucketNames)
        .enter().append("text")
        .attr("x", 0)
        .attr("y", function(d) {return nameY(d) + nameY.rangeBand()/2; })
        .attr("dy", ".36em")
        .attr('class', 'name')
        .text(String);
        
})

.directive('topPredictorAttributeWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});