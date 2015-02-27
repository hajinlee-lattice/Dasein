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
        bar_height = 20,
        height = bar_height * bucketNames.length,
        gap = 2;
    
    var x = d3.scale.linear()
        .domain([0, d3.max(liftValues)])
        .range([0, width]);
    
    var y = d3.scale.ordinal()
        .domain(liftValues)
        .rangeBands([0, height]);
    
    var nameY = d3.scale.ordinal()
            .domain(bucketNames)
            .rangeBands([0, height]);
    
    chart = d3.select("#attributeChart") 
      .append('svg')
      .attr('class', 'chart')
      .attr('width', left_width + width + 40)
      .attr('height', (bar_height + gap * 2) * bucketNames.length + 30)
      .append("g")
      .attr("transform", "translate(10, 20)");
    
    chart.selectAll("rect")
        .data(liftValues)
        .enter().append("rect")
        .attr("x", left_width)
        .attr("y", y)
        .attr("width", x)
        .attr("height", y.rangeBand())
        .style("fill", data.color);
   
    chart.selectAll("text.lift")
        .data(liftValues)
        .enter().append("text")
        .attr("x", function(d) { return x(d) + left_width; })
        .attr("y", function(d){ return y(d) + y.rangeBand()/2; } )
        .attr("dx", -5)
        .attr("dy", ".36em")
        .attr("text-anchor", "end")
        .attr("class", "lift")
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