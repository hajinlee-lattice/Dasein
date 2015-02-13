angular.module('mainApp.appCommon.widgets.TopPredictorWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.service('TopPredictorService', function () {
    this.GetCategoryListByType = function (fullList, type) {
        var toReturn = [];
        if (fullList == null || type == null) {
            return toReturn;
        }
        
        for (var i = 0; i < fullList.length; i++) {
            if (fullList[i].Type == type) {
                toReturn.push(fullList[i]);
            }
        }
        
        return toReturn;
    };
 })

.controller('TopPredictorWidgetController', function ($scope, $element, ResourceUtility, WidgetFrameworkService, TopPredictorService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    //var data = $scope.data;
    //TODO:pierce temp
    var data = {"name":"root","TotalInternal":354,"TotalExternal":51,"Color":"#FFFFFF","children":[{"name":"Firmographics","Color":"#27D2AE","Count":143,"Size":6.55,"Type":"Internal","children":[{"name":"Attribute1","Size":16.768,"Color":"#27D2AE"},{"name":"Attribute2","Size":16.768,"Color":"#27D2AE"},{"name":"Attribute3","Size":1,"Color":"#27D2AE"}]},{"name":"Growth Trends","Color":"#3279DF","Count":45,"Size":1,"Type":"Internal","children":[{"name":"Attribute1","Size":6.55,"Color":"#3279DF"},{"name":"Attribute2","Size":2.56,"Color":"#3279DF"},{"name":"Attribute3","Size":1,"Color":"#3279DF"}]},{"name":"Online Presence","Color":"#FF9403","Count":72,"Size":1,"Type":"Internal","children":[{"name":"Attribute1","Size":6.55,"Color":"#FF9403"},{"name":"Attribute2","Size":2.56,"Color":"#FF9403"},{"name":"Attribute3","Size":1,"Color":"#FF9403"}]},{"name":"Website Technologies","Color":"#BD8DF6","Count":14,"Size":2.56,"Type":"Internal","children":[{"name":"Attribute1","Size":6.5536,"Color":"#BD8DF6"},{"name":"Attribute2","Size":6.5536,"Color":"#BD8DF6"},{"name":"Attribute3","Size":6.5536,"Color":"#BD8DF6"}]},{"name":"Behind Firewll Tech","Color":"#96E01E","Count":52,"Size":2.56,"Type":"Internal","children":[{"name":"Attribute1","Size":6.5536,"Color":"#96E01E"},{"name":"Attribute2","Size":2.56,"Color":"#96E01E"},{"name":"Attribute3","Size":2.56,"Color":"#96E01E"}]},{"name":"Financials","Color":"#A8A8A8","Count":129,"Size":2.56,"Type":"Internal","children":[{"name":"Attribute1","Size":2.56,"Color":"#A8A8A8"},{"name":"Attribute2","Size":6.5536,"Color":"#A8A8A8"},{"name":"Attribute3","Size":2.56,"Color":"#A8A8A8"}]},{"name":"Marketing Activity","Color":"#3279DF","Count":42,"Size":1,"Type":"External","children":[{"name":"Attribute1","Size":1,"Color":"#3279DF"},{"name":"Attribute2","Size":1,"Color":"#3279DF"},{"name":"Attribute3","Size":1,"Color":"#3279DF"}]},{"name":"Lead Information","Color":"#FF7A44","Count":19,"Size":1,"Type":"External","children":[{"name":"Attribute1","Size":6.55,"Color":"#FF7A44"},{"name":"Attribute2","Size":1,"Color":"#FF7A44"},{"name":"Attribute3","Size":1,"Color":"#FF7A44"}]}]};
    var parentData = $scope.parentData;
    
    var container = $('<div></div>');
    $($element).append(container);

    var options = {
        element: container,
        widgetConfig: widgetConfig,
        metadata: metadata,
        data: data,
        parentData: parentData
    };
    WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
    
    $scope.ResourceUtility = ResourceUtility;
    var totalAttributes = data.TotalInternal + data.TotalExternal;
    $scope.topPredictorTitle = totalAttributes + " " + ResourceUtility.getString("TOP_PREDICTORS_TITLE");
    $scope.internalPredictorTotal = data.TotalInternal + " " + ResourceUtility.getString("TOP_PREDICTORS_INTERNAL_TITLE");
    $scope.externalPredictorTotal = data.TotalExternal + " " + ResourceUtility.getString("TOP_PREDICTORS_EXTERNAL_TITLE");
    $scope.internalCategories = TopPredictorService.GetCategoryListByType(data.children, "Internal");
    $scope.externalCategories = TopPredictorService.GetCategoryListByType(data.children, "External");
    
    //Draw Donut chart
    var width = 300,
        height = 300,
        radius = Math.min(width, height) / 2;
        
    $(".js-top-predictor-donut").empty();
    var svg = d3.select(".js-top-predictor-donut").append("svg")
        .attr("width", width)
        .attr("height", height)
      .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
        
    var partition = d3.layout.partition()
        .sort(null)
        .size([2 * Math.PI, radius * radius])
        .value(function(d) { return d.Size; });
        
    var arc = d3.svg.arc()
        .startAngle(function(d) { return d.x; })
        .endAngle(function(d) { return d.x + d.dx; })
        .innerRadius(function(d) {
            if (d.depth === 1) {
                return 70;
            } else if (d.depth === 2) {
                return 81;
            } 
        })
        .outerRadius(function(d) { 
            if (d.depth === 1) {
                return 80;
            } else if (d.depth === 2) {
                return 120;
            }
        });
    
    var path = svg.datum(data).selectAll("path")
          .data(partition.nodes)
        .enter().append("path")
          .attr("display", function(d) { 
              return d.depth ? null : "none"; 
          }) // hide inner ring
          .attr("d", arc)
          .style("stroke", "#fff")
          .attr('opacity', function(d) {
              if (d.depth === 1) {
                return 1;
            } else if (d.depth === 2) {
                return 0.6;
            }
          })
          .style("fill", function(d) {
              return d.Color;
          });
    
    // Add value to the middle of the arc
    svg.append("text")
        .attr("class", "top-predictor-donut-text")
        .style("fill", "#555555")
        .attr("dy", ".10em")
        .text("Top 3 Attributes");
        
    svg.append("text")
        .attr("class", "top-predictor-donut-text")
        .style("fill", "#555555")
        .attr("dy", "1.250em")
        .text("per Category");
    
    // Interpolate the arcs in data space.
    function arcTween(a) {
      var i = d3.interpolate({x: a.x0, dx: a.dx0}, a);
      return function(t) {
        var b = i(t);
        a.x0 = b.x;
        a.dx0 = b.dx;
        return arc(b);
      };
    }
  
})

.directive('topPredictorWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});