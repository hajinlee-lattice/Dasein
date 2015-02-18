angular.module('mainApp.appCommon.widgets.TopPredictorWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.TopPredictorService'
])

.controller('TopPredictorWidgetController', function ($scope, $element, ResourceUtility, WidgetFrameworkService, TopPredictorService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;
    $scope.ResourceUtility = ResourceUtility;
    $scope.backToSummaryView = false;
    
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
    
    var chartData = TopPredictorService.FormatDataForChart(data);
    var chartTitle1 = ResourceUtility.getString("TOP_PREDICTORS_CHART_TITLE_1", [chartData.attributesPerCategory]);
    var chartTitle2 = ResourceUtility.getString("TOP_PREDICTORS_CHART_TITLE_2");
    
    // Get Internal category list
    var internalCategoryObj = TopPredictorService.GetNumberOfAttributesByCategory(chartData.children, "Internal", data.Predictors);
    $scope.internalPredictorTotal = internalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_INTERNAL_TITLE");
    $scope.internalCategories = internalCategoryObj.categories;
    $scope.showInternalCategories = internalCategoryObj.total > 0;
    
    // Get External category list
    var externalCategoryObj = TopPredictorService.GetNumberOfAttributesByCategory(chartData.children, "External", data.Predictors);
    $scope.externalPredictorTotal = externalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_EXTERNAL_TITLE");
    $scope.externalCategories = externalCategoryObj.categories;
    $scope.showExternalCategories = externalCategoryObj.total > 0;
    
    // Calculate total
    var totalAttributes = internalCategoryObj.total + externalCategoryObj.total;
    $scope.topPredictorTitle = totalAttributes + " " + ResourceUtility.getString("TOP_PREDICTORS_TITLE");
    
    $scope.categoryClicked = function (category) {
        var categoryList = TopPredictorService.GetAttributesByCategory(data.Predictors, category.name, category.color, 50);
    };
    
    //Draw Donut chart
    var width = 300,
        height = 300,
        radius = Math.min(width, height) / 2;
    
    function stash(d) {
      d.x0 = d.x;
      d.dx0 = d.dx;
    }
    $(".js-top-predictor-donut").empty();
    var svg = d3.select(".js-top-predictor-donut").append("svg")
        .attr("width", width)
        .attr("height", height)
      .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
        
    var partition = d3.layout.partition()
        .sort(null)
        .size([2 * Math.PI, radius * radius])
        .value(function(d) { return d.size; });
        
    var arc = d3.svg.arc()
        .startAngle(function(d) { return d.x; })
        .endAngle(function(d) { return d.x + d.dx; })
        .innerRadius(function(d) {
            if (d.depth === 1) {
                return 70;
            } else if (d.depth === 2) {
                return 81;
            }  else {
                return 0;
            }
        })
        .outerRadius(function(d) { 
            if (d.depth === 1) {
                return 80;
            } else if (d.depth === 2) {
                return 120;
            } else {
                return 0;
            }
        });
    
    var path = svg.datum(chartData).selectAll("path")
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
              return d.color;
          }).each(stash);
    
    // Add value to the middle of the arc
    svg.append("text")
        .attr("class", "top-predictor-donut-text")
        .style("fill", "#555555")
        .attr("dy", ".10em")
        .text(chartTitle1);
        
    svg.append("text")
        .attr("class", "top-predictor-donut-text")
        .style("fill", "#555555")
        .attr("dy", "1.250em")
        .text(chartTitle2);
    
    
    
    $scope.categoryClicked = function (category) {
        var categoryList = TopPredictorService.GetAttributesByCategory(data.Predictors, category.name, category.color, 50);
        TopPredictorService.CalculateAttributeSize(categoryList);
        $scope.backToSummaryView = true;
        var root = {
            name: "root",
            size : 1,
            color: "#FFFFFF",
            children: categoryList
        };
        $(".js-top-predictor-donut").empty();
        var svg = d3.select(".js-top-predictor-donut").append("svg")
            .attr("width", width)
            .attr("height", height)
          .append("g")
            .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
            
        var path = svg.datum(root).selectAll("path")
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
              return d.color;
          })
          .each(stash)
          .transition()
            .duration(1500)
            .attrTween("d", arcTween);
        
        var arc2 = d3.svg.arc()
        .startAngle(function(d) { return d.x; })
        .endAngle(function(d) { return d.x + d.dx; })
        .innerRadius(function(d) {
            if (d.depth === 1) {
                return 70;
            }  else {
                return 0;
            }
        })
        .outerRadius(function(d) { 
            if (d.depth === 1) {
                return 120; 
            } else {
                return 0;
            }
        });
        // Interpolate the arcs in data space.
        function arcTween(a) {
          var i = d3.interpolate({x: a.x0, dx: a.dx0}, a);
          return function(t) {
            var b = i(t);
            a.x0 = b.x;
            a.dx0 = b.dx;
            return arc2(b);
          };
        }
    };
  
})

.directive('topPredictorWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});