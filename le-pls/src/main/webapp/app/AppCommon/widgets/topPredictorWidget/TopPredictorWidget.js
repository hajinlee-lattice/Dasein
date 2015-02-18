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
    
    //Draw Donut chart
    $scope.drawSummaryChart = function () {
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
              .style("cursor", "pointer")
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
              .on("click", attributeClicked);
              
          function attributeClicked(d) {
              var category = null;
              for (var i = 0; i < chartData.children.length; i++) {
                  if (chartData.children[i].categoryName == d.categoryName) {
                      category = chartData.children[i];
                      break;
                  }
              }
              
              if (category != null) {
                  // This is required to update bindings (although not sure why)
                  $scope.$apply($scope.categoryClicked(category));
              }
          }
        
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
    };
    $scope.drawSummaryChart();
    
    $scope.backToSummaryClicked = function () {
        $scope.backToSummaryView = false;
        $scope.drawSummaryChart();
    };
    
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
        var width = 300,
            height = 300,
            radius = Math.min(width, height) / 2;
            
        var svg = d3.select(".js-top-predictor-donut").append("svg")
            .attr("width", width)
            .attr("height", height)
          .append("g")
            .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
            
        var partition = d3.layout.partition()
            .sort(null)
            .size([2 * Math.PI, radius * radius])
            .value(function(d) { return d.size; });
        
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
        
        var path = svg.datum(root).selectAll("path")
          .data(partition.nodes)
        .enter().append("path")
          .attr("display", function(d) { 
              return d.depth ? null : "none"; 
          }) // hide inner ring
          .attr("d", arc2)   
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
          .transition()
            .duration(1000)
            .call(arcTween);
        
        // Interpolate the arcs in data space.
        function arcTween(transition, newAngle) {
            transition.attrTween("d", function(d) {
                var interpolate = d3.interpolate(d.endAngle, newAngle);
                return function(t) {
                    d.endAngle = interpolate(t);
                    return arc2(d);
                };
            });
        }
        
        var categoryTitle1 = ResourceUtility.getString("TOP_PREDICTORS_CATEGORY_CHART_TITLE_1");
        var categoryTitle2 = ResourceUtility.getString("TOP_PREDICTORS_CATEGORY_CHART_TITLE_2");
        
        // Add value to the middle of the arc
        svg.append("text")
            .attr("class", "top-predictor-donut-text")
            .style("fill", "#555555")
            .attr("dy", ".10em")
            .text(categoryTitle1);
            
        svg.append("text")
            .attr("class", "top-predictor-donut-text")
            .style("fill", "#555555")
            .attr("dy", "1.250em")
            .text(categoryTitle2);
    };
  
})

.directive('topPredictorWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});