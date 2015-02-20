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
    
    var chartData = data.ChartData;
    $scope.backToSummaryView = false;
    $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_HEADER", [chartData.attributesPerCategory]);
    
    // Get Internal category list
    var internalCategoryObj = data.InternalAttributes;
    $scope.internalPredictorTotal = internalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_INTERNAL_TITLE");
    $scope.internalCategories = internalCategoryObj.categories;
    $scope.showInternalCategories = internalCategoryObj.total > 0;
    
    // Get External category list
    var externalCategoryObj = data.ExternalAttributes;
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
                    return 50;
                } else if (d.depth === 2) {
                    return 61;
                }  else {
                    return 0;
                }
            })
            .outerRadius(function(d) { 
                if (d.depth === 1) {
                    return 60;
                } else if (d.depth === 2) {
                    return 125;
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
              .on("click", attributeClicked)
              .on("mouseover", attributeMouseover)
              .on("mouseout", attributeMouseout);
              
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
          
          function attributeMouseover (d) {
              svg.selectAll("path")
                  .filter(function(node) {
                            return node.depth == 2 && node.name == d.name;
                          })
                  .style("opacity", 1);
              //TODO:pierce Here is where the hover chart will go
          }
          
          function attributeMouseout (d) {
              svg.selectAll("path")
                  .filter(function(node) {
                            return node.depth == 2;
                          })
                  .style("opacity", 0.6);
          }
    };
    $scope.drawSummaryChart();
    
    $scope.backToSummaryClicked = function () {
        $scope.backToSummaryView = false;
        $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_HEADER", [chartData.attributesPerCategory]);
        $scope.drawSummaryChart();
    };
    
    $scope.categoryClicked = function (category) {
        var categoryList = TopPredictorService.GetAttributesByCategory(data.Predictors, category.name, category.color, 50);
        TopPredictorService.CalculateAttributeSize(categoryList);
        $scope.backToSummaryView = true;
        $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_CATEGORY_HEADER", [category.name, categoryList.length]);
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
                    return 50;
                }  else {
                    return 0;
                }
            })
            .outerRadius(function(d) { 
                if (d.depth === 1) {
                    return 125; 
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
    };
  
})

.directive('topPredictorWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});