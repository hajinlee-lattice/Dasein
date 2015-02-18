angular.module('mainApp.appCommon.widgets.TopPredictorWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.service('TopPredictorService', function (StringUtility, AnalyticAttributeUtility) {
    
    this.ShowBasedOnTags = function (predictor, tag) {
        
        //TODO:pierce This is a hack because DataLoader is not providing internal metadata
        if (predictor.Tags == null) {
            predictor.Tags = ["External"];
        }
        
        var toReturn = false;
        for (var x=0; x<predictor.Tags.length; x++) {
            if (tag == predictor.Tags[x]) {
                toReturn = true;
                break;
            }
        }
        
        return toReturn;
    };
    
    this.GetCategoryListByType = function (categoryList, type, fullPredictorList) {
        var toReturn = {
            total: 0,
            categories: []
        };
        if (categoryList == null || type == null || fullPredictorList == null) {
            return toReturn;
        }
        for (var x = 0; x < categoryList.length; x++) {
            var category = categoryList[x];
            var displayCategory = {
                name: category.name,
                count: 0,
                color: category.color
            };
            for (var i = 0; i < fullPredictorList.length; i++) {
                var predictor = fullPredictorList[i];
                if (predictor.Category == category.name && 
                    this.ShowBasedOnTags(predictor, type) &&
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor)) {
                        //TODO:pierce Might need to check frequency on categoricals before adding the number
                        toReturn.total += predictor.Elements.length;
                        displayCategory.count = displayCategory.count + predictor.Elements.length;
                    }
            }
            
            if (displayCategory.count > 0) {
                toReturn.categories.push(displayCategory);
            }
        }
        
        return toReturn;
    };
    
    this.GetColorByCategory = function (categoryName) {
        var toReturn;
        switch (categoryName) {
            case "Firmographics":
                toReturn = "#27D2AE";
                break;
            case "Growth Trends":
                toReturn = "#3279DF";
                break;
            case "Online Presence":
                toReturn = "#FF9403";
                break;
            case "Technologies":
                toReturn = "#BD8DF6";
                break;
            case "Behind Firewall Tech":
                toReturn = "#96E01E";
                break;
            case "Financial":
                toReturn = "#A8A8A8";
                break;
            case "Marketing Activity":
                toReturn = "#3279DF";
                break;
            case "Lead Information":
                toReturn = "#FF7A44";
                break;
            default:
                toReturn = "#000000";
                break;
        }
        
        return toReturn;
    };
    
    this.SortByPredictivePower = function (a, b) {
        if (a.UncertaintyCoefficient > b.UncertaintyCoefficient) {
            return -1;
        }
        if (a.UncertaintyCoefficient < b.UncertaintyCoefficient) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.GetAttributesByCategory = function (predictorList, categoryName, categoryColor, maxNumber) {
        if (StringUtility.IsEmptyString(categoryName) || predictorList == null) {
            return [];
        }
        
        var totalPredictors = [];
        for (var i = 0; i < predictorList.length; i++) {
            if (categoryName == predictorList[i].Category) {
                totalPredictors.push(predictorList[i]);
            }
        }
        totalPredictors = totalPredictors.sort(this.SortByPredictivePower);
        
        var toReturn = [];
        for (var y = 0; y < totalPredictors.length; y++) {
            var predictor = totalPredictors[y];
            if (maxNumber == null || toReturn.length < maxNumber) {
                var displayPredictor = {
                  name: predictor.Name,
                  power: predictor.UncertaintyCoefficient,
                  size: 1,
                  color: categoryColor
                };
                toReturn.push(displayPredictor);
            } else {
                break;
            }
        }
        return toReturn;
        
    };
    
    this.CalculateAttributeSize = function (attributeList) {
        if (attributeList == null || attributeList.length === 0) {
            return null;
        }
        var numLargeCategories = Math.round(attributeList.length * 0.16);
        var numMediumCategories = Math.round(attributeList.length * 0.32);
        
        for (var i = 0; i < attributeList.length; i++) {
            var attribute = attributeList[i];
            if (numLargeCategories > 0) {
                attribute.size = 6.55;
                numLargeCategories--;
            } else if (numMediumCategories > 0) {
                attribute.size = 2.56;
                numMediumCategories--;
            } else {
                attribute.size = 1;
            }
        }
    };
    
    this.FormatDataForChart = function (modelSummary) {
        if (modelSummary == null || modelSummary.Predictors == null || modelSummary.Predictors.length === 0) {
            return null;
        }
        
        // First sort all predictors by UncertaintyCoefficient
        modelSummary.Predictors = modelSummary.Predictors.sort(this.SortByPredictivePower);
        
        // Then pull all unique categories
        var topCategories = [];
        var topCategoryNames = [];
        var category;
        for (var i = 0; i < modelSummary.Predictors.length; i++) {
            var predictor = modelSummary.Predictors[i];
            if (!StringUtility.IsEmptyString(predictor.Category) &&  topCategoryNames.indexOf(predictor.Category) === -1 && topCategoryNames.length < 8) {
                topCategoryNames.push(predictor.Category);
                category = {
                    name: predictor.Category,
                    power: predictor.UncertaintyCoefficient,
                    size: 1, // This doesn't matter because the inner ring takes on the saze of the outer
                    color: this.GetColorByCategory(predictor.Category),
                    children: []
                };
                topCategories.push(category);
            }
        }
        
        //And finally calculate the size based on predictive power
        var attributesPerCategory = topCategories.length >= 7 ? 3 : 5;
        var numLargeCategories = Math.round((topCategories.length * attributesPerCategory) * 0.16);
        var numMediumCategories = Math.round((topCategories.length * attributesPerCategory) * 0.32);
        var totalAttributes = [];
        for (var x = 0; x < topCategories.length; x++) {
            category = topCategories[x];
            category.children = this.GetAttributesByCategory(modelSummary.Predictors, category.name, category.color, attributesPerCategory);
            for (var y = 0; y < category.children.length; y++) {
                totalAttributes.push(category.children[y]);
            }
        }
        
        totalAttributes.Predictors = totalAttributes.sort(this.SortByPredictivePower);
        
        for (var z = 0; z < totalAttributes.length; z++) {
            var attribute = totalAttributes[z];
            if (numLargeCategories > 0) {
                attribute.size = 6.55;
                numLargeCategories--;
            } else if (numMediumCategories > 0) {
                attribute.size = 2.56;
                numMediumCategories--;
            } else {
                attribute.size = 1;
            }
            
        }
        
        var toReturn = {
            name: "root",
            size : 1,
            color: "#FFFFFF",
            attributesPerCategory: attributesPerCategory,
            children: topCategories
        };
        
        return toReturn;
    };
 })

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
    var internalCategoryObj = TopPredictorService.GetCategoryListByType(chartData.children, "Internal", data.Predictors);
    $scope.internalPredictorTotal = internalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_INTERNAL_TITLE");
    $scope.internalCategories = internalCategoryObj.categories;
    
    // Get External category list
    var externalCategoryObj = TopPredictorService.GetCategoryListByType(chartData.children, "External", data.Predictors);
    $scope.externalPredictorTotal = externalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_EXTERNAL_TITLE");
    $scope.externalCategories = externalCategoryObj.categories;
    
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