angular.module('mainApp.appCommon.widgets.TopPredictorWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.appCommon.widgets.TopPredictorAttributeWidget',
    'ngSanitize'
])

    .controller('TopPredictorWidgetController', function ($scope, $sce, $element, $compile, $rootScope, 
        ResourceUtility, WidgetFrameworkService, TopPredictorService) {
        
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
        var totalAttributes = data.TotalAttributeValues;
        $scope.topPredictorTitle = totalAttributes + " " + ResourceUtility.getString("TOP_PREDICTORS_TITLE");
        
        $scope.generateCategoryLabel = function(category) {
        	return $sce.trustAsHtml(category.name + '<span style="background-color:' + category.color + '">' + category.count + '</span>');
        };
        
        // Methods used for the Sunburst chart
        // Stash the old values for transition.
        function stash (d) {
            d.x0 = d.x;
            d.dx0 = d.dx;
        }
        
        var width = 300,
            height = 300,
            radius = Math.min(width, height) / 2,
            hoverOpacity = 0.7,
            showAttributeTimeout;

        // This is used to get an initial size so it can animate 
        var fakePartition = d3.layout.partition()
            .sort(null)
            .size([0.01, 0.01])
            .value(function(d) { return 1; });
            
        var partition = d3.layout.partition()
            .sort(null)
            .size([2 * Math.PI, radius * radius])
            .value(function(d) { return d.size; });
        
        //Draw Sunburst chart
        $scope.drawSummaryChart = function () {
            $(".js-top-predictor-donut").empty();
            var svg = d3.select(".js-top-predictor-donut").append("svg")
                .attr("width", width)
                .attr("height", height)
              .append("g")
                .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
            
            var gutterWidth = 0.0075;  // radians

            var arc = d3.svg.arc()
                .startAngle(function(d) { 
                    // if first category, make start angle larger for more gutter
                    var gutter = null;

                    if (d.depth === 2) {
                        var children = d.parent.children;
                        gutter = children[0].name == d.name ? gutterWidth : 0;
                    }

                    return d.x + (gutter || 0);
                })
                .endAngle(function(d) {
                    // if last category, make end angle shorter for more gutter
                    var gutter = null;

                    if (d.depth === 2) {
                        var children = d.parent.children,
                            child = children[children.length - 1];
                        gutter = child && child.name == d.name ? gutterWidth * 2 : 0;
                    }

                    return d.x + d.dx - (gutter || 0);
                })
                .innerRadius(function(d) {
                    switch (d.depth) {
                        case 1: return 50;
                        case 2: return 61;
                        default: return 0;
                    }
                })
                .outerRadius(function(d) {
                    switch (d.depth) {
                        case 1: return 60;
                        case 2: return 125;
                        default: return 0;
                    }
                });

            var path = svg.datum(chartData).selectAll("path")
                .data(fakePartition.nodes)
            .enter().append("path")
                .attr("display", function(d) { 
                    return d.depth ? null : "none"; 
                }) // hide inner ring
                .attr("d", arc)
                .attr('stroke-width', function(d) { return d.depth === 1 ? 2.5 : 1; })
                .style("stroke", "#fff")
                .style("cursor", "pointer")
                .attr('opacity', function(d) {
                    if  (d.depth === 1) {
                        return 1;
                    } else if (d.depth === 2) {
                        return hoverOpacity;
                    }
                })
                .style("fill", function(d) {
                    return d.color;
                })
                .on("click", attributeClicked)
                .on("mouseover", function (d) {
                    var path = svg.selectAll("path")
                        .filter(function(node) {
                            return node.depth == 2 && node.name == d.name;
                        });

                    path.style("opacity", 1);

                    if (d.depth == 2) {
                        showAttributeHover(d.name, d.color, d3.mouse(this)[0], d3.mouse(this)[1], path);
                    }
                })
                .on("mouseout", function (d) {
                    svg.selectAll("path")
                        .filter(function(node) {
                            return node.depth == 2;
                        })
                        .style("opacity", hoverOpacity);
                    hideAttributeHover();
                })
                .each(stash);

            function attributeClicked (d) {
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
            
            // Interpolate the arcs in data space.
            function arcTween (a) {
                var i = d3.interpolate({x: a.x0, dx: a.dx0}, a);
                return function(t) {
                    var b = i(t);
                    a.x0 = b.x;
                    a.dx0 = b.dx;
                    return arc(b);
                };
            }
            path.data(partition.nodes).transition().duration(1000).attrTween("d", arcTween);
        };

        $scope.drawSummaryChart();

        $scope.exportClicked = function () {
            var fileName = "attributes.csv";
        	var csvRows = TopPredictorService.GetTopPredictorExport(data);
            alasql("SELECT * INTO CSV('" + fileName + "') FROM ?", [csvRows]);
        };

        $scope.backToSummaryClicked = function () {
            clearSelectedCategory();
            $scope.backToSummaryView = false;
            $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_HEADER", [chartData.attributesPerCategory]);
            $scope.drawSummaryChart();
        };
        
        function clearSelectedCategory () {
            TopPredictorService.ClearCategoryClasses($scope.externalCategories);
            TopPredictorService.ClearCategoryClasses($scope.internalCategories);
        }
        
        function highlightSelectedCategory (category) {
            clearSelectedCategory();
            
            for (var i = 0; i < $scope.externalCategories.length; i++) {
                if ($scope.externalCategories[i].name === category.name) {
                    $scope.externalCategories[i].activeClass = "active";
                    break;
                }
            }
            
            for (var x = 0; x < $scope.internalCategories.length; x++) {
                if ($scope.internalCategories[x].name === category.name) {
                    $scope.internalCategories[x].activeClass = "active";
                    break;
                }
            }
        }
        
        $scope.categoryClicked = function (category) {
            clearTimeout(showAttributeTimeout);
            var categoryList = TopPredictorService.GetAttributesByCategory(data, category.name, category.color, 50);
            highlightSelectedCategory(category);
            category.activeClass = "active";
            TopPredictorService.CalculateAttributeSize(categoryList);
            $scope.backToSummaryView = true;
            var prefix = categoryList.length >= 50 ? ResourceUtility.getString("TOP_PREDICTORS_CHART_CATEGORY_HEADER_PREFIX") : "";
            $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_CATEGORY_HEADER", [category.name, prefix, categoryList.length]);
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
            
            // Append a back button
            setTimeout(function () {
                svg.append("svg:image")
                    .attr("id", "donutChartBackButton")
                    .attr("class", "donut-chart-back-button")
                    .attr("xlink:href", "assets/images/Donut-Center-Back.png")
                    .attr("x", -14)
                    .attr("y", -13)
                    .attr("width", "29px")
                    .attr("height", "29px")
                    .on("mouseover", function (d) {
                        d3.select(this).attr("xlink:href", "assets/images/Donut-Center-Back-Hover.png");
                    })
                    .on("mouseout", function (d) {
                        d3.select(this).attr("xlink:href", "assets/images/Donut-Center-Back.png");
                    })
                    .on("click", function () {
                        $("#back-to-summary-link").click();
                    });
                    
            }, 1000);
            
            svg.append("circle")
               .attr("cx", 0)
               .attr("cy", 0)
               .attr("r", 50)
               .attr("fill", "#fff")
               .on("mouseover", function() {
                   svg.selectAll('circle').style('cursor', 'pointer');
               })
               .on("click", function() {
                   $("#back-to-summary-link").click();
               });
                
            var arc2 = d3.svg.arc()
                .startAngle(function(d) { return d.x; })
                .endAngle(function(d) { return d.x + d.dx; })
                .innerRadius(function(d) {
                    if (d.depth === 1) {
                        return 50;
                    } else {
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
                .data(fakePartition.nodes)
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
                        return hoverOpacity;
                    }
                })
                .style("fill", function(d) {
                    return d.color;
                })
                .on("mouseover", function (d) {
                    var path = svg.selectAll("path")
                        .filter(function(node) {
                            return node.depth == 1 && node.name == d.name;
                        });

                    path.style("opacity", hoverOpacity);

                    if (d.depth == 1) {
                        showAttributeHover(d.name, d.color, d3.mouse(this)[0], d3.mouse(this)[1], path);
                    }
                })
                .on("mouseout", function (d) {
                    svg.selectAll("path")
                        .filter(function(node) {
                            return node.depth == 1;
                        })
                        .style("opacity", 1);
                    hideAttributeHover();
                })
                .each(stash);

            // Interpolate the arcs in data space.
            function arcTween (a) {
                var i = d3.interpolate({x: a.x0, dx: a.dx0}, a);
                return function(t) {
                    var b = i(t);
                    a.x0 = b.x;
                    a.dx0 = b.dx;
                    return arc2(b);
                };
            }
            path.data(partition.nodes).transition().duration(1000).attrTween("d", arcTween);
        };
        
        function showAttributeHover (attributeName, attributeColor, mouseX, mouseY, path) {
            if (showAttributeTimeout != null) {
                clearTimeout(showAttributeTimeout);
            }
            showAttributeTimeout = setTimeout(function () {
                var topPredictorAttributeHover = $("#topPredictorAttributeHover");
                var scope = $rootScope.$new();
                scope.mouseX = mouseX;
                scope.mouseY = mouseY;
                scope.selectedPath = path; // for anchoring tail and hoverElem position
                scope.data = TopPredictorService.FormatDataForAttributeValueChart(attributeName, attributeColor, data);
                $compile(topPredictorAttributeHover.html('<div data-top-predictor-attribute-widget></div>'))(scope);
            }, 500);
        }
        
        function hideAttributeHover () { 
            if (showAttributeTimeout != null) {
                clearTimeout(showAttributeTimeout);
            }
            var topPredictorAttributeHover = $("#topPredictorAttributeHover");
            topPredictorAttributeHover.hide();
            topPredictorAttributeHover.empty();
            
        }
      
    })

    .directive('topPredictorWidget', function () {
        var directiveDefinitionObject = {
            templateUrl: '/app/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
        };
      
        return directiveDefinitionObject;
    });