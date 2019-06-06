angular.module('mainApp.appCommon.widgets.TopPredictorWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.appCommon.widgets.TopPredictorAttributeWidget',
    'ngSanitize'
])

.controller('TopPredictorWidgetController', function (
        $scope, $sce, $compile, $rootScope, $timeout, ResourceUtility, TopPredictorService, ModelStore, FeatureFlagService
    ) {

    var widgetConfig = ModelStore.widgetConfig;
    var metadata = ModelStore.metadata;
    var data = ModelStore.data;
    var parentData = $scope.parentData;
    var flags = FeatureFlagService.Flags();
    $scope.ResourceUtility = ResourceUtility;
    var chartData = data.ChartData;

    // console.log(chartData);

    if (chartData && chartData.children) {
        // THIS IS PART OF THE UI BAND-AID TO COMBINE INTERNAL, EXTERNAL CATEGORIES WITH SAME NAME
        for (var i = 0; i < chartData.children.length; i++) {
            if (chartData.children[i].children.length == 0) {
                chartData.children.splice(i, 1);
            }
        }
        $scope.chartHeader = ResourceUtility.getString("TOP_PREDICTORS_CHART_HEADER", [chartData.attributesPerCategory]);
    }

    $scope.backToSummaryView = false;

    $scope.formatAttributes = function (categories) {
        for (var i = 0; i < categories.length; i++) {
            if(categories[i].name == 'ACCOUNT_ATTRIBUTES') {
                var myAttributesCategory = chartData.children.find(function(category) { return category.name == 'ACCOUNT_ATTRIBUTES' });
                if (myAttributesCategory) {
                    myAttributesCategory.categoryName = 'My Attributes';
                    myAttributesCategory.name = 'My Attributes';

                    var childAttributes = myAttributesCategory.children;
                    angular.forEach(childAttributes, function(attribute){
                        attribute.categoryName = 'My Attributes';
                    }); 
                    setAttributeColor(myAttributesCategory, '#457DB9');
                }
                $scope.hasAccountAttributes = true;
                
                categories[i].name = 'My Attributes';
                categories[i].color = '#457DB9';
                
                
            }
            if (categories[i].name == 'My Attributes') {

                $scope.hasAccountAttributes = false;
                categories[i].color = '#457DB9';
                
                var myAttributesCategory = chartData.children.find(function(category) { return category.name == 'My Attributes' });
                if (myAttributesCategory) {
                    setAttributeColor(myAttributesCategory, '#457DB9');
                }
            }
            if(categories[i].name == 'Product Spend Profile') {
                categories[i].color = '#B887B6';
                var productSpendCategory = chartData.children.find(function(category) { return category.name == 'Product Spend Profile' });
                if (productSpendCategory) {
                    setAttributeColor(productSpendCategory, '#B887B6');
                }
            }
        }
    }

    // Get Internal category list
    var internalCategoryObj = data.InternalAttributes;
    if (internalCategoryObj) {
        $scope.internalPredictorTotal = internalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_INTERNAL_TITLE");
        $scope.internalCategories = internalCategoryObj.categories;
        $scope.showInternalCategories = internalCategoryObj.total > 0;
        $scope.formatAttributes($scope.internalCategories);

        // Get External category list
        var externalCategoryObj = data.ExternalAttributes;
        $scope.externalPredictorTotal = externalCategoryObj.total + " " + ResourceUtility.getString("TOP_PREDICTORS_EXTERNAL_TITLE");
        $scope.externalCategories = externalCategoryObj.categories;
        $scope.formatAttributes($scope.externalCategories);
        $scope.showExternalCategories = externalCategoryObj.total > 0;
    }

    // Calculate total
    var totalPredictors = data.TotalPredictors;
    $scope.topPredictorTitle = totalPredictors + " " + ResourceUtility.getString("TOP_PREDICTORS_TITLE");

    $scope.generateCategoryLabel = function(category) {
    	return $sce.trustAsHtml(category.name + '<span style="background-color:' + category.color + '">' + category.count + '</span>');
    };

    function setAttributeColor(category, color) {
        category.color = color;
        if (category.children) {
            category.children.forEach(function(attr) {
                attr.color = color;
            });
        }
    }

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

        if (chartData) {
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
        }

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

        if (path) {
            path.data(partition.nodes).transition().duration(1000).attrTween("d", arcTween);
        }
    };

    $scope.drawSummaryChart();

    $scope.exportClicked = function () {
    	var csvRows = TopPredictorService.GetTopPredictorExport(data);
        var lineArray = [];

        csvRows.forEach(function (infoArray, index) {
            var line = infoArray.join(",");
            lineArray.push(line);
        });
        
        var csvContent = lineArray.join("\n");
        var element = document.createElement("a");

        element.setAttribute(
          "href",
          "data:text/csv;charset=utf-8," + encodeURIComponent(csvContent)
        );
        element.setAttribute("download", "attributes.csv");
        element.style.display = "none";
        document.body.appendChild(element);
        element.click();
        document.body.removeChild(element);

    };

    $scope.backToSummaryClicked = function () {

        if($scope.hasAccountAttributes){
            for (var attribute of chartData.children) {
                if(attribute.categoryName === 'My Attributes'){
                    attribute.name = 'ACCOUNT_ATTRIBUTES';
                    attribute.categoryName = 'ACCOUNT_ATTRIBUTES';
                }
            }
        }

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

        if(category.name === 'My Attributes'){
            category.name = 'ACCOUNT_ATTRIBUTES';
            category.categoryName = 'ACCOUNT_ATTRIBUTES';
        }

        clearTimeout(showAttributeTimeout);
        var categoryList = TopPredictorService.GetAttributesByCategory(data, category.name, category.color, 50);
        highlightSelectedCategory(category);
        category.activeClass = "active";
        TopPredictorService.CalculateAttributeSize(categoryList);
        $scope.backToSummaryView = true;
        var prefix = categoryList.length >= 50 ? ResourceUtility.getString("TOP_PREDICTORS_CHART_CATEGORY_HEADER_PREFIX") : "";

        if(category.name === 'ACCOUNT_ATTRIBUTES'){
            category.name = 'My Attributes';
            category.categoryName = 'My Attributes';
        }

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
        var topPredictorAttributeHover = $("#topPredictorAttributeHover");
        var scope = $rootScope.$new();
        scope.mouseX = mouseX;
        scope.mouseY = mouseY;
        scope.selectedPath = path; // for anchoring tail and hoverElem position
        var displaySimpleBuckets = false;
        for (var i = 0; i < data.ModelDetails.ModelSummaryProvenanceProperties.length; i++) {
            if (data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.option == "IsV2ProfilingEnabled") {
                displaySimpleBuckets = data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.value == "true";
            }
        }

        scope.data = displaySimpleBuckets ?
            TopPredictorService.FormatSimpleBuckets(attributeName, attributeColor, data) :
            TopPredictorService.FormatDataForAttributeValueChart(attributeName, attributeColor, data);

        $compile(topPredictorAttributeHover.html('<div data-top-predictor-attribute-widget></div>'))(scope);

    }

    function hideAttributeHover () {

        var topPredictorAttributeHover = $("#topPredictorAttributeHover");
        topPredictorAttributeHover.hide();
        topPredictorAttributeHover.empty();

    }

})

.directive('topPredictorWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});