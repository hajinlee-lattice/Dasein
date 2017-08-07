'use strict';

describe('TopPredictorServiceSpec Tests', function () {
    var topPredictorService,
        analyticAttributeUtility,
        stringUtility,
        sampleModelSummary,
        hoverTestModelSummary;

    beforeEach(function () {
        module('mainApp.appCommon.utilities.StringUtility');
        module('mainApp.appCommon.utilities.ResourceUtility');
        module('mainApp.appCommon.utilities.UnderscoreUtility');
        module('mainApp.appCommon.utilities.AnalyticAttributeUtility');
        module('mainApp.appCommon.services.TopPredictorService');
        module('test.testData.TopPredictorTestDataService');

        inject(['TopPredictorService', 'AnalyticAttributeUtility', 'StringUtility', 'ResourceUtility', '_', 'TopPredictorTestDataService',
            function (TopPredictorService, AnalyticAttributeUtility, StringUtility, ResourceUtility, _, TopPredictorTestDataService) {
                topPredictorService = TopPredictorService;
                analyticAttributeUtility = AnalyticAttributeUtility;
                stringUtility = StringUtility;
                sampleModelSummary = TopPredictorTestDataService.GetSampleModelSummary();
                hoverTestModelSummary = TopPredictorTestDataService.GetHoverTestData();
            }
        ]);

    });

    describe('GetSuppressedCategories tests', function () {
        it('should return 1 as the suppressed category in the test modelSummary', function () {
            var toReturn = topPredictorService.GetSuppressedCategories(sampleModelSummary);
            var expected = [{name: 'Financial', categoryName: 'Financial', UncertaintyCoefficient: 0.000023156115769041482, size: 1, color: null, children: []}];
            expect(toReturn.length).toEqual(1);
            expect(toReturn).toEqual(expected);
        });
    });
    
    describe('GetTopCategories tests', function () {
        it('should return 8 as the top categories in the test modelSummary', function () {
            var toReturn = topPredictorService.GetTopCategories(sampleModelSummary);
            expect(toReturn.length).toEqual(8);
        });
    });
    
    describe('GetAllCategories tests', function () {
        it('should return 9 as the total number of categories in the test modelSummary', function () {
            var toReturn = topPredictorService.GetAllCategories(sampleModelSummary);
            expect(toReturn.length).toEqual(9);
        });
    });

    describe('GetNumberOfAttributesByCategory tests', function () {
        var testCategory = {
            name: "Technologies",
            count: 0,
            color: "#4bd1bb",
            activeClass: ""
        };

        var expected = {
            totalAttributeValues: 186,
            total: 57,
            categories: [{
                name: "Technologies",
                count: 57,
                color: "#4bd1bb",
                activeClass: ""
            }]
        };
        it('should return 57 as the total for Technologies attribute-value in the test modelSummary', function () {
            var toReturn = topPredictorService.GetNumberOfAttributesByCategory([testCategory], true, sampleModelSummary);
            expect(toReturn).toEqual(expected);
        });
    });

    describe('SortBySize tests', function () {

        var unsorted = [{
                size: 1
            }, {
                size: 2.56
            }, {
                size: 6.55
            }
        ];

        var sorted = [{
                size: 6.55
            }, {
                size: 2.56
            }, {
                size: 1
            }
        ];

        it('should return the list sorted by size descending', function () {
            var toReturn = unsorted.sort(topPredictorService.SortBySize);
            expect(toReturn).toEqual(sorted);
        });
    });

    describe('SortByPredictivePower tests', function () {

        var unsorted = [{
                UncertaintyCoefficient: 2
            }, {
                UncertaintyCoefficient: 1
            }, {
                UncertaintyCoefficient: 5
            }
        ];

        var sorted = [{
                UncertaintyCoefficient: 5
            }, {
                UncertaintyCoefficient: 2
            }, {
                UncertaintyCoefficient: 1
            }
        ];

        it('should return the list sorted by UncertaintyCoefficient descending', function () {
            var toReturn = unsorted.sort(topPredictorService.SortByPredictivePower);
            expect(toReturn).toEqual(sorted);
        });
    });

    describe('SortByCategoryName tests', function () {

        var unsorted = [{
                name: "b"
            }, {
                name: "a"
            }, {
                name: "d"
            },  {
                name: "c"
            }
        ];

        var sorted = [{
                name: "a"
            }, {
                name: "b"
            }, {
                name: "c"
            },  {
                name: "d"
            }
        ];

        it('should return the list sorted by name ascending', function () {
            var toReturn = unsorted.sort(topPredictorService.SortByCategoryName);
            expect(toReturn).toEqual(sorted);
        });
    });

    // START: Top Predictor acceptance tests

    describe('The categories are colored and ordered', function () {
        it('should return a constant set of colors so the same categories get about the same color', function () {
            var topCategories = topPredictorService.GetTopCategories(sampleModelSummary);
            var toReturn = topPredictorService.AssignColorsToCategories(topCategories);

            expect(topCategories[0].color).toEqual("#4bd1bb");
            expect(topCategories[1].color).toEqual("#00a2d0");
            expect(topCategories[2].color).toEqual("#f6b300");
            expect(topCategories[3].color).toEqual("#a981e1");
            expect(topCategories[4].color).toEqual("#95cb2c");
            expect(topCategories[5].color).toEqual("#9a9a9a");
        });
    });

    describe('The donut chart is colored, ordered and sized', function () {
        it('should return a correct chartObject that is used to populate the donut chart', function () {
            var chartData = topPredictorService.FormatDataForTopPredictorChart(sampleModelSummary);

            // Verify there will only be 3 attributes per category
            expect(chartData.attributesPerCategory).toEqual(3);

            // Verify that the chart data is sortied by highest predictive power
            var firstcategory = chartData.children[0];
            expect(firstcategory.name).toEqual("Technologies");
            expect(firstcategory.children.length).toEqual(3);
            var secondcategory = chartData.children[1];
            expect(secondcategory.name).toEqual("Website");

            // Verify that the first attribute for the first category is a large size and has the same color as the category
            var firstcategoryFirstAttribute = firstcategory.children[0];
            expect(firstcategoryFirstAttribute.size).toEqual(6.55);
            expect(firstcategoryFirstAttribute.color).toEqual(firstcategory.color);
        });
    });

    describe('Verify that: Total # of Attributes == Sum of # of each attributes == Attributes in CSV', function () {
        it('should return the same number of attributes for the CSV and UI', function () {
            var topCategories = topPredictorService.GetTopCategories(sampleModelSummary);
            var externalAttributes = topPredictorService.GetNumberOfAttributesByCategory(topCategories, true, sampleModelSummary);
            var internalAttributes = topPredictorService.GetNumberOfAttributesByCategory(topCategories, false, sampleModelSummary);
            var uiTotal = externalAttributes.totalAttributeValues + internalAttributes.totalAttributeValues;

            var csvAttributes = topPredictorService.GetTopPredictorExport(sampleModelSummary);
            // Subtracting 1 because of the column headers
            var csvTotal = csvAttributes.length - 1;

            expect(uiTotal).toEqual(csvTotal);
        });
    });

    describe('Upload a model with no External Attributes (Lattice), verify the UI remove the header for External Attributes', function () {
        it('should return chart data with no external attributes', function () {

            // Turn all predictors into Internal predictors for testing purposes
            for (var x = 0; x < sampleModelSummary.Predictors.length; x++) {
                sampleModelSummary.Predictors[x].Tags = ["Internal"];
            }
            var chartData = topPredictorService.FormatDataForTopPredictorChart(sampleModelSummary);

            var externalAttributesObj = topPredictorService.GetNumberOfAttributesByCategory(chartData.children, true, sampleModelSummary);
            expect(externalAttributesObj.total).toEqual(0);
        });
    });

    describe('Upload a model with no Internal Attributes (MAP), verify the UI remove the header for Internal Attributes', function () {
        it('should return chart data with no internal attributes', function () {

            // Turn all predictors into External predictors for testing purposes
            for (var x = 0; x < sampleModelSummary.Predictors.length; x++) {
                sampleModelSummary.Predictors[x].Tags = ["External"];
            }
            var chartData = topPredictorService.FormatDataForTopPredictorChart(sampleModelSummary);

            var internalAttributesObj = topPredictorService.GetNumberOfAttributesByCategory(chartData.children, false, sampleModelSummary);
            expect(internalAttributesObj.total).toEqual(0);
        });
    });
    
    describe('ShowBasedOnInternalOrExternal tests', function () {
        var nullPredictor = {
            Tags: null
        };
        it('should return false if Tags is null', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(nullPredictor, true);
            expect(toReturn).toEqual(false);
        });

        var externalPredictor = {
            Tags: ["External"]
        };
        it('should return true if Tags contains External and we are looking for External', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(externalPredictor, true);
            expect(toReturn).toEqual(true);
        });

        var externalPredictor2 = {
            Tags: ["External"]
        };
        it('should return false if Tags contains External and we are looking for Internal', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(externalPredictor2, false);
            expect(toReturn).toEqual(false);
        });

        var internalPredictor = {
            Tags: ["Internal"]
        };
        it('should return true if Tags contains Internal and we are looking for Internal', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(internalPredictor, false);
            expect(toReturn).toEqual(true);
        });

        var internalPredictor2 = {
            Tags: ["Internal"]
        };
        it('should return false if Tags contains Internal and we are looking for External', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(internalPredictor, true);
            expect(toReturn).toEqual(false);
        });

        var badPredictor = {
            Tags: ["Nope"]
        };
        it('should return false if Tags contains an invalid value', function () {
            var toReturn = topPredictorService.ShowBasedOnInternalOrExternal(badPredictor, true);
            expect(toReturn).toEqual(false);
        });
    });
    
    
    describe('GetAttributesByCategory tests', function () {
        it('should only return 50 attributes if 50 is specified and there are more than 50 in total', function () {
            var categoryList = topPredictorService.GetAttributesByCategory(sampleModelSummary, "Technologies", "blah", 50);
            expect(categoryList.length).toEqual(50);
        });
    });

    describe('For an attribute with a blank category name, it should NOT show up in UI', function () {
        it('should return chart data with no null category names', function () {
            var chartData = topPredictorService.FormatDataForTopPredictorChart(sampleModelSummary);
            for (var i = 0; i < chartData.children.length; i++) {
                expect(chartData.children[i].name).not.toBe(null);
            }
        });
    });

    describe('Format attribute data as required', function () {
        function printHoverData (elementList, predictor) {
            if (predictor != null) {}
            _.each(elementList, function(e){
                console.log(e.name + ": " + e.lift + " " + e.percentTotal);
            });
        }

        it('should return "null" and "not available" as "N/A"', function () {

            // examine the sum of counts equals the total leads for each predictor
            _.each(hoverTestModelSummary.Predictors, function(p){
                expect(_.reduce(p.Elements, function(memo, e){ return memo + e.Count; }, 0)).toEqual(hoverTestModelSummary.ModelDetails.TotalLeads);
                // examine total average lift should be one
                var weightedLiftSum = _.reduce(p.Elements, function(memo, e){
                    return e.Lift * e.Count + memo;
                }, 0);
                expect(Math.round(weightedLiftSum)).toEqual(hoverTestModelSummary.ModelDetails.TotalLeads);
            });

            var chartData = topPredictorService.FormatDataForAttributeValueChart("NullAttribute", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[0].name).toEqual('N/A');
        });

        it('should put "N/A" at bottom and sort "Yes" and "No"', function () {
            var chartData = topPredictorService.FormatDataForAttributeValueChart("BooleanAttribute", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[0].name).toEqual('Yes');
            expect(chartData.elementList[1].name).toEqual('No');
            expect(chartData.elementList[2].name).toEqual('N/A');

            chartData = topPredictorService.FormatDataForAttributeValueChart("BooleanAttribute2", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[0].name).toEqual('No');
            expect(chartData.elementList[1].name).toEqual('Yes');
            expect(chartData.elementList[2].name).toEqual('N/A');
            
        });

        it('should not apply doOtherBucket to Boolean type bucekts', function () {
        	var chartData = topPredictorService.FormatDataForAttributeValueChart("BooleanAttribute3", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[0].name).toEqual('No');
            expect(chartData.elementList[1].name).toEqual('Yes');
            expect(chartData.elementList[2].name).toEqual('N/A');
            expect(chartData.elementList[0].percentTotal) == 99.5;
            expect(chartData.elementList[1].percentTotal) == 0.4;
            expect(chartData.elementList[2].percentTotal) == 0.1;
        });
        
        it('should put "Other" and "N/A" at bottom', function () {
            var chartData = topPredictorService.FormatDataForAttributeValueChart("CategoricalAttribute", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[chartData.elementList.length - 1].name).toEqual('N/A');
            expect(chartData.elementList[chartData.elementList.length - 2].name).toEqual('Other');

            chartData = topPredictorService.FormatDataForAttributeValueChart("CategoricalAttribute4", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[chartData.elementList.length - 1].name).toEqual('N/A');
            expect(chartData.elementList[chartData.elementList.length - 2].name).toEqual('Other');
        });


        it('should merge extra buckets into "Other"', function () {
            var chartData = topPredictorService.FormatDataForAttributeValueChart("CategoricalAttribute2", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList.length).toEqual(7);
            expect(chartData.elementList[chartData.elementList.length - 1].name).toEqual('N/A');
            expect(chartData.elementList[chartData.elementList.length - 2].name).toEqual('Other');
            
            // the sum of percentage should be 100
            expect(Math.round(_.reduce(chartData.elementList, function(memo, e){
                return memo + e.percentTotal;
            }, 0))).toEqual(100);

            // examine total average lift should be one
            var weightedLiftSum = _.reduce(chartData.elementList, function(memo, e){
                return e.lift * e.percentTotal + memo;
            }, 0);
           expect(Math.round(weightedLiftSum)).toEqual(100);
        });


        it('should not merge buckets into "Other" if there are exactly 7 buckets', function () {
            var chartData = topPredictorService.FormatDataForAttributeValueChart("CategoricalAttribute3", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList.length).toEqual(7);
            expect(chartData.elementList[chartData.elementList.length - 1].name).toEqual('N/A');
            expect(chartData.elementList[chartData.elementList.length - 2].name).toEqual('Bucket 2');
        });
    });
    
    //DP-932
    describe('For a continuous attribute', function () {
    	
        it('should return chart data with first element named "Available" when it has two attribute values, one of which is an "NA" bucket', function () {
        	var chartData = topPredictorService.FormatDataForAttributeValueChart("ContinuousAttribute", "#FFFFFF", hoverTestModelSummary);
            expect(chartData.elementList[0].name).toEqual("Available");
            expect(chartData.elementList[1].name).toEqual('N/A');
        });
    });
    
});