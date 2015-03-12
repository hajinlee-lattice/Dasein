'use strict';

describe('TopPredictorServiceSpec Tests', function () {
    var topPredictorService,
        analyticAttributeUtility,
        stringUtility,
        sampleModelSummary;

    beforeEach(function () {
        module('mainApp.appCommon.utilities.StringUtility');
        module('mainApp.appCommon.utilities.AnalyticAttributeUtility');
        module('mainApp.appCommon.services.TopPredictorService');
        module('test.testData.TopPredictorTestDataService');

        inject(['TopPredictorService', 'AnalyticAttributeUtility', 'StringUtility', 'TopPredictorTestDataService',
            function (TopPredictorService, AnalyticAttributeUtility, StringUtility, TopPredictorTestDataService) {
                topPredictorService = TopPredictorService;
                analyticAttributeUtility = AnalyticAttributeUtility;
                stringUtility = StringUtility;
                sampleModelSummary = TopPredictorTestDataService.GetSampleModelSummary();
            }
        ]);

    });
    
    describe('ShowBasedOnTags tests', function () {
        var nullPredictor = {
            Tags: null
        };
        it('should return false if Tags is null', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(nullPredictor, true);
            expect(toReturn).toEqual(false);
        });
        
        var externalPredictor = {
            Tags: ["External"]
        };
        it('should return true if Tags contains External and we are looking for External', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(externalPredictor, true);
            expect(toReturn).toEqual(true);
        });
        
        var externalPredictor2 = {
            Tags: ["External"]
        };
        it('should return false if Tags contains External and we are looking for Internal', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(externalPredictor2, false);
            expect(toReturn).toEqual(false);
        });
        
        var internalPredictor = {
            Tags: ["Internal"]
        };
        it('should return true if Tags contains Internal and we are looking for Internal', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(internalPredictor, false);
            expect(toReturn).toEqual(true);
        });
        
        var internalPredictor2 = {
            Tags: ["Internal"]
        };
        it('should return false if Tags contains Internal and we are looking for External', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(internalPredictor, true);
            expect(toReturn).toEqual(false);
        });
        
        var badPredictor = {
            Tags: ["Nope"]
        };
        it('should return false if Tags contains an invalid value', function () {
            var toReturn = topPredictorService.ShowBasedOnTags(badPredictor, true);
            expect(toReturn).toEqual(false);
        });
    });

    describe('GetAttributesByCategory tests', function () {
        it('should only return 50 attributes if 50 is specified and there are more than 50 in total', function () {
            var categoryList = topPredictorService.GetAttributesByCategory(sampleModelSummary, "Technologies", "blah", 50);
            expect(categoryList.length).toEqual(50);
        });
    });
    
    describe('GetNumberOfAttributesByCategory tests', function () {
        var testCategory = {
            name: "Technologies",
            count: 0,
            color: "#4bd1bb"
        };
        
        var expected = {
            total: 188,
            categories: [{
                name: "Technologies",
                count: 188,
                color: "#4bd1bb"
            }]
        };
        it('should return 188 as the total for Technologies attribute-value in the test modelSummary', function () {
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
            var uiTotal = externalAttributes.total + internalAttributes.total;
            
            var csvAttributes = topPredictorService.GetTopPredictorExport(sampleModelSummary);
            // Subtracting 1 because of the column headers
            var csvTotal = csvAttributes.length - 1;
            
            expect(uiTotal).toEqual(csvTotal);
        });
    });
});