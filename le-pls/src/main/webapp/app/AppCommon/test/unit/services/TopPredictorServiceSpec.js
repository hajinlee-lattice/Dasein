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
});