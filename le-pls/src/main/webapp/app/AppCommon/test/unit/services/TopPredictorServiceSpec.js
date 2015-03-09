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

    describe('GetAttributesByCategory tests', function () {
        it('should only return 50 attributes if 50 is specified and there are more than 50 in total', function () {
            var categoryList = topPredictorService.GetAttributesByCategory(sampleModelSummary, "Technologies", "blah", 50);
            expect(categoryList.length).toEqual(50);
        });
    });


});