'use strict';

describe('ModelDetailSpec Tests', function () {
    var modelSummary,
        modelDetails;

    beforeEach(function () {
        module('mainApp.models.services.ModelService');
        module('test.testData.TopPredictorTestDataService');

        inject(['ModelService', 'TopPredictorTestDataService',
            function (ModelService, TopPredictorTestDataService) {
                modelSummary = TopPredictorTestDataService.GetSampleModelSummary();
                modelDetails = modelSummary.ModelDetails;
            }
        ]);
    });

    describe('name tests', function () {
        it('name should be present', function () {
            expect(modelDetails.Name ? true : false).toBe(true);
        });
    });

    describe('lookup-id tests', function () {
        it('lookup-id should be a pipe-delimited-triple', function () {
            expect(modelDetails.LookupID.split("|").length).toEqual(3);
        });
    });

    describe('lead tests', function () {
        it('total-leads should equal testing-leads plus training-leads', function () {
            expect(modelDetails.TestingLeads + modelDetails.TrainingLeads).toEqual(modelDetails.TotalLeads);
        });
    });

    describe('conversion tests', function () {
        it('total-conversions should equal testing-conversions plus training-conversions', function () {
            expect(modelDetails.TestingConversions + modelDetails.TrainingConversions).toEqual(modelDetails.TotalConversions);
        });
    });

    describe('roc-score tests', function () {
        it('roc-score should be a float', function () {
            expect(modelDetails.RocScore === parseFloat(modelDetails.RocScore)).toBe(true);
        });
    });

    describe('construction-time tests', function () {
        it('construction-time should be an int', function () {
            expect(modelDetails.ConstructionTime === parseInt(modelDetails.ConstructionTime)).toBe(true);
        });

        it('construction-time should allow for date construction', function () {
            var date = new Date(modelDetails.ConstructionTime * 1000);
            expect(date.getFullYear() === parseInt(date.getFullYear())).toBe(true);
            expect(date.getMonth() === parseInt(date.getMonth())).toBe(true);
            expect(date.getDate() === parseInt(date.getDate())).toBe(true);
        });
    });
});