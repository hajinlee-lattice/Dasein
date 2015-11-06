'use strict';

describe('LeadSampleSpec Tests', function () {
    var modelSummary,
        topSample,
        bottomSample;

    beforeEach(function () {
        module('mainApp.models.services.ModelService');
        module('test.testData.TopPredictorTestDataService');

        inject(['ModelService', 'TopPredictorTestDataService',
            function (ModelService, TopPredictorTestDataService) {
                modelSummary = TopPredictorTestDataService.GetSampleModelSummary();
                topSample = ModelService.FormatLeadSampleData(modelSummary.TopSample);
                bottomSample = ModelService.FormatLeadSampleData(modelSummary.BottomSample);
            }
        ]);
    });

    //==================================================
    // TopSample Tests
    //==================================================
    describe('top-sample tests', function () {
        it('expectations should hold (if the test-data is sufficient and not spam-ridden)', function () {
            expect(topSample.length).toEqual(10);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Converted ? 1 : 0); }, 0)).toEqual(7);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Converted ? 0 : 1); }, 0)).toEqual(3);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Contact ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Contact.length ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Company ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Company.length ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Score ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(topSample, function(acc, e) { return acc + (e.Score === parseInt(e.Score) ? 0 : 1); }, 0)).toEqual(0);
            expect(Object.keys(_.groupBy(topSample, function(e) { return e.Company; })).length).toEqual(10);
        });
    });

    //==================================================
    // BottomSample Tests
    //==================================================
    describe('bottom-sample tests', function () {
        it('expectations should hold (if the test-data is sufficient)', function () {
            expect(bottomSample.length).toEqual(10);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Converted ? 1 : 0); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Contact ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Contact.length ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Company ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Company.length ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Score ? 0 : 1); }, 0)).toEqual(0);
            expect(_.reduce(bottomSample, function(acc, e) { return acc + (e.Score === parseInt(e.Score) ? 0 : 1); }, 0)).toEqual(0);
            expect(Object.keys(_.groupBy(bottomSample, function(e) { return e.Company; })).length).toEqual(10);
        });
    });
});