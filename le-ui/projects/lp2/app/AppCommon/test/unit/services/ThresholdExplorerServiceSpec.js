'use strict';

describe('ThresholdExplorerServiceSpec Tests', function () {
    var thresholdExplorerService,
        modelSummary,
        thresholdData,
        thresholdChartData,
        thresholdDecileData,
        thresholdLiftData,
        thresholdExportData;

    beforeEach(function () {
        module('mainApp.appCommon.services.ThresholdExplorerService');
        module('test.testData.TopPredictorTestDataService');

        inject(['ThresholdExplorerService', 'TopPredictorTestDataService',
            function (ThresholdExplorerService, TopPredictorTestDataService) {
                thresholdExplorerService = ThresholdExplorerService;
                modelSummary = TopPredictorTestDataService.GetSampleModelSummary();
                thresholdData = ThresholdExplorerService.PrepareData(modelSummary);
                thresholdChartData = thresholdData.ChartData;
                thresholdDecileData = thresholdData.DecileData;
                thresholdLiftData = thresholdData.LiftData;
                thresholdExportData = ThresholdExplorerService.PrepareExportData(modelSummary);
            }
        ]);
    });

    //==================================================
    // ChartData Tests
    //==================================================
    describe('chart-data tests', function () {
        it('chart-data should have 101 elements', function () {
            expect(thresholdChartData.length).toEqual(101);
        });

        it('chart-data leads should increment by 1', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].leads).toEqual(i);
            }
        });

        it('chart-data leads should start with 0,1', function () {
            expect(thresholdChartData[0].leads).toEqual(0);
            expect(thresholdChartData[1].leads).toEqual(1);
        });

        it('chart-data leads should end with 99,100', function () {
            expect(thresholdChartData[99].leads).toEqual(99);
            expect(thresholdChartData[100].leads).toEqual(100);
        });

        it('chart-data score should decrement by 1', function () {
            expect(thresholdChartData[0].score).toEqual(0);
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].score).toEqual(101 - i);
            }
        });

        it('chart-data score should start with 0,100', function () {
            expect(thresholdChartData[0].score).toEqual(0);
            expect(thresholdChartData[1].score).toEqual(100);
        });

        it('chart-data score should end with 2,1', function () {
            expect(thresholdChartData[99].score).toEqual(2);
            expect(thresholdChartData[100].score).toEqual(1);
        });

        it('chart-data conversions should increase', function () {
            for (var i = 1; i < 101; i++) {
                expect(parseInt(thresholdChartData[i].conversions.toFixed(0)) >=
                    parseInt(thresholdChartData[i - 1].conversions.toFixed(0))).toBe(true);
            }
        });

        it('chart-data conversions should start with 0', function () {
            expect(thresholdChartData[0].conversions).toEqual(0);
        });

        it('chart-data conversions should end with 100', function () {
            expect(thresholdChartData[100].conversions).toEqual(100);
        });

        it('chart-data leftLift should start with 0', function () {
            expect(thresholdChartData[0].leftLift).toEqual(0);
        });

        it('chart-data leftLift should end with 1', function () {
            expect(thresholdChartData[100].leftLift).toEqual(1);
        });

        it('chart-data rightLift should start with 1', function () {
            expect(thresholdChartData[0].rightLift).toEqual(1);
        });

        it('chart-data rightLift should end with 0', function () {
            expect(thresholdChartData[100].rightLift).toEqual(0);
        });
    });

    //==================================================
    // DecileData Tests
    //==================================================
    describe('decile-data tests', function () {
        it('decile-data should have 10 conversions', function () {
            expect(thresholdDecileData.length).toBe(10);
        });

        it('decile-data conversions should increase', function () {
            for (var i = 1; i < 10; i++) {
                expect(parseInt(thresholdDecileData[i]) >=
                    parseInt(thresholdDecileData[i - 1])).toBe(true);
            }
        });
    });

    //==================================================
    // ChartDecile Tests
    //==================================================
    describe('chart-decile tests', function () {
        it('chart-decile conversions should match', function () {
            for (var i = 0; i < 10; i++) {
                expect(thresholdChartData[10 * (i + 1)].conversions.toFixed(0)).
                    toEqual(thresholdDecileData[i].toFixed(0));
            }
        });
    });

    //==================================================
    // LiftData Tests
    //==================================================
    describe('lift-data tests', function () {
        it('lift-data should have 10 values', function () {
            expect(thresholdLiftData.length).toBe(10);
        });

        it('lift-data conversions should sum to 1', function () {
            var sum = thresholdLiftData.reduce(function(agg, d){
                return agg + d;
            }, 0);
            var avg = (sum/thresholdLiftData.length).toFixed(2);
            expect(avg).toBe("1.00");
        });
    });

    //==================================================
    // ChartDecile Tests
    //==================================================
    describe('chart-decile tests', function () {
        it('chart-decile conversions should match', function () {
            for (var i = 0; i < 10; i++) {
                expect(thresholdChartData[10 * (i + 1)].conversions.toFixed(0)).
                    toEqual(thresholdDecileData[i].toFixed(0));
            }
        });
    });

    //==================================================
    // Export Tests (Good Enough For Now)
    //==================================================
    describe('export tests', function () {
        it('chart-export score should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].score).
                    toEqual(thresholdExportData[i][0]);
            }
        });

        it('chart-export leads should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].leads).
                    toEqual(parseInt(thresholdExportData[i][1].slice(0, -1)));
            }
        });

        it('chart-export conversions should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].conversions.toFixed(0)).
                    toEqual(thresholdExportData[i][2].slice(0, -1));
            }
        });

        it('chart-export leftLift should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].leftLift.toFixed(2)).
                    toEqual(thresholdExportData[i][3]);
            }
        });

        it('chart-export rightLift should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(thresholdChartData[i].rightLift.toFixed(2)).
                    toEqual(thresholdExportData[i][4]);
            }
        });

        it('segment-export count should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(modelSummary.Segmentations[0].Segments[i - 1].Count).
                    toEqual(thresholdExportData[i][5]);
            }
        });

        it('segment-export converted should match', function () {
            for (var i = 1; i < 101; i++) {
                expect(modelSummary.Segmentations[0].Segments[i - 1].Converted).
                    toEqual(thresholdExportData[i][6]);
            }
        });
    });
});