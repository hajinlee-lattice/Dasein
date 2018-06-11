'use strict';

describe('ProductTotalFactorySpec Tests', function () {
    var REPRESENTATIVE_ACCOUNTS = 5;
    var productTotal, p0;

    beforeEach(function () {
        module('mainApp.appCommon.factory.ProductTotalFactory');
        module('test.testData.PurchaseHistoryTestDataService');
        inject(['ProductTotal', 'PurchaseHistoryTestDataService',
            function (ProductTotal, PurchaseHistoryTestDataService) {
                productTotal = ProductTotal;
                p0 = new productTotal(REPRESENTATIVE_ACCOUNTS);
            }
        ]);
    });

    describe('ProductTotal constructor', function () {
        it('should be a constructor (function)', function () {
            expect(typeof productTotal).toEqual('function');
        });

        it('should be called with "new"', function () {
            expect(typeof p0).toEqual('object');
        });

        it('should be constructor of instance', function () {
            expect(p0 instanceof productTotal).toBeTruthy();
        });
    });
    
    describe('ProductTotal properties', function () {
        it('should have properties initialized to 0 ', function () {
            var props = [
                'TotalSpend',
                'TotalVolume',
                'TransactionCount',
                'AverageSpend'
            ];
            for (var k = 0; k < props.length; k++) {
                expect(p0[props[k]]).toEqual(0);
            }
        });

        it('should have representativeAccounts', function () {
            expect(p0.RepresentativeAccounts).toEqual(REPRESENTATIVE_ACCOUNTS);
        });

        it('should have default RepresentativeAccounts set to 1', function () {
            var pTotal = new productTotal();
            expect(pTotal.RepresentativeAccounts).toEqual(1);
        });
        
        it('should have method ProductTotal.aggregate', function () {
            expect(typeof p0.aggregate).toEqual('function');
        });
    });

    describe('ProductTotal.aggregate', function () {
        var p1;
        beforeEach(function () {
            p0.TotalVolume = 13;
            p0.TotalSpend = 888;
            p0.TransactionCount = 7;

            p1 = new productTotal(REPRESENTATIVE_ACCOUNTS);
            p1.TotalVolume = 10;
            p1.TotalSpend = 999;
            p1.TransactionCount = 4;
        });

        it('should aggregate argument into ProductTotal instance', function () {
            p0.aggregate(p1);

            // p0 aggregated (sum)
            expect(p0.TotalVolume).toEqual(23);
            expect(p0.TotalSpend).toEqual(1887);
            expect(p0.TransactionCount).toEqual(11);
            expect(p0.AverageSpend).toEqual(1887/REPRESENTATIVE_ACCOUNTS);

            // p1 remains unchanged
            expect(p1.TotalVolume).toEqual(10);
            expect(p1.TotalSpend).toEqual(999);
            expect(p1.TransactionCount).toEqual(4);
            expect(p1.AverageSpend).toEqual(0);
        });

        it('should aggregate AverageSpend using representativeCount', function () {
            p0.aggregate(p1);

            expect(p0.AverageSpend).toEqual(1887/REPRESENTATIVE_ACCOUNTS);
        });
    });
});
