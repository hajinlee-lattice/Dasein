'use strict';

describe('PurchaseHistoryUtilitySpec Tests', function () {
    var purchaseHistoryUtility,
        resourceUtility,
        moment;

    beforeEach(function () {
        module('mainApp.appCommon.utilities.PurchaseHistoryUtility');
        module('mainApp.appCommon.utilities.ResourceUtility');
        inject(['PurchaseHistoryUtility', 'ResourceUtility', '$window',
            function (PurchaseHistoryUtility, ResourceUtility, $window) {
                moment = $window.moment;
                purchaseHistoryUtility = PurchaseHistoryUtility;
                if (ResourceUtility.configStrings === null) {
                    ResourceUtility.configStrings = {};
                }
                ResourceUtility.configStrings.CURRENCY_SYMBOL = '$';
            }
        ]);
    });

    describe('PurchaseHistoryUtility.periodToMomentFormat', function () {
        var toReturn;

        it('should retrieve format for "D"', function () {
            toReturn = purchaseHistoryUtility.periodToMomentFormat['D'];
            expect(toReturn).toEqual('YYYY MMM DD');
        });

        it('should retrieve format for "M"', function () {
            toReturn = purchaseHistoryUtility.periodToMomentFormat['M'];
            expect(toReturn).toEqual('YYYY MMM');
        });

        it('should retrieve format for "Q"', function () {
            toReturn = purchaseHistoryUtility.periodToMomentFormat['Q'];
            expect(toReturn).toEqual('YYYY Q');
        });

        it('should retrieve format for "Y"', function () {
            toReturn = purchaseHistoryUtility.periodToMomentFormat['Y'];
            expect(toReturn).toEqual('YYYY');
        });

        it('should return undefined for everything else', function () {
            toReturn = purchaseHistoryUtility.periodToMomentFormat['foo'];
            expect(toReturn).toEqual(undefined);
        });
    });

    describe('PurchaseHistoryUtility.formatNumber', function () {
        var toReturn;

        it('should return unabbreviated', function () {
            toReturn = purchaseHistoryUtility.formatNumber(12345, false);
            expect(toReturn).toEqual('12,345');
        });
        it('should return number < 1 million abbreviated with 1 decimal', function () {
            toReturn = purchaseHistoryUtility.formatNumber(12345, true);
            expect(toReturn).toEqual('12.3K');
        });
        it('should return number >= 1 million abbreviated with 2 decimals', function () {
            toReturn = purchaseHistoryUtility.formatNumber(987654321, true);
            expect(toReturn).toEqual('987.65M');
        });
    });

    describe('PurchaseHistoryUtility.formatDollar', function () {
        var toReturn;

        it('should return formatted number with dollar sign', function () {
            toReturn = purchaseHistoryUtility.formatDollar(12345, false);
            expect(toReturn).toMatch(/^\$/);
            expect(toReturn).toEqual('$12,345');
        });

        it('should return formatted negative number with dollar sign', function () {
            toReturn = purchaseHistoryUtility.formatDollar(-12345, false);
            expect(toReturn).toMatch(/^(\-\s\$)/);
            expect(toReturn).toEqual('- $12,345');
        });
    });

    describe('PurchaseHistoryUtility.formatPercent', function () {
        var toReturn;

        it('should return perfect up to 1 decimal places', function () {
            toReturn = purchaseHistoryUtility.formatPercent(56.78);
            expect(toReturn).toMatch(/\%$/);
            expect(toReturn).toEqual('56.8%');
        });
    });

    describe('PurchaseHistoryUtility.getPrevYearPeriod', function () {
        var toReturn;

        it('should return the period of pervious year give a Month period', function () {
            toReturn = purchaseHistoryUtility.getPrevYearPeriod('2013 Apr');
            expect(toReturn).toEqual('2012 Apr');
        });

        it('should return the period of pervious year give a Quarter period', function () {
            toReturn = purchaseHistoryUtility.getPrevYearPeriod('2014 2');
            expect(toReturn).toEqual('2013 2');
        });
        
        it('should return the period of pervious year give a Year period', function () {
            toReturn = purchaseHistoryUtility.getPrevYearPeriod('2015');
            expect(toReturn).toEqual('2014');
        });

        it('should return itself when given invalid input', function () {
            toReturn = purchaseHistoryUtility.getPrevYearPeriod('foo');
            expect(toReturn).toEqual('foo');
        });
    });

    xdescribe('PurchaseHistoryUtility.periodIdComparator', function () {
        var toReturn;

        it('should compare two month periodId', function () {
            toReturn = purchaseHistoryUtility.periodIdComparator('2014 Jan', '2015 Dec');
            console.info(toReturn);
            expect(toReturn < 0).toBeTruthy();

            toReturn = purchaseHistoryUtility.periodIdComparator('2015 Jan', '2014 Dec');
            expect(toReturn > 0).toBeTruthy();

            toReturn = purchaseHistoryUtility.periodIdComparator('2014 Jan', '2014 Jan');
            expect(toReturn === 0).toBeTruthy();
        });

        it('should compare two quarter periodId', function () {
            toReturn = purchaseHistoryUtility.periodIdComparator('2014 1', '2015 4');
            expect(toReturn < 0).toBeTruthy();

            toReturn = purchaseHistoryUtility.periodIdComparator('2015 1', '2014 4');
            expect(toReturn > 0).toBeTruthy();

            toReturn = purchaseHistoryUtility.periodIdComparator('2015 1', '2015 1');
            expect(toReturn === 0).toBeTruthy();
        });
    });

    describe('PurchaseHistoryUtility.convertPeriodOffsetToDate', function () {
        var toReturn,
            diff,
            periodStartDate = Math.round((new Date()).getTime()/1000); // in second not ms

        // difficult to get difference between two Date object in javascript
        // borrowing momentjs library for difference
        // would like to use 
        var dateDiff = function (dateStart, dateEnd, periodType) {
            var start = moment(dateStart);
            var end = moment(dateEnd);
            return Math.abs(end.diff(start, periodType));
        };

        it('should convert periodOffset of Day', function () {
            toReturn = purchaseHistoryUtility.convertPeriodOffsetToDate('D', periodStartDate, 18);
            diff = dateDiff(toReturn.toDate(), new Date(periodStartDate*1000), 'days');
            expect(diff).toEqual(18);
        });

        it('should convert periodOffset of Month', function () {
            toReturn = purchaseHistoryUtility.convertPeriodOffsetToDate('M', periodStartDate, 4);
            diff = dateDiff(toReturn.toDate(), new Date(periodStartDate*1000), 'months');
            expect(diff).toEqual(4);
        });

        it('should convert periodOffset of Quarter', function () {
            toReturn = purchaseHistoryUtility.convertPeriodOffsetToDate('Q', periodStartDate, 2);
            diff = dateDiff(toReturn.toDate(), new Date(periodStartDate*1000), 'quarters');
            expect(diff).toEqual(2);
        });

        it('should convert periodOffset of Year', function () {
            toReturn = purchaseHistoryUtility.convertPeriodOffsetToDate('Y', periodStartDate, 1);
            diff = dateDiff(toReturn.toDate(), new Date(periodStartDate*1000), 'years');
            expect(diff).toEqual(1);
        });
    });

    describe('PurchaseHistoryUtility.isValidPeriodId', function () {
        var toReturn;
        it('should return true if valid', function () {
            toReturn = purchaseHistoryUtility.isValidPeriodId('M', '2014 Jan');
            expect(toReturn).toBeTruthy();
            
            toReturn = purchaseHistoryUtility.isValidPeriodId('Q', '2014 1');
            expect(toReturn).toBeTruthy();
        });

        it('should return false if invalid', function () {
            toReturn = purchaseHistoryUtility.isValidPeriodId('Q', '2014 Jan');
            expect(toReturn).toBeFalsy();

            toReturn = purchaseHistoryUtility.isValidPeriodId('M', '2014 1');
            expect(toReturn).toBeFalsy();
        });
    });

    describe('PurchaseHistoryUtility.formatDisplayPeriodId', function () {
        var toReturn;
        it('should format month period id', function () {
            toReturn = purchaseHistoryUtility.formatDisplayPeriodId('M', '2014 Jan', false);
            expect(toReturn).toEqual("Jan '14");
        });
        it('should format quarter period id', function () { 
            toReturn = purchaseHistoryUtility.formatDisplayPeriodId('Q', '2014 1', false);
            expect(toReturn).toEqual("Q1 '14");
        });

        it('should return abbreviated year', function () {
            toReturn = purchaseHistoryUtility.formatDisplayPeriodId('M', '2014 Jan', false);
            expect(toReturn).toEqual("Jan '14");
            expect(toReturn).toMatch(/\'/);
        });
        it('should return unabbreviated year', function () {
            toReturn = purchaseHistoryUtility.formatDisplayPeriodId('Q', '2014 1', true);
            expect(toReturn).toEqual('Q1 2014');
            expect(toReturn).not.toMatch(/\'/);
        });
    });

    describe('PurchaseHistoryUtility.getAdjacentPeriodId', function () {
        var toReturn;
        it('should get adjacent quarter for quarter period id', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('Q', '2014 3', true);
            expect(toReturn).toEqual('2014 2');
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('Q', '2014 4', false);
            expect(toReturn).toEqual('2015 1');
        });
        it('should get adjacent month for month period id', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('M', '2014 Jan', true);
            expect(toReturn).toEqual('2013 Dec');
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('M', '2014 Dec', false);
            expect(toReturn).toEqual('2015 Jan');
        });
        it('should get previous period id', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('M', '2014 Mar', true);
            expect(toReturn).toEqual('2014 Feb');
        });
        it('should get next period id', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('Q', '2014 1', false);
            expect(toReturn).toEqual('2014 2');
        });
        it('should get previous period id wrap around to previous', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('M', '2014 Jan', true);
            expect(toReturn).toEqual('2013 Dec');
        });
        it('should get next period id wrap around to next year', function () {
            toReturn = purchaseHistoryUtility.getAdjacentPeriodId('Q', '2014 4', false);
            expect(toReturn).toEqual('2015 1');
        });
    });

    describe('PurchaseHistoryUtility.getNextPeriodId', function () {
        var toReturn;
        it('should call getAdjacentPeriodId with 3rd argument as false', function () {
            spyOn(purchaseHistoryUtility, 'getAdjacentPeriodId');
            toReturn = purchaseHistoryUtility.getNextPeriodId('Q', '2014 4');

            expect(purchaseHistoryUtility.getAdjacentPeriodId).toHaveBeenCalled();
            expect(purchaseHistoryUtility.getAdjacentPeriodId).toHaveBeenCalledWith('Q', '2014 4', false);
        });
    });
    
    describe('PurchaseHistoryUtility.getPrevPeriodId', function () {
        var toReturn;
        it('should call getAdjacentPeriodId with 3rd argument as true', function () {
            spyOn(purchaseHistoryUtility, 'getAdjacentPeriodId');
            toReturn = purchaseHistoryUtility.getPrevPeriodId('M', '2014 Mar');

            expect(purchaseHistoryUtility.getAdjacentPeriodId).toHaveBeenCalled();
            expect(purchaseHistoryUtility.getAdjacentPeriodId).toHaveBeenCalledWith('M', '2014 Mar', true);
        }); 

    });

    describe('PurchaseHistoryUtility.getPeriodRange', function () {
        var toReturn, expected;
        it('should return range of periodId for month', function () {
            expected = ['2014 Nov', '2014 Dec', '2015 Jan', '2015 Feb'];
            toReturn = purchaseHistoryUtility.getPeriodRange('M', '2014 Nov', '2015 Feb');
            
            expect(angular.equals(toReturn, expected)).toBeTruthy();
        });
        it('should return range of periodId for quarter', function () {
            expected = ['2014 2', '2014 3', '2014 4', '2015 1']; 
            toReturn = purchaseHistoryUtility.getPeriodRange('Q', '2014 2', '2015 1');

            expect(angular.equals(toReturn, expected)).toBeTruthy();
        });

        // xit('should return empty range for invalid period', function () {
        //     expected = [];
        //     toReturn = purchaseHistoryUtility.getPeriodRange('Q', '2014 2', '2014 1');
        //     expect(angular.equals(toReturn, expected)).toBeTruthy();
        // });
    });

    xdescribe('PurchaseHistoryUtility.getQuarterFromPeriodId', function () {
        var toReturn;
        it('should return quarter period id from month period id', function () {
            toReturn = purchaseHistoryUtility.getQuarterFromPeriodId('M', '2014 Jan');
            expect(toReturn).toEqual('2014 1');
        });
        it('should return quarter period id from quarter period id', function () {
            toReturn = purchaseHistoryUtility.getQuarterFromPeriodId('Q', '2014 3');
            expect(toReturn).toEqual('2014 3');
        });
    });
    
    xdescribe('PurchaseHistoryUtility.getPrevQuarterPeriod', function () {
        var toReturn;
        it('should return previous quarter period id from month period id', function () {
            toReturn = purchaseHistoryUtility.getPrevQuarterPeriod('M', '2014 Dec');
            expect(toReturn).toEqual('2014 3');
        });
        it('should return previous quarter period id from quarter period id', function () {
            toReturn = purchaseHistoryUtility.getPrevQuarterPeriod('Q', '2014 3');
            expect(toReturn).toEqual('2014 2');
        });
        it('should return previous year if given quarter is Q1', function () {
            toReturn = purchaseHistoryUtility.getPrevQuarterPeriod('Q', '2014 1');
            expect(toReturn).toEqual('2013 4');
            toReturn = purchaseHistoryUtility.getPrevQuarterPeriod('M', '2014 Jan');
            expect(toReturn).toEqual('2013 4');
        });
    });

});