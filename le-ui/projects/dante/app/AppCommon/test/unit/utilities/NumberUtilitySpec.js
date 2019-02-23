'use strict';

describe('NumberUtility Tests', function () {
    
    var numberUtility;

    beforeEach(function () {
        module('common.utilities.number');
        inject(['NumberUtility', 
            function (NumberUtility) {
                numberUtility = NumberUtility;
            }
        ]);
    });

    describe('AbbreviateLargeNumber', function () {
        var toReturn;

        it('AbbreviateLargeNumber should return null if no arguments passed', function() {
            toReturn = numberUtility.AbbreviateLargeNumber( )
            expect(toReturn).toBe(null);
        });

        it('AbbreviateLargeNumber for K to 0 decimal places rounded', function() {
            toReturn = numberUtility.AbbreviateLargeNumber(12345, 0)
            expect(toReturn).toBe('12K');
        });

        it('AbbreviateLargeNumber for M to 2 decimal places rounded', function() {
            toReturn = numberUtility.AbbreviateLargeNumber(888888888, 2)
            expect(toReturn).toBe('888.89M');
        });

        it('AbbreviateLargeNumber for B to default 2 decimal places rounded', function() {
            toReturn = numberUtility.AbbreviateLargeNumber(12345000000);
            expect(toReturn).toBe('12.35B');
        });

        it('AbbreviateLargeNumber for T to 3 decimal places rounded', function() {
            toReturn = numberUtility.AbbreviateLargeNumber(9876543210123, 3)
            expect(toReturn).toBe('9.877T');
        });
    });

    describe('NumberWithCommas', function () {
        var toReturn;

        it('NumberWithCommas should handle number less than 1000', function () {
            toReturn = numberUtility.NumberWithCommas(888);
            expect(toReturn).toBe('888');
        });

        it('NumberWithCommas should handle whole numbers', function () {
            toReturn = numberUtility.NumberWithCommas(123456789.0);
            expect(toReturn).toBe('123,456,789');
        });

        it('NumberWithCommas should handle whole numbers', function () {
            toReturn = numberUtility.NumberWithCommas(54321);
            expect(toReturn).toBe('54,321');
        });

        it('NumberWithCommas should handle decimals number', function () {
            toReturn = numberUtility.NumberWithCommas(1234567.89);
            expect(toReturn).toBe('1,234,567.89');
        });

        it('NumberWithCommas should handle whole numbers', function () {
            toReturn = numberUtility.NumberWithCommas('123456789.0');
            expect(toReturn).toBe('123,456,789.0');
        });

        it('NumberWithCommas should handle string whole numbers', function () {
            toReturn = numberUtility.NumberWithCommas('987654321');
            expect(toReturn).toBe('987,654,321');
        });

        it('NumberWithCommas should handle string decimals number', function () {
            toReturn = numberUtility.NumberWithCommas('1234567.89');
            expect(toReturn).toBe('1,234,567.89');
        });


        it('NumberWithCommas should handle number with greater than 3 decimal points', function () {
            toReturn = numberUtility.NumberWithCommas(9876.54321);
            expect(toReturn).toBe('9,876.54321');
        });

        it('NumberWithCommas should handle string number with greater than 3 decimal points', function () {
            toReturn = numberUtility.NumberWithCommas('9876.54321');
            expect(toReturn).toBe('9,876.54321');
        });
    
        it('NumberWithCommas should handle invalid input', function () {
            toReturn = numberUtility.NumberWithCommas('abcdef');
            expect(toReturn).toBe(null);
        });

        it('NumberWithCommas should handle NaN inputs', function () {
            toReturn = numberUtility.NumberWithCommas('Not A Number!');
            expect(toReturn).toBe(null);
        });
    })

});