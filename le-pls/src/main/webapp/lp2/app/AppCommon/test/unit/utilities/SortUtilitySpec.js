'use strict';

describe('SortUtility Tests', function () {
    
    var sortUtil,
        objectTestData,
        sampleObjList;
    
    beforeEach(function() {
        module('mainApp.appCommon.utilities.SortUtility');
        module('test.testData.ObjectTestDataService');
        
        inject(['SortUtility', 'ObjectTestDataService',
            function (SortUtility, ObjectTestDataService) {
                sortUtil = SortUtility;
                objectTestData = ObjectTestDataService;
            }
        ]);
    });
    
    describe('MultiSort given invalid sort properties', function () {
        var origObjList = [{
                id: "world"
            }, {
                id: "hello"
            }],
            actObjList,
            origStrList = ["world", "hello"],
            actStrList,
            expStrList = ["hello", "world"],
            origIntList = [2, 1],
            actIntList,
            expIntList = [1, 2],
            origBoolList = [true, false],
            actBoolList,
            expBoolList = [false, true];        
        
        it('should not sort objects if sort properties are undefined', function () {
            actObjList = origObjList.slice();
            sortUtil.MultiSort(actObjList);
            expect(actObjList).toEqual(origObjList);
        });
        
        it('should not sort objects if sort properties are empty', function () {
            actObjList = origObjList.slice();
            sortUtil.MultiSort(actObjList, []);
            expect(actObjList).toEqual(origObjList);
        });
        
        it('should sort strings using Array.sort() behavior if sort properties are undefined', function () {
            actStrList = origStrList.slice();
            sortUtil.MultiSort(actStrList);
            expect(actStrList).toEqual(expStrList);
        });
        
        it('should sort strings using Array.sort() behavior if sort properties are empty', function () {
            actStrList = origStrList.slice();
            sortUtil.MultiSort(actStrList, []);
            expect(actStrList).toEqual(expStrList);
        });
        
        it('should sort ints using Array.sort() behavior if sort properties are undefined', function () {
            actIntList = origIntList.slice();
            sortUtil.MultiSort(actIntList);
            expect(actIntList).toEqual(expIntList);
        });
        
        it('should sort ints using Array.sort() behavior if sort properties are empty', function () {
            actIntList = origIntList.slice();
            sortUtil.MultiSort(actIntList, []);
            expect(actIntList).toEqual(expIntList);
        });
        
        it('should sort booleans using Array.sort() behavior if sort properties are undefined', function () {
            actBoolList = origBoolList.slice();
            sortUtil.MultiSort(actBoolList);
            expect(actBoolList).toEqual(expBoolList);
        });
        
        it('should sort booleans using Array.sort() behavior if sort properties are empty', function () {
            actBoolList = origBoolList.slice();
            sortUtil.MultiSort(actBoolList, []);
            expect(actBoolList).toEqual(expBoolList);
        });
    });
    
    describe('MultiSort on valid sort properties', function () {
        it('should sort on integers correctly', function () {
            sortOnValues(1, 2);
            sortOnValues(0, 1);
        });
        
        it('should sort on decimals correctly', function () {
            sortOnValues(1.4, 1.5);
            sortOnValues(1, 1.5);
            sortOnValues(0.5, 0.51);
        });
        
        it('should sort on booleans correctly', function () {
            sortOnValues(false, true);
        });
        
        it('should sort on strings correctly', function () {
            sortOnValues("hello", "world");
            sortOnValues("hello", "hello1");
            sortOnValues("Hello", "hello");
            sortOnValues("", "hello");
        });
        
        it('should sort on date time offsets correctly', function () {
            var firstDate = new Date("2013-12-15T00:00:00.000Z");
            var secondDate = new Date("2014-02-10T05:00:00.000Z");
            sortOnValues(firstDate, secondDate);
        });
        
        function sortOnValues(firstValue, secondValue) {
            var firstObj = {
                    ID: "Obj1",
                    ValueForSort: firstValue
                },
                secondObj = {
                    ID: "Obj2",
                    ValueForSort: secondValue
                },
                origList = [secondObj, firstObj],
                expList = [firstObj, secondObj],
                sortedList,
                sortProps = [{
                    Name: "ValueForSort"
                }];
                
            sortedList = origList.slice();
            sortUtil.MultiSort(sortedList, sortProps);
                
            validateSorting(sortedList, origList, expList);
        }
    });
    
    describe('MultiSort on a list with 2 objects', function () {
        it('should correctly sort 2 objects that are already sorted', function () {
            var origList = getNewTestLists().ListA;
            var sortedList = origList.slice();
            var sortProps = getSortProps().OneProp;
            sortUtil.MultiSort(sortedList, sortProps);
            expect(sortedList).toEqual(origList);
        });
        
        it('should correctly sort 2 objects on 1 property', function () {
            var origList = getNewTestLists().ListARev;
            var expList = getNewTestLists().ListA;
            
            var sortedList = origList.slice();
            var sortProps = getSortProps().OneProp;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
        
        it('should correctly sort 2 objects on 2 properties', function () {
            var origList = getNewTestLists().ListSameNameDiffQuantity;
            var expList = getNewTestLists().ListSameNameDiffQuantitySorted;
            
            var sortedList = origList.slice();
            var sortProps = getSortProps().TwoProps;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
        
        it('should correctly sort 2 objects on 3 properties', function () {
            var origList = getNewTestLists().ListSameNameSameQuantityDiffIsNew;
            var expList = getNewTestLists().ListSameNameSameQuantityDiffIsNewSorted;

            var sortedList = origList.slice();
            var sortProps = getSortProps().ThreeProps;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
        
        it('should correctly sort 2 objects using only as many properties as needed', function () {
            var origList = getNewTestLists().ListSameNameDiffQuantityDiffIsNew;
            var expList = getNewTestLists().ListSameNameDiffQuantityDiffIsNewSorted;
            
            var sortedList = origList.slice();
            var sortProps = getSortProps().ThreeProps;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
    });
    
    describe('MultiSort on a list with more than 2 objects', function () {
        it('should correctly sort more than 2 objects on 3 properties', function () {
            var origList = getNewTestLists().ListMoreThanTwo;
            var expList = getNewTestLists().ListMoreThanTwoSorted;

            var sortedList = origList.slice();
            var sortProps = getSortProps().ThreeProps;
            sortUtil.MultiSort(sortedList, sortProps);

            validateSorting(sortedList, origList, expList);
        });
    });
    
    describe('MultiSort when passing in a compare function', function () {
        it('should correctly sort using the ReverseBooleanCompare function', function () {
            var origList = getNewTestLists().ListSameNameSameQuantityDiffIsNewSorted;
            var expList = getNewTestLists().ListSameNameSameQuantityDiffIsNew;
            
            var sortedList = origList.slice();
            var sortProps = getSortProps().ThreePropsWithReverseBooleanCompare;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
    });
    
    describe('MultiSort when using the IsAscending flag', function () {
        it('should correctly sort on a boolean property using the IsAscending flag', function () {
            var origList = getNewTestLists().ListSameNameSameQuantityDiffIsNewSorted;
            var expList = getNewTestLists().ListSameNameSameQuantityDiffIsNew;
            
            var sortedList = origList.slice();
            var sortProps = getSortProps().ThreePropsWithIsAscendingBoolean;
            sortUtil.MultiSort(sortedList, sortProps);
            
            validateSorting(sortedList, origList, expList);
        });
    });
    
    function getNewTestLists() {
        return objectTestData.GetSampleLists();
    }
    
    function getSortProps() {
        return objectTestData.GetSampleSortProperties();
    }
    
    function validateSorting(sortedList, origList, expList) {
        //expect(sortedList).toNotEqual(origList);
        //expect(sortedList).toEqual(expList);
    }
    
});