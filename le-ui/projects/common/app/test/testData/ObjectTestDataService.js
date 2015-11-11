angular.module('test.testData.ObjectTestDataService', [
    'mainApp.appCommon.utilities.SortUtility'])
.service('ObjectTestDataService', function(SortUtility) {

    this.GetSampleLists = function () {
        var sampleObjects = this.GetSampleObjects();
        return {
            ListA: [
                sampleObjects.ObjectA,
                sampleObjects.ObjectC,
            ],
            ListARev: [
                sampleObjects.ObjectC,
                sampleObjects.ObjectA
            ],
            ListB: [
                sampleObjects.ObjectB,
                sampleObjects.ObjectC
            ],
            ListSameNameDiffQuantity: [
                sampleObjects.ObjectA,
                sampleObjects.ObjectAQ1
            ],
            ListSameNameDiffQuantitySorted: [
                sampleObjects.ObjectAQ1,
                sampleObjects.ObjectA
            ],
            ListSameNameDiffQuantityDiffIsNew: [
                sampleObjects.ObjectAQ5IsNewFalse,
                sampleObjects.ObjectA
            ],
            ListSameNameDiffQuantityDiffIsNewSorted: [
                sampleObjects.ObjectA,
                sampleObjects.ObjectAQ5IsNewFalse
            ],
            ListSameNameSameQuantityDiffIsNew: [
                sampleObjects.ObjectA,
                sampleObjects.ObjectAIsNewFalse
            ],
            ListSameNameSameQuantityDiffIsNewSorted: [
                sampleObjects.ObjectAIsNewFalse,
                sampleObjects.ObjectA
            ],
            ListMoreThanTwo: [
                sampleObjects.ObjectAQ1,
                sampleObjects.ObjectB,
                sampleObjects.ObjectAQ5IsNewFalse,
                sampleObjects.ObjectC,
                sampleObjects.ObjectA,
                sampleObjects.ObjectAIsNewFalse
            ],
            ListMoreThanTwoSorted: [
                sampleObjects.ObjectAQ1,
                sampleObjects.ObjectAIsNewFalse,
                sampleObjects.ObjectA,
                sampleObjects.ObjectAQ5IsNewFalse,
                sampleObjects.ObjectB,
                sampleObjects.ObjectC
            ]
        };
    };

    this.GetSampleSortProperties = function () {
        return {
            OneProp: [{ Name: "Name" }],
            TwoProps: [
                { Name: "Name" },
                { Name: "Quantity" }
            ],
            ThreeProps: [
                { Name: "Name" },
                { Name: "Quantity" },
                { Name: "IsNew" }
            ],
            ThreePropsWithReverseBooleanCompare: [
                { Name: "Name" },
                { Name: "Quantity" },
                { Name: "IsNew", CompareFunction: SortUtility.ReverseBooleanCompare} //true before false
            ],
            ThreePropsWithIsAscendingBoolean: [
                { Name: "Name" },
                { Name: "Quantity" },
                { Name: "IsNew", IsAscending: false} //true before false
            ]
        };
    };
    
    this.GetSampleObjects = function () {
        return {
            ObjectA: {
                Name: "Apple",
                Quantity: 3,
                IsNew: true
            },
            ObjectAIsNewFalse: {
                Name: "Apple",
                Quantity: 3,
                IsNew: false
            },
            ObjectAQ1: {
                Name: "Apple",
                Quantity: 1,
                IsNew: true
            },
            ObjectAQ5IsNewFalse: {
                Name: "Apple",
                Quantity: 5,
                IsNew: false
            },
            ObjectB: {
                Name: "Bard",
                Quantity: 3,
                IsNew: true
            },
            ObjectC: {
                Name: "Cookie",
                Quantity: 4,
                IsNew: false
            }
        };
    };

});