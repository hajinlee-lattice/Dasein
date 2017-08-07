angular.module('mainApp.appCommon.utilities.SortUtility', [])                                                                                                                                                                        
.service('SortUtility', function () {
    
    // Sort a list of objects on multiple object properties
    // Each object in propsToSortOn should be something like:
    // { Name: "Quantity", IsAscending: true, CompareFunction: compareNumeric }
    this.MultiSort = function (objsToSort, propsToSortOn) {
        if (!(objsToSort instanceof Array)) {
            return;
        }

        if (objsToSort.length <= 0) {
            return;
        }

        var firstObj = objsToSort[0];
        var objType = typeof firstObj;
        // Just use default Array.sort for non-objects
        if (objType === "boolean" ||
            objType === "number" ||
            objType === "string") {
            objsToSort.sort();
        } else { // Sort objects using the properties to sort on
            var compareFunction = this.GetCompareFunction(propsToSortOn);
            if (typeof compareFunction === "function") {
                objsToSort.sort(compareFunction);
            }
        }
    };

    this.GetCompareFunction = function (propsToSortOn) {
        var self = this;

        // undefined means we don't know how to sort this
        var compareFunction;

        if (propsToSortOn instanceof Array &&
            propsToSortOn.length > 0) {

            compareFunction = function (a, b) {
                // compare a and b on each property
                // until you find one where they don't match
                for (var i = 0; i < propsToSortOn.length; i++) {
                    var prop = propsToSortOn[i];
                    var propName = prop.Name;
                    var propIsAscending = prop.IsAscending;
                    if (typeof propIsAscending !== "boolean") {
                        propIsAscending = true;
                    }
                    var propCompareFunction = prop.CompareFunction;

                    var compareResult = self.CompareObjectsByProperty(
                        a, b, propName, propIsAscending, propCompareFunction);
                    if (compareResult !== 0) {
                        return compareResult;
                    }
                }
                // a and b matched on all properties
                return 0;
            };

        }
        return compareFunction;
    };

    this.CompareObjectsByProperty = function (a, b, propName, propIsAscending, propCompareFunction) {

        var result = 0;

        if (a == null && b == null) {
            result = 0;
        } else if (a == null) {
            result = -1;
        } else if (b == null) {
            result = 1;
        } else {
            var propA = a[propName];
            var propB = b[propName];

            var compareFunction;
            if (typeof propCompareFunction === "function") {
                compareFunction = propCompareFunction;
            } else {
                compareFunction = this.DefaultCompare;
            }

            result = compareFunction(propA, propB);
        }
        return (propIsAscending ? result : -1 * result);
    };

    this.DefaultCompare = function (a, b) {
        if (a < b) {
            return -1;
        }
        if (a == b) {
            return 0;
        }
        if (a > b) {
            return 1;
        }
        return 0;
    };

    // true comes before false
    this.ReverseBooleanCompare = function (a, b) {
        if ((typeof a !== "boolean") || (typeof b !== "boolean")) {
            return 0;
        }
        if (a === b) {
            return 0;
        }
        if (a === false) { // b is true
            return 1;
        }
        if (a === true) { // b is false
            return -1;
        }
    };
});