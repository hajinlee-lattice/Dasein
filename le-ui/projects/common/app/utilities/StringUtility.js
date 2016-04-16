angular.module('mainApp.appCommon.utilities.StringUtility', [])                                                                                                                                                                        
.service('StringUtility', function () {

    this.IsEmptyString = function (stringToCheck) {
        var isEmpty = true;
        if (stringToCheck != null && stringToCheck.trim() !== "") {
            isEmpty = false;
        }
        return isEmpty;
    };
    
    this.AddCommas = function (stringToChange) {
        if (stringToChange == null) {
            return null;
        }
        var parts = stringToChange.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        return parts.join(".");
    };

    this.SubstitueAllSpecialCharsWithDashes = function (stringToChange) {
        return stringToChange.replace(/[^a-zA-Z0-9]/g, '-');
    };
});