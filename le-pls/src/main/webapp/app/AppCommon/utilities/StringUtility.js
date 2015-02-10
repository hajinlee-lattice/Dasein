angular.module('mainApp.appCommon.utilities.StringUtility', [])                                                                                                                                                                        
.service('StringUtility', function () {

    this.IsEmptyString = function (stringToCheck) {
        var isEmpty = true;
        if (stringToCheck != null && stringToCheck.trim() !== "") {
            isEmpty = false;
        }
        return isEmpty;
    };
    
});