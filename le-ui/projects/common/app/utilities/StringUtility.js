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

    this.SubstituteAllSpecialCharsWithDashes = function (stringToChange) {
        stringToChange = stringToChange.trim();
        return stringToChange.replace(/[^a-zA-Z0-9]/g, '-');
    };

    this.SubstituteAllSpecialCharsWithSpaces = function(stringToChange) {
        stringToChange = stringToChange.trim();
        return stringToChange.replace(/[^a-zA-Z0-9]/g, ' ');
    };

    this.Title = function(stringToChange) {
        return stringToChange.replace(/(^|\s)[a-z]/g, function(c) {
            return c.toUpperCase();
        });
    };
    this.TitleCase = function(string) {
        return string.split(' ').map(w => w[0].toUpperCase() + w.substr(1).toLowerCase()).join(' ') ;
    };
})
.filter('title', function(StringUtility) {
    return function(input) {
        return StringUtility.Title(input);
    };
});
