angular.module('mainApp.appCommon.services.ManageFieldsService', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ManageFieldsService', function (StringUtility, ResourceUtility) {

    this.GetOptionsForSelects = function (fields) {
        var allSources = [];
        var allCategories = [];
        var allOptions = [];
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i];
            if (!StringUtility.IsEmptyString(field.SourceToDisplay) && allSources.indexOf(field.SourceToDisplay) < 0) {
                allSources.push(field.SourceToDisplay);
            }
            if (!StringUtility.IsEmptyString(field.Category) && allCategories.indexOf(field.Category) < 0) {
                allCategories.push(field.Category);
            }

            var exist = false;
            for (var j = 0; j < allOptions.length; j++) {
                if (allOptions[j][0] == field.SourceToDisplay && allOptions[j][1] == field.Category) {
                    exist = true;
                    break;
                }
            }
            if (!exist) {
                allOptions.push([field.SourceToDisplay, field.Category]);
            }
        }

        var obj = {};
        obj.sourcesToSelect = allSources.sort();
        obj.categoriesToSelect = allCategories.sort();
        obj.allOptions = allOptions;
        return obj;
    };

    this.CategoryEditable = function (dataItem) {
        if (dataItem != null) {
            if (dataItem.Tags == null || dataItem.Tags === '') {
                return true;
            } else {
                return dataItem.Tags.toLowerCase() === 'internal';
            }
        } else {
            return false;
        }
    };

});