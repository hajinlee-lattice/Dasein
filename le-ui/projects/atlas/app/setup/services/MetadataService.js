angular.module('mainApp.setup.services.MetadataService', [
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'common.utilities.browserstorage',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])

.service('MetadataStore', function($q, MetadataService) {
    var MetadataStore = this;
    this.metadataMap = {};

    this.GetMetadataForModel = function(modelId) {
        var deferred = $q.defer(),
        metadata = this.metadataMap[modelId];
        if (typeof metadata == 'object') {
            deferred.resolve(metadata);
        } else {
            MetadataService.GetMetadataForModelId(modelId).then(function(result) {
                if (result != null && result.Success === true) {
                    MetadataStore.SetMetadataForModel(modelId, result.ResultObj);
                    deferred.resolve(result.ResultObj);
                } else {
                    deferred.resolve(result);
                }
            });
        }

        return deferred.promise;
    };

    this.SetMetadataForModel = function(modelId, metadata) {
        this.metadataMap[modelId] = metadata;
    };
})
.service('MetadataService', function ($http, $q, _, BrowserStorageUtility, RightsUtility, ResourceUtility, StringUtility, SessionService) {

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
        return (dataItem != null && dataItem.Tags != null && dataItem.Tags.toLowerCase() === "internal");
    };

    this.IsLatticeAttribute = function (dataItem) {
        if (dataItem == null) {
            return false;
        }
        if (dataItem.Tags == null) {
            return false;
        }
        return dataItem.Tags.toLowerCase() === "external" || dataItem.Tags.toLowerCase() == "externaltransform";
    };

    this.GetOptions = function () {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/vdbmetadata/options?' + new Date().getTime(),
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function (data) {
            var result = {
                Success: true,
                ResultObj: data,
                ResultErrors: null
            };
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_OPTIONS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetMetadataForModel = function (modelId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/modelsummaries/metadata/' + modelId,
            headers: {
                'Content-Type': "application/json"
            }
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data !== null) {
                result.Success = true;
                result.ResultObj = data;
            } else {
                result.ResultErrors = ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_FIELDS_ERROR');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('SETUP_MANAGE_FIELDS_GET_FIELDS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.UpdateAndCloneFields = function (dedupType, includePersonalEmailDomains, useLatticeAttributes, enableTransformations, modelName,
                                        modelDisplayName, notesContent, originalModelSummaryId, fields, dataRules) {
        var deferred = $q.defer();

        var cloneParams = {
            name : modelName,
            displayName: modelDisplayName,
            notesContent: notesContent,
            description: 'cloned from model: ' + originalModelSummaryId,
            attributes: fields || [],
            sourceModelSummaryId: originalModelSummaryId,
            deduplicationType: dedupType,
            excludePublicDomain: includePersonalEmailDomains ? false : true,
            //excludeDataCloudAttrs ??
            excludePropDataAttributes: useLatticeAttributes ? false : true,
            //transformationGroup ??
            enableTransformations: enableTransformations ? true : false,
            dataRules: dataRules
        };

        $http({
            method: 'POST',
            url: '/pls/models/' + originalModelSummaryId + '/clone',
            headers: {
                'Content-Type': "application/json"
            },
            data: cloneParams
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: null
            };
            if (data !== null) {
                result.Success = true;
                result.ResultObj = data;
            } else {
                result.ResultErrors = ResourceUtility.getString('UPDATE_FIELDS_ERROR_MESSAGE');
            }
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('UPDATE_FIELDS_ERROR_MESSAGE')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

});
