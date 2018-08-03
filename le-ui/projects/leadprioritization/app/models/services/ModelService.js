angular.module('mainApp.models.services.ModelService', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.services.SessionService',
    'mainApp.appCommon.services.ModelSummaryValidationService',
    'common.exceptions'
])
.service('ModelStore', function($q, ModelService, $timeout, ServiceErrorUtility) {
    var ModelStore = this;

    this.models = [];
    this.modelsMap = {};
    this.stale = true;

    this.widgetConfig = {
        "ScoreProperty": "RocScore",
        "NameProperty": "DisplayName",
        "StatusProperty": "Status",
        "TypeProperty": "SourceSchemaInterpretation",
        "ExternalAttributesProperty": "ExternalAttributes",
        "InternalAttributesProperty": "InternalAttributes",
        "CreatedDateProperty": "ConstructionTime",
        "TotalLeadsProperty": "TotalLeads",
        "TestSetProperty": "TestingLeads",
        "TrainingSetProperty": "TrainingLeads",
        "TotalSuccessEventsProperty": "TotalConversions",
        "ConversionRateProperty": "ConversionRate",
        "LeadSourceProperty": "LeadSource",
        "OpportunityProperty": "Opportunity"
    };

    // checks if items matching args exists, performs XHR to fetch if they don't
    this.getModel = function(modelId) {
        var deferred = $q.defer(),
            model = this.modelsMap[modelId];

        if (typeof model == 'object') {
            deferred.resolve(model);
        } else {
            ModelService.GetModelById(modelId).then(function(result) {
                if (result != null && result.success === true) {
                    ModelStore.addModel(modelId, result.resultObj);
                    deferred.resolve(result.resultObj);
                } else {
                    deferred.reject(result.resultObj);
                }
            });
        }

        return deferred.promise;
    };

    this.getModels = function(use_cache) {
        var deferred = $q.defer();

        if (use_cache && ModelStore.models.length > 0) {
            deferred.resolve(ModelStore.models);
        } else if (this.stale) {
            ModelService.GetAllModels().then(function(response) {
                var models = response.resultObj;

                if (!models) {
                    ServiceErrorUtility.process(response);
                    return;
                }

                ModelStore.models.length = 0;

                models.forEach(function(model, index) {
                    ModelStore.models.push(model);
                });

                ModelStore.stale = false;

                $timeout(function() {
                    ModelStore.stale = true;
                }, 500);

                deferred.resolve(models);
            });
        } else {
            deferred.resolve(ModelStore.models);
        }

        return deferred.promise;
    };

    this.addModel = function(modelId, model) {
        this.modelsMap[modelId] = model;
    };

    this.removeModel = function(modelId) {
        delete this.modelsMap[modelId];
    };
})
.service('ModelService', function ($http, $q, _, ResourceUtility, StringUtility, DateTimeFormatUtility, SessionService, ModelSummaryValidationService, ModelServiceUtility) {

    this.GetAllModels = function (isValidOnly) {
            var deferred = $q.defer();
            var result;
            var request;
            request = {
                method: 'GET',
                url: '/pls/modelsummaries/',
                headers: {
                    "Content-Type": "application/json"
                }
            };
            if (isValidOnly === false) {
                request.url += '?selection=all';
            }
            $http(request)
            .success(function(data, status, headers, config) {
                if (data == null) {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                    deferred.resolve(result);
                } else {
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };

                    data = _.sortBy(data, 'ConstructionTime').reverse();
                    // sync with front-end json structure
                    result.resultObj = _.map(data, function(rawObj) {

                        return {
                            Id                          : rawObj.Id,
                            DisplayName                 : rawObj.DisplayName == null || rawObj.DisplayName == "" ? rawObj.Name : rawObj.DisplayName,
                            CreatedDate                 : DateTimeFormatUtility.FormatShortDate(rawObj.ConstructionTime),
                            ModelFileType               : rawObj.ModelType,
                            Status                      : rawObj.Status,
                            Incomplete                  : rawObj.Incomplete,
                            ModelType                   : rawObj.SourceSchemaInterpretation,
                            HasBucketMetadata           : rawObj.HasBucketMetadata,
                            Uploaded                    : rawObj.Uploaded,
                            ConflictWithOptionalRules   : ModelServiceUtility.getModelSummaryProvenanceProperties(rawObj.ModelSummaryProvenanceProperties, 'ConflictWithOptionalRules')
                        };
                    });

                }
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                SessionService.HandleResponseErrors(data, status);
                if (status == 403) {
                    // Users without the privilege of reading models see empty list instead of an error
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };
                //} else if (data.errorMsg.indexOf("No tenant found")) {
                //    result = {
                //        success: false,
                //        resultObj: null,
                //        resultErrors: "NO TENANT FOUND"
                //    };
                } else {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                }
                deferred.resolve(result);
            });

        return deferred.promise;
    };

    this.GetAllModelsForTenant = function (tenantId) {
        var deferred = $q.defer();
        var result;
        $http({
            method: 'GET',
            url: '/pls/modelsummaries/tenant/'+ tenantId,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data === true || data === 'true') {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.updateAsDeletedModel = function (modelId) {
        var deferred = $q.defer();
        var result;
        $http({
            method: 'PUT',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            },
            data: angular.toJson ({ Status: "UpdateAsDeleted" })
        })
        .success(function(data, status, headers, config) {
            if (data === true || data === 'true') {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_DELETE_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18021') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACTIVE_MODEL_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.updateAsActiveModel = function (modelId) {
        var deferred = $q.defer();
        var result;
        $http({
            method: 'PUT',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            },
            data: angular.toJson ({ Status: "UpdateAsActive" })
        })
        .success(function(data, status, headers, config) {
            if (data === true || data === 'true') {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_DELETE_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18021') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACTIVE_MODEL_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.updateAsInactiveModel = function (modelId) {
        var deferred = $q.defer();
        var result;
        $http({
            method: 'PUT',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            },
            data: angular.toJson ({ Status: "UpdateAsInactive" })
        })
        .success(function(data, status, headers, config) {
            if (data === true || data === 'true') {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_DELETE_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18021') result.ResultErrors = ResourceUtility.getString('MODEL_DELETE_ACTIVE_MODEL_ERROR');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.undoDeletedModel = function (modelId) {
        var deferred = $q.defer();
        var result;
        $http({
            method: 'PUT',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            },
            data: angular.toJson ({ Status: "UpdateAsInactive" })
        })
        .success(function(data, status, headers, config) {
            if (data === true || data === 'true') {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_LIST_UNDO_DELETE_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('MODEL_LIST_UNTO_DELETE_ACCESS_DENIED');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetModelById = function (modelId) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json",
                'ErrorDisplayMethod': 'modal',
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };

                var modelSummary = "";
                if (!StringUtility.IsEmptyString(data.Details.Payload)) {
                    modelSummary = JSON.parse(data.Details.Payload);
                }
                modelSummary.ModelDetails.Status = data.Status;
                modelSummary.ModelDetails.DisplayName = data.DisplayName;
                modelSummary.ModelDetails.Uploaded = data.Uploaded;
                modelSummary.ModelDetails.PivotArtifactPath = data.PivotArtifactPath;
                modelSummary.ModelDetails.SourceSchemaInterpretation = data.SourceSchemaInterpretation;
                modelSummary.ModelDetails.TrainingFileExist = data.TrainingFileExist;
                modelSummary.ModelDetails.ModelSummaryProvenanceProperties = data.ModelSummaryProvenanceProperties;
                modelSummary.ModelDetails.ConflictWithOptionalRules =ModelServiceUtility.getModelSummaryProvenanceProperties(data.ModelSummaryProvenanceProperties, 'ConflictWithOptionalRules');

                // sync with front-end json structure
                result.resultObj = modelSummary;
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.ChangeModelDisplayName = function (modelId, displayName) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'PUT',
            url: '/pls/modelsummaries/'+ modelId,
            data: { DisplayName: displayName },
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    Success: true,
                    ResultErrors: null
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_TILE_EDIT_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('CHANGE_MODEL_NAME_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18014') result.ResultErrors = ResourceUtility.getString('CHANGE_MODEL_NAME_CONFLICT');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    // This will take sample lead data from the server and transform it for display purposes
    this.FormatLeadSampleData = function (sampleLeads) {
        if (sampleLeads == null) {
            return null;
        }
        var toReturn = [];
        for (var i = 0; i < sampleLeads.length; i++) {
            var lead = sampleLeads[i];

            var leadToDisplay = {
                Company: lead.Company,
                Contact: lead.Contact || (lead.FirstName + " " + lead.LastName),
                Website: lead.Website,
                Converted: lead.Converted,
                Score: lead.Score
            };
            toReturn.push(leadToDisplay);
        }

        return toReturn;
    };


    this.validateModelName = function(name) {
        var result = {
            valid: false,
            errMsg: null
        };
        if (name.replace(/ /g,'') === "") {
            result.errMsg = ResourceUtility.getString('MODEL_TILE_EDIT_TITLE_EMPTY_ERROR');
            return result;
        }
        if (name.length > 50) {
            result.errMsg = ResourceUtility.getString('MODEL_TILE_EDIT_TITLE_LONG_ERROR');
            return result;
        }
        result.valid = true;
        return result;
    };

    this.uploadRawModelJSON = function(json) {
        var deferred = $q.defer();

        var data = {
            Tenant: {
                Identifier: "FAKE_TENANT",
                DisplayName: "Fake Tenant"
            },
            RawFile: json
        };

        var result = {
            Success: false,
            ResultErrors: ''
        };

        var errors = ModelSummaryValidationService.ValidateModelSummary(JSON.parse(json));
        if (errors.length > 0) {
            result.ResultErrors = ResourceUtility.getString('MODEL_IMPORT_ERROR_TITLE') + " " + errors.join(", ") + ".";
            deferred.resolve(result);
            return deferred.promise;
        }

        $http({
            method: 'POST',
            url: '/pls/modelsummaries?raw=true',
            data: data,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: null
                };
            }
            deferred.resolve(result);

        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_IMPORT_GENERAL_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;

    };

    this.GetAllSegments = function (modelList) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/segments/',
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };

                var modelDict = {};
                if (modelList != null && modelList.length > 0) {
                    for (var i=0;i<modelList.length;i++) {
                        modelDict[modelList[i].Id] = modelList[i].DisplayName;
                    }
                }

                var segmentList = data;
                if (segmentList != null && segmentList.length > 0 &&
                    Object.keys(modelDict).length > 0) {
                    for (var x=0;x<segmentList.length;x++) {
                        var segment = segmentList[x];
                        if (!StringUtility.IsEmptyString(segment.ModelId)) {
                            segment.ModelName = modelDict[segment.ModelId];
                        } else {
                            segment.ModelId = "FAKE_MODEL";
                        }
                    }
                }

                result.resultObj = segmentList;
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.AddSegment = function (segment) {
        var deferred = $q.defer();
        var result;
        if (segment == null || StringUtility.IsEmptyString(segment.Name)) {
            return null;
        }

        if (segment.ModelId == "FAKE_MODEL") {
            segment.ModelId = null;
        }

        $http({
            method: 'POST',
            url: '/pls/segments/',
            headers: {
                "Content-Type": "application/json"
            },

            data: segment
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: data.Success,
                    resultObj: {},
                    resultErrors: null
                };
                if (result.success === false) {
                    result.resultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                }

            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.DeleteSegment = function (segmentName) {
        var deferred = $q.defer();
        var result;
        if (StringUtility.IsEmptyString(segmentName)) {
            deferred.resolve(result);
            return deferred.promise;
        }

        $http({
            method: 'DELETE',
            url: '/pls/segments/' + segmentName,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: data.Success,
                    resultObj: {},
                    resultErrors: null
                };
                if (result.success === false) {
                    result.resultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                }

            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.UpdateSegments = function (segments) {
        var deferred = $q.defer();
        var result;
        if (segments == null) {
            deferred.resolve(result);
            return deferred.promise;
        }

        for (var i = 0; i < segments.length; i++) {
            if (segments[i].ModelId == "FAKE_MODEL") {
                segments[i].ModelId = "";
            }
        }

        $http({
            method: 'POST',
            url: '/pls/segments/list',
            headers: {
                "Content-Type": "application/json"
            },
            data: segments
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: data.Success,
                    resultObj: {},
                    resultErrors: null
                };
                if (result.success === false) {
                    result.resultErrors = data.Errors[0];
                }

            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.UpdateSegment = function (segment) {
        var deferred = $q.defer();
        var result;
        if (segment == null) {
            deferred.resolve(result);
            return deferred.promise;
        }


        if (segment.ModelId == "FAKE_MODEL") {
            segment.ModelId = null;
        }

        $http({
            method: 'PUT',
            url: '/pls/segments/' + segment.Name,
            headers: {
                "Content-Type": "application/json"
            },
            data: segment
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: data.Success,
                    resultObj: {},
                    resultErrors: null
                };
                if (result.success === false) {
                    result.resultErrors = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR');
                }

            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetModelAlertsByModelId = function (modelId) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/modelsummaries/alerts/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: true,
                    resultObj: data,
                    resultErrors: null
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };
    this.CopyModel = function (modelName, tenantId) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'POST',
            url: '/pls/models/copymodel/' + modelName,
            params: {
                targetTenantId: tenantId
            },
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: true,
                    resultObj: data,
                    resultErrors: null
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };
})
.service('ModelServiceUtility', function () {
    this.getModelSummaryProvenanceProperties = function (properties, target) {
        for (var i = 0; i < properties.length; i++) {
            var prop = properties[i].ModelSummaryProvenanceProperty;
            if (prop.option === target) {
                switch (prop.value) {
                    case "true":
                        return true;
                    case "false":
                        return false;
                    default:
                        return prop.value;
                }
            }
        }

        return null;
    };
});
