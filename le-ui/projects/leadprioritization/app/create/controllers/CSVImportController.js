angular.module('mainApp.create.csvImport', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.service('csvImportStore', function() {
    this.files = {};

    this.Get = function(name) {
        return this.files[name];
    }

    this.Set = function(name, data) {
        this.files[name] = data;
    }
})
.service('csvImportService', function($q, $http, ModelService, ResourceUtility) {
    this.Upload = function(file, fileType) {
        var deferred = $q.defer(),
            formData = new FormData();
        
        formData.append('file', file);

        $http.post('/pls/fileuploads/unnamed?schema=' + fileType, formData, {
            transformRequest: angular.identity,
            headers: {
                'Content-Type': undefined
            }
        })
        .success(function(data, status, headers, config) {
            console.log('UPLOAD SUCCESS', fileType, status, data);
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                    Result: null
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: data.Errors,
                    Result: data.Result
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            console.log('UPLOAD ERROR', fileType, status, data);
            var result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_IMPORT_GENERAL_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.GetUnknownColumns = function(csvFile) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/fileuploads/' + csvFile.name + '/metadata/unknown',
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('VALIDATION SUCCESS', status, data);
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                    Result: null
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: data.Errors,
                    Result: data.Result
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            console.log('VALIDATION ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.SetUnknownColumns = function(csvMetaData, csvUnknownColumns) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/fileuploads/' + csvMetaData.name + '/metadata/unknown',
            data: csvUnknownColumns,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('csvUnknownColumns POST SUCCESS', status, data);
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                    Result: null
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: data.Errors,
                    Result: data.Result
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            console.log('csvUnknownColumns POST ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.StartModeling = function(csvMetaData) {
        var deferred = $q.defer();
console.log('StartModeling', csvMetaData)
        $http({
            method: 'POST',
            url: '/pls/models/' + csvMetaData.modelName,
            data: {
                'description': 'Self-service Model',
                'filename': csvMetaData.name,
                'name': csvMetaData.modelName
            },
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('MODELING POST SUCCESS', status, data);
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                    Result: null
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: data.Errors,
                    Result: data.Result
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            console.log('MODELING POST ERROR', status, data);
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };
})
.directive('csvUploader', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.csvUploader);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}])
.controller('csvImportController', [
'$scope', '$rootScope', 'ModelService', 'ResourceUtility', 'csvImportService', 'csvImportStore', '$state', 
function($scope, $rootScope, ModelService, ResourceUtility, csvImportService, csvImportStore, $state) {
    $scope.showImportError = false;
    $scope.importErrorMsg = "";
    $scope.importing = false;
    $scope.showImportSuccess = false;
    $scope.ResourceUtility = ResourceUtility;
    $scope.accountLeadCheck = false;

    $scope.uploadFile = function(){
        $scope.showImportError = false;
        $scope.importErrorMsg = "";
        $scope.importing = true;

        var fileType = $scope.accountLeadCheck ? 'SalesforceLead' : 'SalesforceAccount';

        csvImportService.Upload($scope.csvFile, fileType).then(function(result) {
            if (result.Success && result.Result) {
                var fileName = result.Result.name,
                    metaData = result.Result,
                    modelName = $scope.modelName;

                console.log('#CSV Upload Complete', fileName, modelName, metaData);
                metaData.modelName = modelName;

                csvImportStore.Set(fileName, metaData);

                $state.go('models.fields', { csvFileName: fileName })
            }
        });

        $('#mainContentView').html(
            '<section id="main-content" class="container">' +
            '<div class="row twelve columns"><div class="loader"></div>' +
            '<h2 class="text-center">Uploading File...</h2></div></section>');
    };

    $scope.cancelClicked = function(){
        console.log('okClicked');
    };
}]);