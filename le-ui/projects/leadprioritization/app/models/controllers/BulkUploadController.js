angular.module('mainApp.create.csvBulkUpload', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.controller('csvBulkUploadController', [
    '$scope', '$rootScope', '$stateParams', 'ModelService', 'ResourceUtility', '$state', '$q', 'csvImportService', 'csvImportStore', 'RequiredFields',
    function($scope, $rootScope, $stateParams, ModelService, ResourceUtility, $state, $q, csvImportService, csvImportStore, RequiredFields) {
        $scope.showImportError = false;
        $scope.importErrorMsg = "";
        $scope.importing = false;
        $scope.showImportSuccess = false;
        $scope.ResourceUtility = ResourceUtility;
        $scope.accountLeadCheck = false;
        $scope.requiredFields = RequiredFields;

        $scope.uploadFile = function() {
            $scope.showImportError = false;
            $scope.importErrorMsg = "";
            $scope.importing = true;

            var modelId = $stateParams.modelId,
                startTime = new Date();

            csvImportService.Upload({
                file: $scope.csvFile, 
                url: '/pls/scores/fileuploads',
                params: {
                    modelId: modelId,
                    displayName: $scope.csvFileName,
                    compressed: true
                },
                ServiceErrorMethod: 'modal|home.model.jobs',
                progress: function(e) {
                    if (e.total / 1024 > 486000) {
                        xhr.abort();
                        $('div.loader').css({ 'display':'none' });

                        html = 'ERROR: Over file size limit.  File must be below 486MB';
                    } else {
                        var done = e.loaded / 1024,
                            total = e.total / 1024,
                            percent = Math.round(done / total * 100),
                            currentTime = new Date(),
                            seconds = Math.floor((currentTime - startTime) / 1000),
                            minutes = Math.floor(seconds / 60),
                            hours = Math.floor(minutes / 60),
                            speed = done / seconds,
                            seconds = seconds % 60,
                            minutes = minutes % 60,
                            hours = hours % 24,
                            seconds = (seconds < 10 ? '0' + seconds : seconds),
                            minutes = (minutes < 10 ? '0' + minutes : minutes),
                            r = Math.round;

                        if (percent < 100) {
                            var html =  '<div style="display:inline-block;position:relative;width:164px;height:.9em;border:1px solid #aaa;padding:2px;vertical-align:top;">'+
                                        '<div style="width:'+percent+'%;height:100%;background:lightgreen;"></div></div>';
                        } else {
                            var html =  'Processing...';
                        }
                    }

                    $('#file_progress').html(html);
                }
            }).then(function(response) {
                var result = response.Result || {};

                console.log('# Upload Successful:' + response.Success, result.state, response);
                if (response.Success && result.state == "Uploaded" && result.name) {
                    var fileName = result.name;

                    console.log('# CSV Upload Complete', fileName, modelId);
                    //csvImportStore.Set(fileName, metaData);

                    csvImportService.StartTestingSet(modelId, fileName).then(function(result) {
                        console.log('scoring testing set',result);
                        $state.go('home.model.jobs', { 'jobCreationSuccess': true });
                    });
                } else {
                    $('div.loader').css({'display':'none'});

                    var errorCode = result.errorCode || 'LEDP_ERR';
                    var errorMsg  = result.errorMsg || result.ResultErrors || 'Undefined error while uploading file.';

                    html = '<span style="display:block;margin:4em auto 0.5em;max-width:27em;">' + errorMsg + '</span><span style="margin-bottom:3em;display:block;font-size:8pt;color:#666;">' + errorCode + '</span>';
                    $('#file_progress').html(html);
                }
            });

            $('#mainSummaryView .summary>h1').html('Uploading File');
            $('#mainSummaryView .summary').append('<p>Please wait while the CSV file is being uploaded.</p>');

            ShowSpinner('<div><h6 id="file_progress">Compressing...<br>This may take a moment.</h6></div><br><button type="button" id="fileUploadCancelBtn" class="button default-button"><span style="color:black">Cancel Upload</span></button>');

            $('#fileUploadCancelBtn').on('click', $scope.cancelClicked.bind(this));
        };

        $scope.cancelClicked = function() {
            console.log('# Upload Cancelled');
            csvImportStore.Get('cancelXHR', true).abort();
            $state.go('home.model.jobs');
        };
    }
]);