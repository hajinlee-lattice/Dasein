angular.module('mainApp.setup.controllers.ImportAndEnrichDataController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.setup.utilities.SetupUtility',
    'mainApp.setup.controllers.CancelDeploymentStepModel',
    'mainApp.setup.controllers.ClearDeploymentModel',
    'mainApp.setup.services.TenantDeploymentService'
])

.controller('ImportAndEnrichDataController', function ($scope, $rootScope, $http, $filter, $timeout, ResourceUtility, DateTimeFormatUtility, BrowserStorageUtility, NavUtility, SessionService, FeatureFlagService, SetupUtility, CancelDeploymentStepModel, ClearDeploymentModel, TenantDeploymentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.SetupUtility = SetupUtility;
    if (BrowserStorageUtility.getClientSession() == null) { return; }

    $scope.loading = true;
    initImportDataArea();

    $scope.ofText = ResourceUtility.getString('SETUP_CONJUNCTION_OF');
    $scope.leadsText = ResourceUtility.getString('SETUP_LEADS');

    function initImportDataArea() {
        var deployment = $scope.deployment;
        if (deployment.Step === SetupUtility.STEP_IMPORT_DATA) {
            if (deployment.Status === SetupUtility.STATUS_IN_PROGRESS) {
                $scope.showImportProgress = true;
                startGetObjectsTimer(deployment.Step, deployment.Status);
            } else if (deployment.Status === SetupUtility.STATUS_SUCCESS) {
                handleImportComplete();
            } else if (deployment.Status === SetupUtility.STATUS_FAIL) {
                $scope.showImportError = true;
                getObjects(deployment.Step, deployment.Status, false);
            }
        } else {
            handleImportComplete();
        }
    }

    function handleImportComplete() {
        $scope.showImportComplete = true;
        TenantDeploymentService.GetImportSfdcDataCompleteTime().then(function (result) {
            if (result.Success === true) {
                try {
                    $scope.importCompleteTime = DateTimeFormatUtility.FormatDateTime(result.ResultObj, "mm/dd/yy hh:MM:ss TT");
                } catch (e) {
                    $scope.importCompleteTime = ResourceUtility.getString('SETUP_FORMAT_DATE_ERROR');
                }
            } else {
                $scope.getImportCompleteTimeError = result.ResultErrors;
            }
            initEnrichDataArea();
        });
    }

    function initEnrichDataArea() {
        var deployment = $scope.deployment;
        if (deployment.Step === SetupUtility.STEP_IMPORT_DATA) {
            $scope.showEnrichProgress = true;
            startGetObjectsTimer(SetupUtility.STEP_ENRICH_DATA, SetupUtility.STATUS_IN_PROGRESS);
        } else if (deployment.Step === SetupUtility.STEP_ENRICH_DATA) {
            if (deployment.Status === SetupUtility.STATUS_IN_PROGRESS) {
                $scope.showEnrichProgress = true;
                startGetObjectsTimer(deployment.Step, deployment.Status);
            } else if (deployment.Status === SetupUtility.STATUS_SUCCESS) {
                handleEnrichComplete();
            } else if (deployment.Status === SetupUtility.STATUS_FAIL) {
                $scope.showEnrichError = true;
                getDataCompleted();
            }
        } else {
            handleEnrichComplete();
        }
    }

    function handleEnrichComplete() {
        $scope.showEnrichComplete = true;
        TenantDeploymentService.GetEnrichDataCompleteTime().then(function (result) {
            if (result.Success === true) {
                try {
                    $scope.enrichCompleteTime = DateTimeFormatUtility.FormatDateTime(result.ResultObj, "mm/dd/yy hh:MM:ss TT");
                } catch (e) {
                    $scope.enrichCompleteTime = ResourceUtility.getString('SETUP_FORMAT_DATE_ERROR');
                }
            } else {
                $scope.getEnrichCompleteTimeError = result.ResultErrors;
            }
            initValidationDataArea();
        });
    }

    function initValidationDataArea() {
        var deployment = $scope.deployment;
        if (deployment.Step === SetupUtility.STEP_VALIDATE_METADATA) {
            if (deployment.Status === SetupUtility.STATUS_SUCCESS) {
                $scope.showValidationComplete = true;
            } else if (deployment.Status === SetupUtility.STATUS_WARNING) {
                $scope.showValidationError = true;
                $scope.missingData = true;
            } else if (deployment.Status === SetupUtility.STATUS_FAIL) {
                $scope.showValidationError = true;
                $scope.missingData = false;
            }
        }
        getDataCompleted();
    }

    function startGetObjectsTimer(step, status) {
        clearTimer(step);
        getObjects(step, status, false);
        var id = window.setInterval(function () { getObjects(step, status, true); }, 5000);
        setTimer(step, id);
    }

    function getObjects(step, status, inTimer) {
        if (!needGetObjects(step, inTimer)) {
            return;
        }

        TenantDeploymentService.GetObjects(step, status).then(function (result) {
            if (result.Success === true) {
                if (result.ResultObj.LaunchStatus === SetupUtility.STATUS_SUCCESS) {
                    clearTimer(step);
                    $scope.gettingObjects = false;
                    $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
                    return;
                }

                if (result.ResultObj != null) {
                    if (step === SetupUtility.STEP_ENRICH_DATA) {
                        $scope.enrichProgress = getEnrichProgress(result.ResultObj.Jobs);
                    } else {
                        $scope.tables = getTables(result.ResultObj.Jobs);
                        $scope.getObjectsError = null;
                    }
                } else if (inTimer) {
                    clearTimer(step);
                }
            } else {
                if (inTimer) {
                    clearTimer(step);
                }
                if (step === SetupUtility.STEP_ENRICH_DATA) {
                    $scope.enrichProgress = result.ResultErrors;
                } else {
                    $scope.getObjectsError = result.ResultErrors;
                    $scope.tables = null;
                }
            }

            if (inTimer) {
                $scope.gettingObjects = false;
            } else {
                getDataCompleted();
            }
        });
    }

    function needGetObjects(step , inTimer) {
        if (!pageExists()) {
            if (inTimer) {
                clearTimer(step);
                $scope.gettingObjects = false;
            }
            return false;
        }

        if (inTimer) {
            if ($scope.gettingObjects === true) {
                return false;
            }
            $scope.gettingObjects = true;
        }

        return true;
    }

    function getDataCompleted() {
        if ($scope.loading) {
            $scope.loading = false;
            var flags = FeatureFlagService.Flags();
            if (FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE)) {
                $scope.showClearDeploymentLink = true;
                $scope.disableClearDeploymentLink = $scope.deployment.Status === SetupUtility.STATUS_IN_PROGRESS;
            }
        }
    }

    function getTables(jobs) {
        if (jobs == null || jobs.length === 0) {
            return null;
        }

        var tables = [];
        for (var i = 0; i < jobs.length; i++) {
            var table = { name: jobs[i].TableName, info: "" };
            if (jobs[i].Status === SetupUtility.STATUS_SUCCESS) {
                table.info = $filter('number')(jobs[i].ExtractedRows);
            } else if (jobs[i].Status === SetupUtility.STATUS_IN_PROCESS && jobs[i].ExtractedRows > 0) {
                table.info = $filter('number')(jobs[i].ExtractedRows);
                if (jobs[i].TotalRows > 0) {
                    table.info += $scope.ofText + $filter('number')(jobs[i].TotalRows);
                }
            }
            tables.push(table);
        }
        return tables;
    }

    function getEnrichProgress(jobs) {
        if (jobs == null || jobs.length === 0) {
            return ResourceUtility.getString('SETUP_ENRICH_DATA_DEFAULT_PROGRESS_LABEL');
        }

        var job = jobs[jobs.length - 1];
        if (job.ExtractedRows > 0 && job.TotalRows > 0) {
            var extractRows = $filter('number')(job.ExtractedRows);
            var totalRows = $filter('number')(job.TotalRows);
            return extractRows + $scope.ofText + totalRows + ' ' + $scope.leadsText;
        } else {
            return ResourceUtility.getString('SETUP_ENRICH_DATA_DEFAULT_PROGRESS_LABEL');
        }
    }

    $scope.retryImportDataClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.loading = true;
        TenantDeploymentService.ImportSfdcData().then(function (result) {
            if (result.Success === true) {
                $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
            } else {
                updateStatusLabelFail(SetupUtility.STEP_IMPORT_DATA, result.ResultErrors);
                $scope.loading = false;
            }
        });
    };

    $scope.retryEnrichDataClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.loading = true;
        TenantDeploymentService.EnrichData().then(function (result) {
            if (result.Success === true) {
                $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
            } else {
                updateStatusLabelFail(SetupUtility.STEP_ENRICH_DATA, result.ResultErrors);
                $scope.loading = false;
            }
        });
    };

    $scope.retryValidationDataClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.loading = true;
        TenantDeploymentService.ValidateMetadata().then(function (result) {
            if (result.Success === true) {
                $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
            } else {
                updateStatusLabelFail(SetupUtility.VALIDATE_METADATA, result.ResultErrors);
                $scope.loading = false;
            }
        });
    };

    $scope.cancelClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var link = $($event.currentTarget);
        if (link.hasClass("disabled")) {
            return;
        }
        CancelDeploymentStepModel.show($scope, link);
    };

    $scope.cancelStep = function(link) {
        link.addClass("disabled");
        var step = link.attr("step");
        updateStatusLabelInProgress(step, ResourceUtility.getString('SETUP_CANCEL_PROGRESS_LABEL'));
        TenantDeploymentService.CancelLaunch().then(function (result) {
            if (result.Success === true) {
                updateStatusLabelSuccess(step, ResourceUtility.getString('SETUP_CANCEL_SUBMITTED_LABEL'));
            } else {
                updateStatusLabelFail(step, result.ResultErrors);
            }
            link.removeClass("disabled");
        });
    };

    $scope.downloadLinkClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        runQuery($($event.currentTarget));
    };

    function runQuery(link) {
        if (link.hasClass("disabled")) {
            return;
        }

        link.addClass("disabled");
        var step = link.attr("step");
        updateStatusLabelInProgress(step, ResourceUtility.getString('SETUP_RUN_QUERY_PROGRESS_LABEL'));
        TenantDeploymentService.RunQuery(step).then(function (result) {
            if (result.Success === true) {
                startGetQueryStatusTimer(link, result.ResultObj);
            } else {
                link.removeClass("disabled");
                updateStatusLabelFail(step, result.ResultErrors);
            }
        });
    }

    function startGetQueryStatusTimer(link, queryHandle) {
        var step = link.attr("step");
        clearTimer(step);
        var id = window.setInterval(function () { getQueryStatus(link, queryHandle); }, 5000);
        setTimer(step, id);
    }

    function getQueryStatus(link, queryHandle) {
        var step = link.attr("step");
        if (!needGetQueryStatus(step)) {
            return;
        }

        TenantDeploymentService.GetQueryStatus(queryHandle).then(function (result) {
            if (result.Success === true) {
                var obj = result.ResultObj;
                if (obj.Status === 3) {
                    clearTimer(step);

                    $http.get(link.attr("url") + "/" + queryHandle).then(function (response) {
                        link.removeClass("disabled");
                        if (step === SetupUtility.STEP_IMPORT_DATA) {
                            $scope.importInfo = null;
                        } else if (step === SetupUtility.STEP_ENRICH_DATA) {
                            $scope.enrichInfo = null;
                        }

                        var file = new Blob([response.data], { type: link.attr("filetype") });
                        saveAs(file, link.attr("filename"));
                    }, function (response) {
                        SessionService.HandleResponseErrors(response.data, response.status);
                        link.removeClass("disabled");
                        updateStatusLabelFail(step, ResourceUtility.getString('SETUP_DOWNLOAD_QUERY_DATA_ERROR', [link.attr("filename")]));
                    });
                } else if (obj.Status > 3) {
                    clearTimer(step);
                    link.removeClass("disabled");
                    updateStatusLabelFail(step, ResourceUtility.getString('SETUP_RUN_QUERY_ERROR'));
                }
            } else {
                clearTimer(step);
                link.removeClass("disabled");
                updateStatusLabelFail(step, result.ResultErrors);
            }

            $scope.gettingQueryStatus = false;
        });
    }

    function needGetQueryStatus(step) {
        if (!pageExists()) {
            clearTimer(step);
            $scope.gettingQueryStatus = false;
            return false;
        }

        if ($scope.gettingQueryStatus === true) {
            return false;
        }
        $scope.gettingQueryStatus = true;

        return true;
    }

    function pageExists() {
        return $('#importAndEnrichData').length > 0;
    }

    function clearTimer(step) {
        if (step === SetupUtility.STEP_IMPORT_DATA) {
            if ($scope.importTimerId != null) {
                window.clearInterval($scope.importTimerId);
                $scope.importTimerId = null;
            }
        } else if (step === SetupUtility.STEP_ENRICH_DATA) {
            if ($scope.enrichTimerId != null) {
                window.clearInterval($scope.enrichTimerId);
                $scope.enrichTimerId = null;
            }
        }
    }

    function setTimer(step, id) {
        if (step === SetupUtility.STEP_IMPORT_DATA) {
            $scope.importTimerId = id;
        } else if (step === SetupUtility.STEP_ENRICH_DATA) {
            $scope.enrichTimerId = id;
        }
    }

    function updateStatusLabelInProgress(step, message) {
        updateStatusLabel(step, SetupUtility.STATUS_IN_PROGRESS, message);
    }

    function updateStatusLabelSuccess(step, message) {
        updateStatusLabel(step, SetupUtility.STATUS_SUCCESS, message);
    }

    function updateStatusLabelFail(step, message) {
        updateStatusLabel(step, SetupUtility.STATUS_FAIL, message);
    }

    function updateStatusLabel(step, status, message) {
        if (step === SetupUtility.STEP_IMPORT_DATA) {
            $scope.importStatus = status;
            $scope.importInfo = message;
            if (status === SetupUtility.STATUS_SUCCESS) {
                $timeout(function () { $scope.importInfo = null; }, 5000);
            }
        } else if (step === SetupUtility.STEP_ENRICH_DATA) {
            $scope.enrichStatus = status;
            $scope.enrichInfo = message;
            if (status === SetupUtility.STATUS_SUCCESS) {
                $timeout(function () {$scope.enrichInfo = null; }, 5000);
            }
        } else if (step === SetupUtility.STEP_VALIDATE_METADATA) {
            $scope.validationStatus = status;
            $scope.validationInfo = message;
            if (status === SetupUtility.STATUS_SUCCESS) {
                $timeout(function () { $scope.validationInfo = null; }, 5000);
            }
        }
    }

    $scope.finishDeploymentBtnClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT);
    };

    $scope.clearDeploymentLinkClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.disableClearDeploymentLink) {
            return;
        }
        ClearDeploymentModel.show();
    };
})

.directive('importAndEnrichData', function () {
    return {
        scope: {
            deployment: '='
        },
        templateUrl: 'app/setup/views/ImportAndEnrichDataView.html'
    };
});