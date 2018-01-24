angular.module('lp.jobs.row.subjobs', [])

    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', function ($scope, JobsStore) {
            function init() {

            }
            $scope.getActionType = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    switch (subjob.reports[0].purpose) {
                        case 'IMPORT_DATA_SUMMARY': {
                            return 'Import';
                        };
                        default: {
                            return 'Unknown';
                        }
                    }
                } else {
                    return 'Unknwon';
                }
            }
            $scope.getActionName = function (subjob) {
                if (subjob.inputs && subjob.inputs != null) {
                    return subjob.inputs['SOURCE_DISPLAY_NAME'];
                }
            }
            $scope.getActionLink = function (subjob) {
                if (subjob.inputs && subjob.inputs != null) {
                    return subjob.inputs['SOURCE_DISPLAY_NAME'];
                }
            }

            $scope.getValidation = function (subjob) {

            }
            $scope.getRecordFound = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.total_rows;
                } else {
                    return '-';
                }
            }
            $scope.getRecordFailed = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.ignored_rows;
                } else {
                    return '-';
                }
            }
            $scope.getRecordUploaded = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.imported_rows;
                } else {
                    return '-';
                }
            }
            $scope.getUser = function (subjob) {
                return subjob.user;
            }

            init();
        }];

        return {
            restrict: 'E',
            replace: true,
            scope: {
                subjobs: '=',
                stepscompleted: '='
            },
            controller: controller,
            templateUrl: "app/jobs/processing/import-job-row-subjobs.component.html",
        };
    }]);

