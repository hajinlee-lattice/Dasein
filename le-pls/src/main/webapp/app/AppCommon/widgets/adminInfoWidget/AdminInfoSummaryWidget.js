angular.module('mainApp.appCommon.widgets.AdminInfoSummaryWidget', [
    'mainApp.appCommon.services.ThresholdExplorerService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.services.SessionService'
])
.controller('AdminInfoSummaryWidgetController', function ($scope, $rootScope, $http, ResourceUtility, ThresholdExplorerService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.Error = { ShowError: false };

    var data = $scope.data;
    $scope.ModelId = data.ModelId;
    $scope.TenantId = data.TenantId;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.TemplateVersion = data.ModelDetails.TemplateVersion;
    $scope.modelUploaded = data.ModelDetails.Uploaded;

    $scope.exportThresholdClicked = function () {
        var csvRows = ThresholdExplorerService.PrepareExportData(data);
        alasql("SELECT * INTO CSV('performance.csv') FROM ?", [csvRows]);
    };
})
.directive('adminInfoSummaryWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/adminInfoWidget/AdminInfoSummaryWidgetTemplate.html'
    };
})
.directive('healthScore', function() {
    return {
        restrict: 'E',
        template: '{{score | number: 4}}&nbsp;&nbsp;&nbsp;&nbsp;<strong class="{{healthClass}}">{{healthLevel}}</strong>',
        scope:    {score: '='},
        controller: ['$scope', 'ResourceUtility', function ($scope, ResourceUtility) {

            if ($scope.score >= 0.75) {
                $scope.healthLevel = ResourceUtility.getString("MODEL_ADMIN_HEALTH_EXCELLENT");
                $scope.healthClass = "health-excellent";
            } else if ($scope.score >= 0.6) {
                $scope.healthLevel = ResourceUtility.getString("MODEL_ADMIN_HEALTH_GOOD");
                $scope.healthClass = "health-good";
            } else if ($scope.score >= 0.5) {
                $scope.healthLevel = ResourceUtility.getString("MODEL_ADMIN_HEALTH_MEDIUM");
                $scope.healthClass = "health-medium";
            } else {
                $scope.healthLevel = ResourceUtility.getString("MODEL_ADMIN_HEALTH_POOR");
                $scope.healthClass = "health-poor";
            }
        }]
    };
})
.directive('fileDownloader', function() {
    return {
        restrict:    'E',
        template:   '<a href="" data-ng-click="downloadFile($event)" data-ng-hide="modelUploaded||fetching">{{ResourceUtility.getString("MODEL_ADMIN_DOWNLOAD")}}</a>' +
                    '<span data-ng-show="fetching">{{ResourceUtility.getString("MODEL_ADMIN_FETCHING")}}</span>' +
                    '<span data-ng-show="modelUploaded">{{ResourceUtility.getString("MODEL_ADMIN_LINK_DISABLED")}}</span>',
        scope:       true,
        link:        function (scope, element, attr) {
            var anchor = element.children()[0];

            // When the download starts, disable the link
            scope.$on('download-start', function () {
                $(anchor).attr('disabled', 'disabled');
            });

            // When the download finishes, attach the data to the link. Enable the link and change its appearance.
            scope.$on('downloaded', function (event, data) {
                scope.downloadFile = function ($event) { };
                $(anchor).attr({
                    href: 'data:' + attr.filetype + ';base64,' + data,
                    download: attr.filename
                }).removeAttr('disabled');
            });

            scope.$on('download-failed', function () {
                $(anchor).removeAttr('disabled');
            });
        },
        controller:  ['$scope', '$attrs', '$http', 'ResourceUtility', 'SessionService', function ($scope, $attrs, $http, ResourceUtility, SessionService) {
            $scope.fetching = false;
            $scope.fetched = false;
            $scope.ResourceUtility = ResourceUtility;            

            $scope.downloadFile = function($event) {
                $scope.fetching = true;
                $scope.$parent.Error.ShowError = false;
                $scope.$emit('download-start');

                $http.get($attrs.url).then(function (response) {
                    if ($attrs.filetype === "application/json") {
                        $scope.$emit('downloaded', btoa(decodeURIComponent(encodeURIComponent(JSON.stringify(response.data)))));
                    } else {
                        $scope.$emit('downloaded', btoa(decodeURIComponent(encodeURIComponent(response.data))));
                    }
                    $scope.fetching = false;
                    setTimeout($event.target.click(), 500);
                }, function (response) {
                    SessionService.HandleResponseErrors(response.data, response.status);
                    $scope.$parent.Error.ShowError = true;
                    $scope.$parent.Error.ErrorMsg = ResourceUtility.getString("MODEL_ADMIN_FETCH_ERROR", [$attrs.filename]);
                    $scope.fetching = false;
                    $scope.$emit('download-failed');
                });
            };
        }]
    };
});