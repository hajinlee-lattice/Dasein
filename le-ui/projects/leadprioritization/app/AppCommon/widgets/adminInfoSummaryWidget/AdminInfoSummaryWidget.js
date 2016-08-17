angular.module('mainApp.appCommon.widgets.AdminInfoSummaryWidget', [
    'mainApp.appCommon.services.ThresholdExplorerService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService'
])
.controller('AdminInfoSummaryWidgetController', function (
    $scope, ResourceUtility, ThresholdExplorerService, BrowserStorageUtility, FeatureFlagService
) {
	FeatureFlagService.GetAllFlags().then(function(result) {
		var flags = FeatureFlagService.Flags();
		$scope.showPivotMapping = FeatureFlagService.FlagIsEnabled(flags.ALLOW_PIVOT_FILE);
	});

    $scope.ResourceUtility = ResourceUtility;
    $scope.Error = { ShowError: false };

    var clientSession = BrowserStorageUtility.getClientSession();
    var data = $scope.data;

    $scope.ModelId = data.ModelId;
    $scope.TenantId = clientSession.Tenant.Identifier;
    $scope.TenantName = clientSession.Tenant.DisplayName;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.modelUploaded = data.ModelDetails.Uploaded;
    $scope.PivotArtifactPath = data.ModelDetails.PivotArtifactPath;
    $scope.AuthToken = BrowserStorageUtility.getTokenDocument();

    $scope.exportThresholdClicked = function () {
        var csvRows = ThresholdExplorerService.PrepareExportData(data);
        alasql("SELECT * INTO CSV('performance.csv') FROM ?", [csvRows]);
    };
})
.directive('adminInfoSummaryWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/adminInfoSummaryWidget/AdminInfoSummaryWidgetTemplate.html'
    };
})
.directive('healthScore', function() {
    return {
        restrict: 'E',
        template: '{{score | number: 4}}&nbsp;&nbsp;&nbsp;&nbsp;<strong class="{{healthClass}}">{{healthLevel}}</strong>',
        scope: {score: '='},
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
                    '<span data-ng-show="fetching" class="fa fa-spinner fa-pulse fa-fw"></span>&nbsp;' +
                    '<span data-ng-show="fetching">{{ResourceUtility.getString("MODEL_ADMIN_FETCHING")}}</span>' +
                    '<span data-ng-show="modelUploaded">{{ResourceUtility.getString("MODEL_ADMIN_LINK_DISABLED")}}</span>',
        scope:       true,
        link:        function (scope, element, attr) {
            var anchor = element.children()[0];

            scope.$on('download-start', function () {
                $(anchor).attr('disabled', 'disabled');
            });

            scope.$on('download-finished', function () {
                $(anchor).removeAttr('disabled');
            });
        },
        controller:  function ($scope, $attrs, $http, ResourceUtility, SessionService) {
            $scope.fetching = false;
            $scope.fetched = false;
            $scope.ResourceUtility = ResourceUtility;
            $scope.blob = null;

            $scope.downloadFile = function() {
                $scope.fetching = true;
                $scope.$parent.Error.ShowError = false;

                if ($scope.blob != null) {
                    $scope.fetching = false;
                    saveAs($scope.blob, $attrs.filename);
                    return;
                }

                $scope.$emit('download-start');
                $http({
                    method: 'GET',
                    url: $attrs.url,
                    headers: {
                        'ErrorDisplayMethod': 'modal|home.models'
                    }
                }).then(
                    function (response) {
                        if ($attrs.filetype === "application/octet-stream") {
                            var byteArray = new Uint8Array(response.data);
                            var data = pako.ungzip(byteArray);
                            var restored = String.fromCharCode.apply(null, new Uint16Array(data));
                            
                            $scope.blob = new Blob([restored], {type : $attrs.filetype});
                        } else if ($attrs.filetype === "application/json") {
                            $scope.blob = new Blob([JSON.stringify(response.data)], {type : $attrs.filetype});
                        } else {
                            $scope.blob = new Blob([response.data], {type : $attrs.filetype});
                        }
                        $scope.downloadFile();
                        $scope.$emit('download-finished');
                    }, function (response) {
                        SessionService.HandleResponseErrors(response.data, response.status);
                        $scope.$parent.Error.ShowError = true;
                        $scope.$parent.Error.ErrorMsg = ResourceUtility.getString("MODEL_ADMIN_FETCH_ERROR", [$attrs.filename]);
                        $scope.fetching = false;
                        $scope.$emit('download-finished');
                    }
                );
            };
        }
    };
});
