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
    $scope.TrainingFileExist = data.ModelDetails.TrainingFileExist;
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
});
