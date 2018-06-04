angular.module('mainApp.appCommon.widgets.AdminInfoSummaryWidget', [
    'mainApp.appCommon.services.ThresholdExplorerService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.services.SessionService',
    'lp.jobs'
])
.controller('AdminInfoSummaryWidgetController', function (
    $scope, ResourceUtility, ThresholdExplorerService, BrowserStorageUtility, FeatureFlagService, JobsStore
) {
	FeatureFlagService.GetAllFlags().then(function(result) {
		var flags = FeatureFlagService.Flags();
		$scope.showPivotMapping = FeatureFlagService.FlagIsEnabled(flags.ALLOW_PIVOT_FILE);
        $scope.isCDL = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
	});

    $scope.ResourceUtility = ResourceUtility;
    $scope.Error = { ShowError: false };

    var clientSession = BrowserStorageUtility.getClientSession();
    var data = $scope.data;

    $scope.ModelId = data.ModelId;
    $scope.DataCloudVersion = data.EventTableProvenance.Data_Cloud_Version;
    $scope.TenantId = clientSession.Tenant.Identifier;
    $scope.TenantName = clientSession.Tenant.DisplayName;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.modelUploaded = data.ModelDetails.Uploaded;
    $scope.PivotArtifactPath = data.ModelDetails.PivotArtifactPath;
    $scope.TrainingFileExist = data.ModelDetails.TrainingFileExist;
    $scope.AuthToken = BrowserStorageUtility.getTokenDocument();
    $scope.sourceType = data.ModelDetails.SourceSchemaInterpretation;
    $scope.EventTableName = data.EventTableProvenance.EventTableName;
    var propertyLength = data.ModelDetails.ModelSummaryProvenanceProperties.length;
    for (var i = 0; i < propertyLength; i++) {
        if (data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.option == ResourceUtility.getString("MODEL_TRAINING_FILE_PATH")) {
            $scope.TrainingFilePath = data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.value;
        } else if (data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.option == "WorkflowJobId") {
            $scope.workflowJobId = data.ModelDetails.ModelSummaryProvenanceProperties[i].ModelSummaryProvenanceProperty.value;
        }
    }

    $scope.modelCreationJobFinished = false;
    if ($scope.workflowJobId != null && $scope.workflowJobId > 0) {
        JobsStore.getJob($scope.workflowJobId).then(function(job) {
            if (job != null && job.jobStatus == "Completed") {
                $scope.modelCreationJobFinished = true;
            }
        });
    }

    $scope.exportThresholdClicked = function () {
        var csvRows = ThresholdExplorerService.PrepareExportData(data);
        alasql("SELECT * INTO CSV('performance.csv') FROM ?", [csvRows]);
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
