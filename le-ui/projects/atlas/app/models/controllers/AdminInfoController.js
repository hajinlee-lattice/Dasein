angular.module('mainApp.models.controllers.AdminInfoController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.core.utilities.NavUtility',
    'mainApp.models.services.ModelService',
    'common.services.featureflag'
])
.controller('AdminInfoController', function ($scope, $rootScope, $http, ResourceUtility, NavUtility, ModelService, FeatureFlagService, ModelStore) {
    $scope.ResourceUtility = ResourceUtility;

    var data = ModelStore.data;
    $scope.ModelId = data.ModelId;
    $scope.TenantId = data.TenantId;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.TemplateVersion = data.ModelDetails.TemplateVersion;
    $scope.modelUploaded = data.ModelDetails.Uploaded;
    $scope.displayName = data.ModelDetails.DisplayName;

    $scope.onBackClicked = function() {
        var model = {
            Id: data.ModelId,
            DisplayName: data.ModelDetails.DisplayName,
            CreatedDate: data.ModelDetails.ConstructionTime,
            Status: data.ModelDetails.Status
        };
        $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, model);
    };

    var flags = FeatureFlagService.Flags();
    var showAlertsTab = FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);

    if (showAlertsTab) {
        $scope.loading= true;
    }

});