angular.module('mainApp.models.controllers.AdminInfoController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.core.utilities.NavUtility',
    'mainApp.models.services.ModelService'
])
.controller('AdminInfoController', function ($scope, $rootScope, $http, ResourceUtility, WidgetService,
    WidgetConfigUtility, WidgetFrameworkService, NavUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "adminInfoScreenWidget"
    );

    if (screenWidgetConfig == null) {
        return;
    }

    var data = $scope.data;
    $scope.ModelId = data.ModelId;
    $scope.TenantId = data.TenantId;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.TemplateVersion = data.ModelDetails.TemplateVersion;
    $scope.modelUploaded = data.ModelDetails.Uploaded;
    $scope.displayName = data.ModelDetails.DisplayName;
    $scope.loading= true;

    $scope.onBackClicked = function() {
        var model = {
            Id: data.ModelId,
            DisplayName: data.ModelDetails.DisplayName,
            CreatedDate: data.ModelDetails.ConstructionTime,
            Status: data.ModelDetails.Status
        };
        $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, model);
    };

    ModelService.GetModelAlertsByModelId(data.ModelId).then(function(result) {
        $scope.loading= false;
        if (result != null && result.success === true) {
            data.ModelAlerts = result.resultObj;

            var contentContainer = $('#adminInfoContainer');
            WidgetFrameworkService.CreateWidget({
                element: contentContainer,
                widgetConfig: screenWidgetConfig,
                metadata: null,
                data: data,
                parentData: null
            });
        }
    });

});