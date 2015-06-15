angular.module('mainApp.models.controllers.AdminInfoController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.core.utilities.NavUtility'
])
.controller('AdminInfoController', function ($scope, $rootScope, $http, ResourceUtility, WidgetService, WidgetConfigUtility, WidgetFrameworkService, NavUtility) {
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

    $scope.onBackClicked = function() {
        var model = {
            Id: $scope.ModelId,
            DisplayName: data.ModelDetails.DisplayName,
            CreatedDate: data.ModelDetails.ConstructionTime,
            Status: data.ModelDetails.Status
        };
        $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, model);
    };

    var contentContainer = $('#adminInfoContainer');
    WidgetFrameworkService.CreateWidget({
        element:      contentContainer,
        widgetConfig: screenWidgetConfig,
        metadata:     null,
        data:         $scope.data,
        parentData:   null
    });

});