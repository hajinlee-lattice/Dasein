angular.module('mainApp.setup.controllers.ManageFieldsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService'

])

.directive('manageFields', function () {
    return {
        templateUrl: 'app/setup/views/ManageFieldsView.html',
        controller: ['$scope', 'ResourceUtility', 'BrowserStorageUtility', 'WidgetConfigUtility', 'WidgetFrameworkService', 'WidgetService',
                        function ($scope, ResourceUtility, BrowserStorageUtility, WidgetConfigUtility, WidgetFrameworkService, WidgetService) {

            $scope.ResourceUtility = ResourceUtility;

            if (BrowserStorageUtility.getClientSession() == null) { return; }

            var widgetConfig = WidgetService.GetApplicationWidgetConfig();
                if (widgetConfig == null) {
                    return;
                }

                var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
                    widgetConfig,
                    "manageFieldsScreenWidget"
                );

                if (screenWidgetConfig == null) {
                    return;
                }

                var contentContainer = $('#manageFieldsContainer');
                WidgetFrameworkService.CreateWidget({
                    element:      contentContainer,
                    widgetConfig: screenWidgetConfig,
                    metadata:     null,
                    data:         $scope.data,
                    parentData:   null
                });
        }]
    };
});