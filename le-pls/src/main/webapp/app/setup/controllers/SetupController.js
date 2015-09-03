angular.module('mainApp.setup.controllers.SetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.core.services.WidgetService',
    'mainApp.setup.controllers.ManageFieldsController',
    'mainApp.setup.services.ManageFieldsService'
])

.controller('SetupController', function ($scope, $http, $rootScope, $compile, ResourceUtility, BrowserStorageUtility, RightsUtility, NavUtility, WidgetConfigUtility, WidgetFrameworkService, WidgetService) {
    $scope.ResourceUtility = ResourceUtility;

    if (BrowserStorageUtility.getClientSession() == null) { return; }

    $scope.selectedNode = null;

    $scope.nodeClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var nodeName = $event.currentTarget.attributes["node-name"].value;
        $scope.selectedNode = nodeName;
    };

    $scope.nodeHandleClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var element = $($event.currentTarget);
        var expand = element.hasClass("fa-caret-down");
        if (expand) {
            element.removeClass("fa-caret-down").addClass("fa-caret-right");
            element.parents("li:first").siblings().hide();
        } else {
            element.removeClass("fa-caret-right").addClass("fa-caret-down");
            element.parents("li:first").siblings().show();
        }
    };

});