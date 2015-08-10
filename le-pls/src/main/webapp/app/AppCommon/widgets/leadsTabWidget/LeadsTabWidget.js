angular.module('mainApp.appCommon.widgets.LeadsTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.appCommon.utilities.TenantIdParsingUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility'
])
.controller('LeadsTabWidgetController', function ($scope, $rootScope, ResourceUtility, TenantIdParsingUtility, BrowserStorageUtility, RightsUtility, NavUtility) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    $scope.showAdminLink = RightsUtility.maySeeAdminInfo();
    $scope.data.TenantId = clientSession.Tenant.Identifier;
    $scope.data.TenantName = clientSession.Tenant.DisplayName;
    $scope.data.DataLoaderTenantName = TenantIdParsingUtility.getDataLoaderTenantNameFromTenantId($scope.data.TenantId);

    $scope.adminLinkClick = function() {
       $rootScope.$broadcast(NavUtility.ADMIN_INFO_NAV_EVENT, $scope.data);
    };

})
.directive('leadsTabWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/leadsTabWidget/LeadsTabWidgetTemplate.html'
    };

    return directiveDefinitionObject;
})
.directive('leadsTable', function() {
    return {
        restrict:    'E',
        templateUrl: 'app/AppCommon/widgets/leadsTabWidget/LeadsTableTemplate.html',
        scope:       {leads: '=', title: '@'},
        controller:  ['$scope', '$attrs', '$http', 'ResourceUtility', function ($scope, $attrs, $http, ResourceUtility) {
            $scope.ResourceUtility = ResourceUtility;
        }]
    };
});