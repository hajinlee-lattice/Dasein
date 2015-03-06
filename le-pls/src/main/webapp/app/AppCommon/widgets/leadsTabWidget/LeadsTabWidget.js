angular.module('mainApp.appCommon.widgets.LeadsTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.GriotNavUtility'
])
.controller('LeadsTabWidgetController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, RightsUtility, GriotNavUtility) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    $scope.showAdminLink = RightsUtility.maySeeHiddenAdminTab(clientSession.availableRights);

    var adminData = {
        ModelDetails:   $scope.data.ModelDetails,
        Segmentaions:   $scope.data.Segmentations,
        ModelId:        $scope.data.ModelId,
        TenantId:       clientSession.Tenant.Identifier
    }

    $scope.adminLinkClick = function() {
       $rootScope.$broadcast(GriotNavUtility.ADMIN_INFO_NAV_EVENT, adminData);
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