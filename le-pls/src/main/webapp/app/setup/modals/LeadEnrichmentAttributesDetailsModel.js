angular.module('mainApp.setup.modals.LeadEnrichmentAttributesDetailsModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.services.LeadEnrichmentService'
])

.service('LeadEnrichmentAttributesDetailsModel', function ($compile, $rootScope, $http) {
    this.show = function($parentScope) {
        $http.get('./app/setup/views/LeadEnrichmentAttributesDetailsView.html').success(function (html) {
            var scope = $parentScope.$new();
            var contentContainer = $('#leadEnrichmentAttributesDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };
})

.controller('LeadEnrichmentAttributesDetailsController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, LeadEnrichmentService) {
    $scope.$parent.setAttributesDetails(true);
    $scope.ResourceUtility = ResourceUtility;

    $scope.loading = true;
    LeadEnrichmentService.GetSavedAttributes().then(function (data) {
        if (!data.Success) {
            $scope.loadingError = data.ResultErrors;
            $scope.showLoadingError = true;
        } else {
            $scope.attributes = data.ResultObj;
        }
        $scope.loading = false;
    });
});