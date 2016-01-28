angular.module('mainApp.setup.controllers.LeadEnrichmentAttributesDetailsModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.services.LeadEnrichmentService'
])

.service('LeadEnrichmentAttributesDetailsModel', function ($compile, $rootScope, $http) {
    this.show = function($parentScope, title, attributes) {
        $http.get('./app/setup/views/LeadEnrichmentAttributesDetailsView.html').success(function (html) {
            var scope = $parentScope.$new();
            scope.title = title;
            scope.attributes = attributes;
            scope.hasAttributes = (attributes != null && attributes.length > 0);
            var contentContainer = $('#leadEnrichmentAttributesDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };
})

.controller('LeadEnrichmentAttributesDetailsController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.$parent.setAttributesDetails(true);
    $scope.backButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.attributes = null;
        $scope.$parent.setAttributesDetails(false);
    };
});