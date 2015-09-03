angular.module('mainApp.setup.modals.EditFieldModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.setup.services.ManageFieldsService'
])

.service('EditFieldModel', function ($compile, $rootScope, $http) {

    this.show = function(fieldToBeEdited) {
        $http.get('./app/setup/views/EditFieldView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.field = fieldToBeEdited;

            var contentContainer = $('#fieldDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };

})

.controller('EditFieldController', function ($scope, $rootScope, _, ResourceUtility, BrowserStorageUtility, StringUtility) {
    $scope.$parent.showFieldDetails = true;
    $scope.ResourceUtility = ResourceUtility;

    $scope.cancelEditClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.$parent.showFieldDetails = false;
    };
});