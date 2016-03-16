angular.module('mainApp.setup.modals.EditFieldModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.setup.utilities.SetupUtility',
    'mainApp.setup.modals.UpdateFieldsModal'
])

.service('EditFieldModel', function ($compile, $templateCache, $rootScope, $http) {

    this.show = function(fieldToBeEdited, $manageFieldsScope) {
        $http.get('app/setup/views/EditFieldView.html', { cache: $templateCache }).success(function (html) {
            var scope = $manageFieldsScope.$new();
            scope.field = angular.copy(fieldToBeEdited);
            scope.manageFieldsScope = $manageFieldsScope;

            var contentContainer = $('#fieldDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };

})

.controller('EditFieldController', function ($scope, $state, ResourceUtility, StringUtility,
                                              SetupUtility, UpdateFieldsModal) {
    $scope.manageFieldsScope.showFieldDetails = true;
    $scope.ResourceUtility = ResourceUtility;

    $scope.saveInProgress = false;
    $scope.approvedUsagesToSelect = $scope.$parent.ApprovedUsageOptions;
    $scope.categoriesToSelect = $scope.$parent.CategoryOptions;
    $scope.fundamentalTypesToSelect = $scope.$parent.FundamentalTypeOptions;
    $scope.statisticalTypesToSelect = $scope.$parent.StatisticalTypeOptions;
    $scope.categoryEditable = $scope.$parent.categoryEditable($scope.field);
    $scope.fieldCopy = angular.copy($scope.field);

    $scope.saveClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showEditFieldError = false;

        $scope.saveInProgress = false;
        $scope.manageFieldsScope.showFieldDetails = false;
        $scope.$emit(SetupUtility.LOAD_FIELDS_EVENT, $scope.fieldCopy, $scope.field);
    };

    $scope.cancelClicked = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.showEditFieldError = false;
        $scope.manageFieldsScope.showFieldDetails = false;
    };
});