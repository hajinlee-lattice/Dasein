var app = angular.module('mainApp.userManagement.modals.DeleteUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.userManagement.services.UserManagementService'
]);

app.service('DeleteUserModal', function ($compile, $templateCache, $rootScope, $http) {
    this.show = function (userToBeDeleted) {
        $http.get('app/userManagement/views/DeleteUserView.html', { cache: $templateCache }).success(function (html) {
            
            var scope = $rootScope.$new();
            scope.user = userToBeDeleted;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            
            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');
            
            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
});

app.controller('DeleteUserController', function ($scope, $rootScope, $state, _, ResourceUtility,
                                                 BrowserStorageUtility, StringUtility, PasswordUtility,
                                                 NavUtility, RightsUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteInProgress = false;
    $scope.deleteLoadingMsg = ResourceUtility.getString('DELETE_USER_WAIT', [$scope.user.Username]);

    $scope.noClick = function() {
        $("#modalContainer").modal('hide');
    };

    $scope.yesClick = function() {
        if ($scope.deleteInProgress) { return; }
        $scope.deleteInProgress = true;

        UserManagementService.DeleteUser($scope.user).then(function(result){
            if(result.Success) {
                $("#modalContainer").modal('hide');
                $state.go('home.users', {}, { reload: true });
            } else {
                if (result.ResultErrors === ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')) {
                    $scope.deleteUserErrorMessage = ResourceUtility.getString("DELETE_USER_GENERAL_ERROR");
                } else {
                    $scope.deleteUserErrorMessage = result.ResultErrors;
                }
                $scope.showDeleteUserError = true;
            }
            $scope.deleteInProgress = false;
        });
    };
    
    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };

});
