angular.module('mainApp.userManagement.modals.AddUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.userManagement.services.UserManagementService'
])
.service('AddUserModal', function ($compile, $rootScope, $http) {
    this.show = function (successCallback, failCallback) {
        $http.get('./app/userManagement/views/AddUserView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.successCallback = successCallback;
            scope.failCallback = failCallback;
            
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
})
.controller('AddUserController', function ($scope, $rootScope, ResourceUtility, StringUtility, PasswordUtility, GriotNavUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.saveInProgress = false;
    $scope.addUserErrorMessage = "";
    $scope.showAddUserError = false;

    $scope.user = {};

    function validateNewUser() {
        if ($scope.form.$error.required) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_REQUIERD");
            return false;
        }

        if ($scope.form.$error.email) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_INVALID_EMAIL");
            return false;
        }

        return true;
    }
    
    $scope.addUserClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.saveInProgress) { return; }
        $scope.saveInProgress = true;
        $scope.showAddUserError = false;
        $scope.showAddUserSuccess = false;

        if (!validateNewUser()) {
            $scope.saveInProgress = false;
            $scope.showAddUserError = true;
            $event.target.blur();
            return;
        }

        UserManagementService.AddUser($scope.user).then(function(result){
            if (result.Success) {
                $scope.showAddUserSuccess = true;
                $scope.addUserSuccessMessage=ResourceUtility.getString("ADD_USER_SUCCESS", [result.ResultObj.Username, result.ResultObj.Password]);
                $scope.saveInProgress = false;
                $event.target.blur();
            } else {
                $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_GENERAL_ERROR");
                $scope.showAddUserError = true;
                $scope.showAddUserSuccess = false;
                $scope.saveInProgress = false;
                $event.target.blur();
            }
        });
    };

    $scope.cancelClick = function () {
        if (!$scope.saveInProgress) {
            $("#modalContainer").modal('hide');
            $rootScope.$broadcast(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT);
        }
    };
});
