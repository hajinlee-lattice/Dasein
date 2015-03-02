angular.module('mainApp.userManagement.modals.AddUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.PasswordUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.userManagement.services.UserManagementService'
])
.service('AddUserModal', function ($compile, $rootScope, $http) {
    this.show = function (emails) {
        $http.get('./app/userManagement/views/AddUserView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.emails = emails;

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
.controller('AddUserController', function ($scope, $rootScope, _, ResourceUtility, StringUtility, PasswordUtility, GriotNavUtility, UserManagementService) {
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

        if (_.contains($scope.emails, $scope.user.Email)) {
            $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_CONFLICT_EMAIL");
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

        UserManagementService.GetUserByEmail($scope.user.Email).then(function(result){
            if (result.Success) {
                $scope.existingUser = result.ResultObj;
                $scope.showExistingUser = true;
            } else {
                UserManagementService.AddUser($scope.user).then(function(result){
                    if (result.Success) {
                        $scope.showAddUserSuccess = true;
                        $scope.addUserSuccessMessage=ResourceUtility.getString("ADD_USER_SUCCESS", [result.ResultObj.Username, result.ResultObj.Password]);
                    } else {
                        $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_GENERAL_ERROR");
                        $scope.showAddUserSuccess = false;
                        $scope.showAddUserError = true;
                    }
                    $scope.saveInProgress = false;
                    $scope.showExistingUser = false;
                    $event.target.blur();
                });
            }
        });

    };

    $scope.yesClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showExistingUser = false;
        $scope.user.FirstName = $scope.existingUser.FirstName;
        $scope.user.LastName = $scope.existingUser.LastName;
        $scope.user.Email = $scope.existingUser.Email;
        $scope.user.Username = $scope.existingUser.Username;

        UserManagementService.GrantDefaultRights($scope.user.Username).then(function(result){
            if (result.Success) {
                $scope.showAddUserSuccess = true;
                $scope.addUserSuccessMessage=ResourceUtility.getString("ADD_EXSITING_USER_SUCCESS", [$scope.user.Username]);
            } else {
                $scope.addUserErrorMessage = ResourceUtility.getString("ADD_USER_GENERAL_ERROR");
                $scope.showAddUserSuccess = false;
                $scope.showAddUserError = true;
            }
            $scope.saveInProgress = false;
            $event.target.blur();
        });
    };

    $scope.cancelClick = function () {
        if (!$scope.saveInProgress) {
            $("#modalContainer").modal('hide');
            $rootScope.$broadcast(GriotNavUtility.USER_MANAGEMENT_NAV_EVENT);
        }
    };

    $scope.noClick = function () {
        $scope.saveInProgress = false;
        $scope.showExistingUser = false;
    };
});
