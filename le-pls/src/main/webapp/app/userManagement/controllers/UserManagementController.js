angular.module('mainApp.userManagement.controllers.UserManagementController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.userManagement.modals.AddUserModal',
    'mainApp.userManagement.services.UserManagementService'
])

.controller('UserManagementController', function ($scope, ResourceUtility, BrowserStorageUtility, AddUserModal, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $("#userManagementError").hide();
    $scope.errorMessage = ResourceUtility.getString("USER_MANAGEMENT_GET_USERS_ERROR");
    UserManagementService.GetUsers().then(function(result) {
        if (result && result.success === true) {
            $scope.Users = result.resultObj;
        } else {
            $("#userManagementError").fadeIn();
        }
    });
    
    $scope.addUserClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        AddUserModal.show(function (newUser) {
            $scope.Users.push(newUser);
        });
    };
});