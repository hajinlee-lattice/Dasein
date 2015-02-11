angular.module('mainApp.userManagement.modals.AddUserModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.userManagement.services.UserManagementService'
])
.service('AddUserModal', function ($compile, $rootScope, $http, ResourceUtility) {
    var self = this;
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
.controller('AddUserController', function ($scope, ResourceUtility, StringUtility, UserManagementService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.emailAddress = null;
    $scope.firstName = null;
    $scope.lastName = null;
    
    $scope.emailAddressInputError = "";
    $scope.firstNameInputError = "";
    $scope.lastNameInputError = "";
    
    $scope.saveInProgess = false;
    
    $scope.addUserErrorMessage = "";
    $("#addUserError").hide();
    
    function updateDisplayValidation () {
        $scope.emailAddressInputError = StringUtility.IsEmptyString($scope.emailAddress) ? "error" : "";
        $scope.firstNameInputError = StringUtility.IsEmptyString($scope.firstName) ? "error" : "";
        $scope.lastNameInputError = StringUtility.IsEmptyString($scope.lastName) ? "error" : "";

    }
    
    function areInputsValid () {
        return !StringUtility.IsEmptyString($scope.emailAddress) && !StringUtility.IsEmptyString($scope.firstName) && !StringUtility.IsEmptyString($scope.lastName);
    }
    
    
    $scope.addUserClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        $("#addUserError").hide();
        updateDisplayValidation();
        if (areInputsValid() && !$scope.saveInProgess) {
            $scope.saveInProgess = true;
            UserManagementService.AddUser($scope.emailAddress, $scope.firstName, $scope.lastName).then(function(result) {
                $scope.saveInProgess = false;
                if (result && result.success === true) {
                    if ($scope.successCallback != null && typeof $scope.successCallback === "function") {
                        var user = result.resultObj;
                        $scope.successCallback(user);
                    }
                    $("#modalContainer").modal('hide');
                } else {
                    // TODO:pierce Need more useful error messages
                    $scope.addUserErrorMessage = result.resultErrors;
                    $("#addUserError").fadeIn();
                }
            });
        }
        
    };
    
    $scope.cancelClick = function () {
        if (!$scope.saveInProgess) {
            $("#modalContainer").modal('hide');
        }
    };
});
