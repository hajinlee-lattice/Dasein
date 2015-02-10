angular.module('mainApp.login.modals.ForgotPasswordModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.login.services.LoginService'
])
.service('ForgotPasswordModal', function ($compile, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function (successCallback, failCallback) {
        $http.get('./app/login/views/ForgotPasswordView.html').success(function (html) {
            
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
.controller('ForgotPasswordController', function ($scope, ResourceUtility, LoginService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.username = null;
    
    $scope.forgotPasswordClick = function () {
        if ($scope.username == null || $scope.username === "") {
            return;
        }
        
        LoginService.ResetPassword($scope.username).then(function(result) {
            $("#modalContainer").modal('hide');
            if (result == null) {
                return;
            }
            
            if (result.Success === true) {
                if ($scope.successCallback != null) {
                    $scope.successCallback();
                }
                $("#modalContainer").modal('hide');
            } else {
                if ($scope.failCallback != null) {
                    $scope.failCallback();
                }
            }
        });
    };
});
