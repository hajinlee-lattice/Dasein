angular.module('login.form', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService'
])
.component('loginForm', {
    templateUrl: 'app/login/form/form.component.html',
    bindings: {
        logindocument: '<'
    },
    controller: function (
        $state, $window, $location, $stateParams, ResourceUtility, LoginService, BrowserStorageUtility, 
        SessionTimeoutUtility, TimestampIntervalUtility, Banner
    ) {
        var vm = this;

        vm.$onInit = function() {

            vm.isLoggedInWithTempPassword = vm.logindocument.MustChangePassword;
            vm.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(vm.logindocument.PasswordLastModified);

            if (vm.logindocument.UserName && !vm.isLoggedInWithTempPassword && !vm.isPasswordOlderThanNinetyDays) {
                $state.go('login.tenants');
                return;
            }

            vm.ResourceUtility = ResourceUtility;
            vm.username = "";
            vm.password = "";
            vm.visible = false;
            vm.loginInProgress = false;
            vm.showForgotPassword = false;
            vm.dateString = '2010 - ' + (new Date()).getFullYear();
            vm.history = [];
            vm.visible = true;

            $('[autofocus]').focus();
        }

        vm.loginClick = function () {
            if (vm.loginInProgress) {
                return;
            }

            vm.usernameInvalid = vm.username === "";
            vm.passwordInvalid = vm.password === "";

            if (vm.usernameInvalid || vm.passwordInvalid) {
                return;
            }

            vm.loginInProgress = true;

            LoginService.Login(vm.username, vm.password).then(function(result) {
                vm.loginInProgress = false;

                if (result !== null && result.Success === true) {

                    var params = $location.$$search;
                    if (Object.keys(params).length != 0 && params.constructor === Object){
                        
                        LoginService.PostToJwt(params).then(function(result){

                            console.log(result);

                            // $window.location.href = result.content;
                        });
                        
                    } else {
                        SessionTimeoutUtility.refreshSessionLastActiveTimeStamp();
                        $state.go('login.tenants');
                    }
                } else {
                    Banner.reset();
                    vm.showLoginHeaderMessage(result);
                }
            });
        };

        vm.ssoClick = function () {
            
            vm.tenantNameInvalid = vm.tenantName === "";
            
            console.log();

            if (vm.tenantNameInvalid) {
                return;
            }

            vm.loginInProgress = true;
            var params = $location.$$search;

            LoginService.SSOLogin(vm.tenantName, params).then(function(result){
                vm.loginInProgress = false;
                $window.location.href = result.content;
            })

        }

        vm.showLoginHeaderMessage = function (message) {
            if (message == null) {
                return;
            }

            if (message.indexOf("Global Auth") > -1) {
                message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
            }

            Banner.error({
                message: message
            });
        };

        vm.forgotPasswordClick = function ($event) {
            $state.go('login.forgot');
        };
    }
});