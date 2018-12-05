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

            var urlParams = $location.$$search;
            vm.params = (Object.keys(urlParams).length != 0 && urlParams.constructor === Object) ? urlParams : $stateParams.obj;

            if (vm.params.logout) {
                Banner.error({
                    message: 'You have been logged out.'
                });
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

                    if (Object.keys(vm.params).length != 0 && vm.params.constructor === Object){
                        
                        LoginService.PostToJwt(vm.params).then(function(result){
                            $window.location.href = result.url;
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