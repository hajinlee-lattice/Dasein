angular.module('login')
.component('loginFrame', {
    templateUrl: 'app/login/login.component.html',
    bindings: {
        logindocument: '<',
        clientsession: '<'
    },
    controller: function(
        $state, $location, $window, ResourceUtility, LoginService, Banner,
        SessionTimeoutUtility, TimestampIntervalUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this;

        vm.$onInit = function() {
            vm.ResourceUtility = ResourceUtility;
            vm.login = LoginStore.login;
            vm.state = $state;
            vm.history = [];
            vm.loginInProgress = {};
            vm.isLoggedInWithTempPassword = vm.logindocument.MustChangePassword;
            vm.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(vm.logindocument.PasswordLastModified);
            vm.params = $location.$$search;

            if (SessionTimeoutUtility.hasSessionTimedOut() && vm.logindocument.UserName) {
                return LoginService.Logout(vm.params);
            } else {
                switch($state.current.name) {
                    case 'login.form': 
                        if (vm.logindocument.UserName) {
                            $state.go('login.tenants', { obj: vm.params });
                        }
                        break;
                    case 'login.tenants':
                        if (!vm.logindocument.UserName) {
                            $state.go('login.form');
                        }
                        break;
                    case 'login':
                        if (((Object.keys(vm.params).length != 0) && (vm.params.type == 'jwt')) && vm.logindocument.UserName) {
                            LoginService.PostToJwt(vm.params).then(function(result){
                                $window.location.href = result.url;
                            });
                        } else {
                            $state.go('login.form', { obj: vm.params });
                        }
                        break;
                }
            }

            LoginStore.set(vm.logindocument, vm.clientsession);

            angular.element('body').addClass('initialized');
        };

        vm.getHistory = function(username) {
            vm.history = BrowserStorageUtility.getHistory(username, vm.login.tenant) || [];

            return vm.history;
        };

        vm.clearHistory = function(username) {
            BrowserStorageUtility.clearHistory(username);
        };

        vm.clickTenant = function(tenant, username) {
            vm.loginInProgress[tenant.DisplayName] = true;

            LoginService.GetSessionDocument(tenant, username).then(function(data) {
                if (data != null && data.Success === true) {
                    vm.aptrinsic(vm.login, tenant);
                    LoginStore.redirectToLP(tenant);
                } else {
                    vm.loginInProgress[tenant.DisplayName] = false;
                    
                    Banner.error({
                        message: ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR")
                    });
                }
            });
        };

        vm.aptrinsic = function(login, tenant) {
            if(window.aptrinsic) {
                window.aptrinsic("identify", { 
                    "id": login.username, // Required for logged in app users 
                    "email": login.username,
                    "firstName": vm.logindocument.FirstName,
                    "lastName": vm.logindocument.LastName
                },{ 
                //Account Fields 
                    "id": tenant.Identifier, //Required 
                    "name": tenant.DisplayName,
                    "tenant_ui_version": tenant.UIVersion,
                    "tenant_type": tenant.TenantType,
                    "tenant_status": tenant.Status
                });
            }
        };

        vm.clickLogout = function($event) {
            if ($event != null) {
                $event.preventDefault();
            }

            LoginService.Logout();
        };

        vm.clickToLP = function() {
            LoginStore.redirectToLP();
        };
    }
});