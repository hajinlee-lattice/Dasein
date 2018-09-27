angular.module('login')
.component('loginFrame', {
    templateUrl: 'app/login/login.component.html',
    bindings: {
        logindocument: '<',
        clientsession: '<'
    },
    controller: function(
        $state, $location, $timeout, ResourceUtility, LoginService, Banner,
        SessionTimeoutUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this;

        vm.$onInit = function() {
            vm.ResourceUtility = ResourceUtility;
            vm.login = LoginStore.login;
            vm.state = $state;
            vm.history = [];
            vm.loginInProgress = {};
            
            if (SessionTimeoutUtility.hasSessionTimedOut() && vm.logindocument.UserName) {
                return LoginService.Logout();
            } else {
                switch($state.current.name) {
                    case 'login.logout':
                        if (vm.logindocument.UserName) {
                            LoginService.Logout();
                        }
                        break;
                    case 'login.form': 
                        if (vm.logindocument.UserName) {
                            var params = $location.$$search;
                            $state.go('login.tenants', { obj: params });
                        }
                        break;
                    case 'login.tenants':
                        if (!vm.logindocument.UserName) {
                            var params = $location.$$search;
                            $state.go('login.form', { obj: params });
                        }
                        break;
                    case 'login':
                        var params = $location.$$search;
                        $state.go('login.form', { obj: params });
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