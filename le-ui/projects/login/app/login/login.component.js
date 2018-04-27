angular.module('login')
.component('loginFrame', {
    templateUrl: 'app/login/login.component.html',
    bindings: {
        logindocument: '<',
        clientsession: '<'
    },
    controller: function(
        $state, $timeout, ResourceUtility, LoginService, 
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
                    case 'login.form': 
                        if (vm.logindocument.UserName) {
                            $state.go('login.tenants');
                        }
                        break;
                    case 'login.tenants':
                        if (!vm.logindocument.UserName) {
                            $state.go('login.form');
                        }
                        break;
                    case 'login':
                        $state.go('login.form');
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
                    LoginStore.redirectToLP(tenant);
                } else {
                    vm.loginInProgress[tenant.DisplayName] = false;
                    showError(ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR"));
                }
            });
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