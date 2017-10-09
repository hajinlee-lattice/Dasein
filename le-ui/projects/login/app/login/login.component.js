angular.module('login')
.component('loginFrame', {
    templateUrl: 'app/login/login.component.html',
    controller: function(
        $scope, $state, $timeout, ResourceUtility, LoginService, 
        SessionTimeoutUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            ClientSession = resolve.ClientSession,
            LoginDocument = resolve.LoginDocument;
        
        angular.extend(vm, {
            ResourceUtility: ResourceUtility,
            login: LoginStore.login,
            state: $state,
            history: [],
            loginInProgress: {}
        });

        vm.init = function() {
            if (SessionTimeoutUtility.hasSessionTimedOut() && LoginDocument.UserName) {
                return LoginService.Logout();
            } else {
                switch($state.current.name) {
                    case 'login.form': 
                        if (LoginDocument.UserName) {
                            $state.go('login.tenants');
                        }
                        break;
                    case 'login.tenants':
                        if (!LoginDocument.UserName) {
                            $state.go('login.form');
                        }
                        break;
                }
            }

            LoginStore.set(LoginDocument, ClientSession);

            $timeout(function() {
                angular.element('body').addClass('initialized');
            },1);
        }

        vm.getHistory = function(username) {
            vm.history = BrowserStorageUtility.getHistory(username, vm.login.tenant) || [];

            return vm.history;
        }

        vm.clearHistory = function(username) {
            BrowserStorageUtility.clearHistory(username);
        }

        vm.clickTenant = function(tenant, username) {
            vm.loginInProgress[tenant.DisplayName] = true;

            LoginService.GetSessionDocument(tenant, username).then(function(data) {
                vm.loginInProgress[tenant.DisplayName] = false;
                
                if (data != null && data.Success === true) {
                    LoginStore.redirectToLP(tenant);
                } else {
                    showError(ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR"));
                }
            });
        }

        vm.clickLogout = function($event) {
            if ($event != null) {
                $event.preventDefault();
            }

            LoginService.Logout();
        }

        vm.clickToLP = function() {
            LoginStore.redirectToLP();
        }

        vm.init();
    }
});