angular.module('login.tenants', [
    'lp.navigation.pagination',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.SessionTimeout'
])
.component('loginTenants', {
    templateUrl: 'app/login/tenants/tenants.component.html',
    bindings: {
        tenantlist: '<',
        logindocument: '<'
    },
    controller: function (
        $state, $location, $timeout, $stateParams, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, 
        LoginService, LoginStore, SessionTimeoutUtility, Banner
    ) {
        var vm = this;

        vm.pagesize = 250;
        vm.current = 1;

        vm.$onInit = function() {

            vm.ResourceUtility = ResourceUtility;
            vm.tenantMap = {};
            vm.isLoggedInWithTempPassword = vm.logindocument.MustChangePassword;
            vm.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(vm.logindocument.PasswordLastModified);
            vm.SortProperty = 'RegisteredTime';
            vm.SortDirection = '-';
            vm.deactivated = false;
            vm.selected = null;
            vm.visible = true;
            vm.initialize = false;
            vm.version = ''; // '' defaults it to all, it could also be '4.0', '3.0'

            var ClientSession = BrowserStorageUtility.getClientSession();
            
            LoginStore.set(vm.logindocument, ClientSession);
            
            if (vm.isLoggedInWithTempPassword || vm.isPasswordOlderThanNinetyDays) {
                $state.go('login.update');
                return;
            }
            
            if (vm.tenantlist.length == 1) {
                vm.select(vm.tenantlist[0]);
                return;
            }

            if (vm.tenantlist == null || vm.tenantlist.length === 0) {
                if (vm.logindocument && !LoginStore.login.username) {
                    showError(ResourceUtility.getString("LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS"));
                } else {
                    // showError(ResourceUtility.getString("NO_TENANT_MESSAGE"));
                    $window.location.href = 'https://help.lattice-engines.com';
                }
                return;
            }

            vm.tenantlist.forEach(function(tenant) {
                vm.tenantMap[tenant.Identifier] = tenant;
            });

            SessionTimeoutUtility.init();
            
            vm.initialize = true;

            $(document.body).click(function(event) { vm.focus(); });

            vm.focus();
        };

        vm.select = function (tenant) {
            Banner.reset();
            
            vm.deactivated = true;
            vm.selected = tenant;
            
            LoginService.GetSessionDocument(tenant, LoginStore.login.username).then(function(data) {
                if (data != null && data.Success === true) {
                    vm.aptrinsic(LoginStore.login, tenant);
                    LoginStore.redirectToLP(tenant);
                } else {
                    vm.deactivated = false;
                    vm.selected = null;
                    showError(ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR"));
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

        vm.sort = function(value) {
            if (vm.SortProperty == value) {
                vm.SortDirection = (vm.SortDirection == '' ? '-' : '');
            } else {
                vm.SortDirection = '';
            }

            vm.SortProperty = value;
        };

        vm.toggle = function() {
            vm.visible = !vm.visible
            
            vm.SearchValue = '';

            vm.focus();
        };

        vm.focus = function (tenant) {
            setTimeout(function() {
                $('[autofocus]').focus();
            }, 100);
        };

        vm.keyDown = function ($event) {
            // convert html collection to array
            var all = [].slice.call($('.tenant-list-item'));
            var selected = [].slice.call($('.tenant-list-item.active,.tenant-list-item:hover'));
            var n, index;

            switch ($event.keyCode) {
                case 38: // up
                    if (selected.length === 0) {
                        $(all[0]).addClass('active');
                    } else {
                        index = all.indexOf(selected[0]);
                        n = index === 0 ? all.length - 1 : index - 1;
                    }

                    break;

                case 40: // down
                    if (selected.length === 0) {
                        $(all[0]).addClass('active');
                    } else {
                        index = all.indexOf(selected[0]);
                        n = index + 1 >= all.length ? 0 : index + 1;
                    }

                    break;

                case 13: // enter
                    if (selected && selected.length > 0) {
                        var tenant = vm.tenantMap[selected[0].id];

                        if (tenant) {
                            vm.select(tenant);
                        }
                    }

                    break;
                    
                default:
                    angular.element('div.tenant-list-item').removeClass('active');
            }
            
            if (typeof n == 'number' && n > -1) {
                $(all[n]).addClass('active');
                $(all[index]).removeClass('active');
            }

            $timeout(function() {
                var filteredItems = angular.element('div.tenant-list-item');

                if (filteredItems.length == 1) {
                    filteredItems.addClass('active');
                } else {}
            },1);
        };

        vm.hover = function() {
            // convert html collection to array
            $('.tenant-list-item.active').removeClass('active');
        };

        function showError(message) {
            if (message === null) {
                return;
            }

            if (message.indexOf("Global Auth") > -1) {
                message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
            }

            Banner.error({
                message: message
            });
        };
    }
});