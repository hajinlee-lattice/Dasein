angular.module('login.tenants', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.SessionTimeout'
])
.component('loginTenants', {
    templateUrl: 'app/login/tenants/tenants.component.html',
    controller: function (
        $scope, $state, $timeout, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, 
        LoginService, LoginStore, SessionTimeoutUtility
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            TenantList = resolve.TenantList,
            LoginDocument = resolve.LoginDocument;

        angular.extend(vm, {
            ResourceUtility: ResourceUtility,
            tenantList: TenantList,
            tenantMap: {}, 
            isLoggedInWithTempPassword: LoginDocument.MustChangePassword,
            isPasswordOlderThanNinetyDays: TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(LoginDocument.PasswordLastModified),
            SortProperty: 'RegisteredTime',
            SortDirection: '-',
            deactivated: false,
            selected: null,
            visible: true,
            initialize: false,
            version: '3.0'
        });

        vm.init = function() {
            var ClientSession = BrowserStorageUtility.getClientSession();
            
            LoginStore.set(LoginDocument, ClientSession);
            
            if (vm.isLoggedInWithTempPassword || vm.isPasswordOlderThanNinetyDays) {
                $state.go('login.update');
                return;
            }
            
            if (TenantList.length == 1) {
                vm.select(TenantList[0]);
                return;
            }

            if (TenantList == null || TenantList.length === 0) {
                if (LoginDocument && !LoginStore.login.username) {
                    showError(ResourceUtility.getString("LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS"));
                } else {
                    showError(ResourceUtility.getString("NO_TENANT_MESSAGE"));
                }
                return;
            }

            vm.tenantList.forEach(function(tenant) {
                vm.tenantMap[tenant.Identifier] = tenant;
            });

            SessionTimeoutUtility.init();
            
            vm.initialize = true;

            $(document.body).click(function(event) { vm.focus(); });

            vm.focus();
        };

        vm.select = function (tenant) {
            vm.deactivated = true;
            vm.selected = tenant;

            LoginService.GetSessionDocument(tenant).then(function(data) {
                if (data != null && data.Success === true) {
                    LoginStore.redirectToLP(tenant);
                } else {
                    vm.deactivated = false;
                    vm.selected = null;
                    showError(ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR"));
                }
            });
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

            switch ($event.keyCode) {
                case 38: // up
                    if (selected.length == 0) {
                        $(all[0]).addClass('active');
                    } else {
                        var index = all.indexOf(selected[0]);
                        var n = index == 0 ? all.length - 1 : index - 1;
                    }

                    break;

                case 40: // down
                    if (selected.length == 0) {
                        $(all[0]).addClass('active');
                    } else {
                        var index = all.indexOf(selected[0]);
                        var n = index + 1 >= all.length ? 0 : index + 1;
                    }

                    break;

                case 13: // enter
                    if (selected && selected.length > 0) {
                        var tenant = vm.tenantMap[selected[0].id];
                        console.log('enter', tenant, selected);
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
        }

        vm.hover = function() {
            // convert html collection to array
            $('.tenant-list-item.active').removeClass('active');
        }

        function showError(message) {
            if (message == null) {
                return;
            }

            if (message.indexOf("Global Auth") > -1) {
                message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
            }

            vm.tenantErrorMessage = message;
            vm.showTenantError = true;
        };

        vm.init();
    }
});