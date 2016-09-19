angular.module('login.tenants', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.SessionTimeout'
])
.controller('TenantSelectController', function (
    $state, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, 
    LoginService, LoginStore, LoginDocument, TenantList, SessionTimeoutUtility
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        tenantList: TenantList,
        isLoggedInWithTempPassword: LoginDocument.MustChangePassword,
        isPasswordOlderThanNinetyDays: TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(LoginDocument.PasswordLastModified),
        SortProperty: 'RegisteredTime',
        SortDirection: '-',
        deactivated: false,
        selected: null,
        visible: false,
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

        SessionTimeoutUtility.init();

        vm.visible = true;
        
        $('[autofocus]').focus();
    }


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
});