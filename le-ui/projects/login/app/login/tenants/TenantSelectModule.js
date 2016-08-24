angular.module('login.tenants', [
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('TenantSelectController', function (
    $scope, $state, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, 
    LoginService, LoginStore, LoginDocument, TenantList
) {
    $('[autofocus]').focus();
    var ClientSession = BrowserStorageUtility.getClientSession();
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.tenantList = TenantList;
    $scope.isLoggedInWithTempPassword = LoginDocument.MustChangePassword;
    $scope.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(LoginDocument.PasswordLastModified);
    $scope.SortProperty = 'RegisteredTime';
    $scope.SortDirection = '-';
    $scope.deactivated = false;
    $scope.selected = null;

    LoginStore.set(LoginDocument, ClientSession);

    if ($scope.isLoggedInWithTempPassword || $scope.isPasswordOlderThanNinetyDays) {
        $state.go('login.update');
    }

    if (TenantList == null || TenantList.length === 0) {
        if (LoginDocument && !LoginStore.login.username) {
            showTenantHeaderMessage(ResourceUtility.getString("LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS"));
        } else {
            showTenantHeaderMessage(ResourceUtility.getString("NO_TENANT_MESSAGE"));
        }

        return;
    }

    $scope.handleTenantSelected = function (tenant) {
        $scope.deactivated = true;
        $scope.selected = tenant;

        LoginService.GetSessionDocument(tenant).then(function(data) {
            if (data != null && data.Success === true) {
                LoginStore.redirectToLP(tenant);
            } else {
                $scope.deactivated = false;
                $scope.selected = null;
                $scope.showTenantHeaderMessage(ResourceUtility.getString("TENANT_SELECTION_FORM_ERROR"));
            }
        });
    };

    if (TenantList.length == 1) {
        $scope.handleTenantSelected(TenantList[0]);
    }

    $scope.sort = function(value) {
        $scope.SortProperty = value;
        $scope.SortDirection = ($scope.SortDirection == '' ? '-' : '');
    };

    function showTenantHeaderMessage(message) {
        if (message == null) {
            return;
        }

        if (message.indexOf("Global Auth") > -1) {
            message = ResourceUtility.getString("LOGIN_GLOBAL_AUTH_ERROR");
        }

        $scope.tenantErrorMessage = message;
        $scope.showTenantError = true;
    };
});