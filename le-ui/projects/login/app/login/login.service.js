angular.module('login')
.service('LoginStore', function(TimestampIntervalUtility, BrowserStorageUtility) {
    this.login = {
        username: '',
        expires: null,
        expireDays: 0,
        tenant: ''
    };

    this.set = function(LoginDocument, ClientSession) {
        this.login.username = LoginDocument.UserName;
        this.login.expires = LoginDocument.PasswordLastModified;
        this.login.expireDays = Math.abs(TimestampIntervalUtility.getDays(LoginDocument.PasswordLastModified) - 90);

        if (ClientSession && ClientSession.Tenant) {
            this.login.tenant = ClientSession.Tenant.DisplayName;
        }
    }

    this.redirectToLP = function (Tenant) {
        if (!Tenant) {
            var ClientSession = BrowserStorageUtility.getClientSession();
            var Tenant = ClientSession.Tenant;
        }

        var UIVersion = Tenant.UIVersion || "2.0";
        var pathMap = {
            "3.0": "/lp/",
            "2.0": "/lp2/"
        };

        var previousSession = BrowserStorageUtility.getClientSession();

        return window.open(pathMap[UIVersion] || "/lp2", "_self");
    }
});