angular.module('login')
.service('LoginStore', function(TimestampIntervalUtility, BrowserStorageUtility) {
    this.login = {
        username: '',
        expires: null,
        alreadyExpired: false,
        expireDays: 0,
        tenant: ''
    };

    this.set = function(LoginDocument, ClientSession) {
        this.login.username = LoginDocument.UserName;
        this.login.expires = TimestampIntervalUtility.getDateNinetyDaysAway(LoginDocument.PasswordLastModified);
        this.login.alreadyExpired = TimestampIntervalUtility.getDays(LoginDocument.PasswordLastModified) >= 90;
        this.login.expireDays = Math.abs(TimestampIntervalUtility.getDays(LoginDocument.PasswordLastModified) - 90);

        if (ClientSession && ClientSession.Tenant) {
            this.login.tenant = ClientSession.Tenant.DisplayName;
        }
    }

    this.redirectToLP = function(Tenant) {
        var pathMap = {
                "4.0": "/atlas/",
                "3.0": "/lpi/",
                "2.0": "/lp2/"
            },
            ClientSession, UIVersion;

        if (!Tenant) {
            ClientSession = BrowserStorageUtility.getClientSession();
            Tenant = ClientSession.Tenant;
        }

        UIVersion = Tenant.UIVersion || "2.0";

        return window.open(pathMap[UIVersion] || "/lp2", "_self");
    }
});