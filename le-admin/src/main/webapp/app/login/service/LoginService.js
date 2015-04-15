var app = angular.module("app.login.service.LoginService",
    ['le.common.util.BrowserStorageUtility']
);

app.service('LoginService', function($q, $http, BrowserStorageUtility){

    this.Attach = function () {
        var deferred = $q.defer();

        var result = {
            success: false,
            errorMsg: "Attaching to global admin tenant failed."
        };

        var tenant = BrowserStorageUtility.getGlobalAdminTenantDocument();
        delete tenant.Timestamp;
        if (tenant === null) {
            return deferred.resolve(result);
        }

        $http({
            method: 'POST',
            url: '/admin/attach',
            data: tenant,
            headers: { "Content-Type": "application/json" }
        })
        .success(function(data, status, headers, config) {
            if (data !== null && data.Success === true) {
                result.success = true;
                BrowserStorageUtility.setClientSession(data.Result.User);
            } else {
                //SessionService.HandleResponseErrors(data, status);
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            //SessionService.HandleResponseErrors(data, status);
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.Login = function (username, password) {
        var deferred = $q.defer();

        var passwordHash = CryptoJS.SHA256(password);

        var result = {
            success: false,
            resultObj: null,
            errorMsg: "Authentication failed."
        };

        $http({
            method: 'POST',
            url: '/admin/login',
            data: { Username: username, Password: passwordHash.toString() },
            headers: { "Content-Type": "application/json" }
        })
        .success(function(data, status, headers, config) {
            if (data !== null && data !== "") {
                result.success = data.Success;
                result.resultObj = data;
                BrowserStorageUtility.setTokenDocument(data.Uniqueness + "." + data.Randomness);
                data.Result.UserName = username;
                BrowserStorageUtility.setLoginDocument(data.Result);
                // var tenant = _.findWhere(data.Result.Tenants, {Identifier: "GLOBAL_ADMIN_TENANT"});
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            if (data.errorCode !== 'LEDP_18001')
                result.errorMsg = data.errorMsg;

            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.RetrieveGlobalAdminTenant = function() {
        var deferred = $q.defer();

        var result = {
            success: false,
            errorMsg: "Failed to retrieve global admin tenant."
        };

        $http({
            method: 'GET',
            url: '/admin/globaladmintenant'
        })
            .success(function(data, status, headers, config) {
                if (data !== null && data !== "") {
                    BrowserStorageUtility.setGlobalAdminTenantDocument(data);
                    result.success = true;
                }
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                deferred.resolve(result);
            });

        return deferred.promise;

    };

});


