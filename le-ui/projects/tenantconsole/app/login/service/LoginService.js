var app = angular.module("app.login.service.LoginService",
    ['le.common.util.BrowserStorageUtility']
);

app.service('LoginService', function($q, $http, BrowserStorageUtility){
    this.Login = function (username, password) {
        var deferred = $q.defer();

        var result = {
            success: false,
            resultObj: null,
            errorMsg: "Authentication failed."
        };

        $http({
            method: 'POST',
            url: '/admin/adlogin',
            data: { Username: username, Password: password },
            headers: { "Content-Type": "application/json" }
        })
        .success(function(data, status, headers, config) {
            if (data !== null && data !== "") {
                result.success = true;
                result.resultObj = data;
                BrowserStorageUtility.setTokenDocument(data.Token);
                BrowserStorageUtility.setLoginDocument({ Token: data.Token, Principal: data.Principal });
                BrowserStorageUtility.setSessionDocument({ Token: data.Token });
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            if (data.errorCode !== 'LEDP_18001') {
                result.errorMsg = data.errorMsg;
            }

            deferred.resolve(result);
        });

        return deferred.promise;
    };

});


