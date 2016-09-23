angular
.module('lp.marketo')
.service('MarketoService', function($http, $q, $state) {
    this.GetMarketoCredentials = function() {
        var deferred = $q.defer();
        var result;
        var url = '/pls/marketo/credentials/';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.GetMarketoCredentialById = function(credentialId) {
        var deferred = $q.defer(),
            result,
            url = '/pls/marketo/credentials/' + credentialId;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.CreateMarketoCredential = function(credential) {
        var deferred = $q.defer(),
            data = {
                name: credential.credentialName,
                soap_endpoint: credential.soapEndpoint,
                soap_user_id: credential.soapUserId,
                soap_encryption_key: credential.soapEncryptionKey,
                rest_endpoint: credential.restEndpoint,
                rest_identity_endpoint: credential.restIdentityEndpoint,
                rest_client_id: credential.restClientId,
                rest_client_secret: credential.restClientSecret
            };
        $http({
            method: 'POST',
            url: '/pls/marketo/credentials/',
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
                $state.go('home.marketosettings.apikey');
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        )

        return deferred.promise;
    }

    this.DeleteMarketoCredential = function(credentialId) {
        var deferred = $q.defer(),
            result,
            url = '/pls/marketo/credentials/' + credentialId;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.UpdateMarketoCredential = function(credentialId, marketoCredential) {
        var deferred = $q.defer(),
            data = {
                soapEndpoint: credential.soapEndpoint,
                soapUserId: credential.soapUserId,
                soapEncryptionKey: credential.soapEncryptionKey,
                restEndpoint: credential.restEndpoint,
                restIdentityEndpoint: credential.restIdentityEndpoint,
                restClientId: credential.restClientId,
                restClientSecret: credential.restClientSecret
            };

        $http({
            method: 'PUT',
            url: '/pls/marketo/credentials/' + credentialId,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        )

        return deferred.promise;
    }

    this.UpdateEnrichmentFields = function(credentialId, marketoMatchFields) {
        var deferred = $q.defer(),
            data = marketoMatchFields;

        $http({
            method: 'PUT',
            url: '/pls/marketo/credentials/' + credentialId + '/enrichment',
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        )

        return deferred.promise;
    }

});
