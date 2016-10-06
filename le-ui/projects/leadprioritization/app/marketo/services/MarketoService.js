angular
.module('lp.marketo')
.service('MarketoService', function($http, $q, $state) {
    this.GetMarketoCredentials = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/marketo/credentials/' + id;

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
                var result = {
                    data: response.data,
                    success: true
                };
                
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.DeleteMarketoCredential = function(credentialId) {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/marketo/credentials/' + credentialId;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = {
                    data: response.data,
                    success: true
                };
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

    this.UpdateMarketoCredential = function(credentialId, credential) {
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
            method: 'PUT',
            url: '/pls/marketo/credentials/' + credentialId,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
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

    this.GetMarketoMatchFields = function(credential) {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/marketo/credentials/matchfields';

        $http({
            method: 'GET',
            url: url,
            params: {
                'marketoSoapEndpoint': credential.soap_endpoint,
                'marketoSoapUserId': credential.soap_user_id,
                'marketoSoapEncryptionKey': credential.soap_encryption_key
            },
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var result = {
                    errorMsg: response.data.errorMsg || 'unspecified error',
                    success: false
                };

                deferred.reject(result);
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

    this.GetMarketoMatchFields = function(credential) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/marketo/credentials/matchfields',
            params: {
                marketoSoapEndpoint: credential.soap_endpoint,
                marketoSoapUserId: credential.soap_user_id,
                marketoSoapEncryptionKey: credential.soap_encryption_key
            },
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

});
