angular
.module('lp.marketo')
.service('MarketoStore', function($http, $q, $state, MarketoService) {
    var MarketoStore = this;
    this.init = function() {
        this.marketoCredentialId = null;
        this.marketoCredential = {};
        this.marketoMatchFields = {};
        this.activeModels = [];
        this.useMarketoLatticeIntegration = null;
        this.primaryAttributeFields = null;
        this.scoringRequestList = null;
        this.scoringRequest = null;
        this.configId = null;

    }

    this.init();

    this.clear = function() {
        this.init();
    }

    this.setUseMarketoLatticeIntegration = function(bool) {
        this.useMarketoLatticeIntegration = bool;
    }

    this.getUseMarketoLatticeIntegration = function(bool) {
        return this.useMarketoLatticeIntegration != null && this.useMarketoLatticeIntegration == true;
    }

    this.setActiveModels = function(models) {
        this.activeModels = models;
    }

    this.getActiveModels = function() {
        return this.activeModels;
    }

    this.setMarketoCredential = function(credential) {
        this.marketoCredential = credential;
    }

    this.getMarketoCredential = function(credentialId) {
        var deferred = $q.defer();

        if (this.marketoCredential != null && this.marketoCredentialId == credentialId) {
            deferred.resolve(this.marketoCredential);
        } else {
            MarketoService.GetMarketoCredentials(credentialId).then( function(credential) {
                MarketoStore.marketoCredential = credential;
                MarketoStore.marketoCredentialId = credential.credential_id;
                deferred.resolve(credential);
            });
        }

        return deferred.promise;
    }

    this.setScoringRequestList = function(scoringRequestList) {
        this.scoringRequestList = scoringRequestList;
    }

    this.getScoringRequestList = function(credentialId, cacheOnly) {
        if (credentialId == this.marketoCredentialId && this.scoringRequestList && cacheOnly) {
            return this.scoringRequestList;
        } else {
            var deferred = $q.defer();

            MarketoService.GetMarketoScoringRequestSummaries(credentialId).then(function(result) {
                console.log(result);
                MarketoStore.setScoringRequestList(result);
                deferred.resolve(result);
            });

            return deferred.promise;
        }
    }

    this.setScoringRequest = function(scoringRequest) {
        this.scoringRequest = scoringRequest;
        this.configId = scoringRequest.configId ? scoringRequest.configId : null;
    }

    this.updateScoringRequestMatchFields = function(scoringRequest) {
        if (this.scoringRequest) {
            this.scoringRequest.marketoScoringMatchFields = scoringRequest.marketoScoringMatchFields;
        }
    }

    this.getScoringRequest = function(cacheOnly, credentialId, configId) {
        var deferred = $q.defer();

        if (credentialId == this.marketoCredentialId && configId == this.configId && this.scoringRequest && cacheOnly) {
            deferred.resolve(this.scoringRequest);
        } else {
            MarketoService.GetScoringRequest(credentialId, configId).then(function(result) {
                console.log(result);
                MarketoStore.setScoringRequest(result);
                deferred.resolve(result);
            });

        }

        return deferred.promise;
    }

    this.setPrimaryAttributeFields = function(fields) {
        this.primaryAttributeFields = fields;
    }

    this.getPrimaryAttributeFields = function() {
        var deferred = $q.defer();

        if (this.primaryAttributeFields != null) {
            deferred.resolve(this.primaryAttributeFields);
        } else {
            MarketoService.GetPrimaryAttributeFields().then( function(primaryFields) {
                MarketoStore.setPrimaryAttributeFields(primaryFields);
                deferred.resolve(primaryFields);
            });
        }

        return deferred.promise;
    }

})
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
                var result = response.data;
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
                var result = response.data;
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
            rest_client_secret: credential.restClientSecret,
            lattice_secret_key: credential.latticeSecretKey || null
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
                    }
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
                var result = {
                        data: response.data,
                        success: true
                    }
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
                rest_client_secret: credential.restClientSecret,
                lattice_secret_key: credential.latticeSecretKey || null
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
                    }
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }

        )

        return deferred.promise;
    }

    // this.GetMarketoMatchFields = function(credential) {
    //     var deferred = $q.defer();
    //     var result;

    //     $http({
    //         method: 'GET',
    //         url: '/pls/marketo/credentials/matchfields',
    //         params: {
    //             marketoSoapEndpoint: credential.soap_endpoint,
    //             marketoSoapUserId: credential.soap_user_id,
    //             marketoSoapEncryptionKey: credential.soap_encryption_key
    //         },
    //         headers: {
    //             'Accept': 'application/json'
    //         }
    //     }).then(
    //         function onSuccess(response) {
    //             var result = response.data;
    //             deferred.resolve(result);
    //         }, function onError(response) {
    //             if (!response.data) {
    //                 response.data = {};
    //             }

    //             var errorMsg = response.data.errorMsg || 'unspecified error';
    //             deferred.resolve(errorMsg);
    //         }
    //     );

    //     return deferred.promise;
    // }

    this.GetActiveModels = function() {
        var deferred = $q.defer();
        var result;
        var url = '/pls/scoringapi-internal/models';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
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
                    }
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
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
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        )

        return deferred.promise;
    }

    this.GetMarketoScoringRequestSummaries = function(credential) {
        var deferred = $q.defer(),
            url = '/pls/marketo/credentials/' + credential + '/scoring-requests';

        $http({
            method: 'GET',
            url: url
        }).then(
            function onSuccess(response) {
                console.log(response);
                deferred.resolve(response.data);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        )

        return deferred.promise;
    }

    this.GetPrimaryAttributeFields = function() {
        var deferred = $q.defer(),
            url = "/pls/primary-attributes/primaryfield-configuration"

        $http({
            method: 'GET',
            url: url
        }).then(
            function onSuccess(response) {
                console.log(response);
                deferred.resolve(response.data);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        )

        return deferred.promise;
    }

    this.GetScoringFields = function(modelId) {
        var deferred = $q.defer(),
            url = "/pls/scoringapi-internal/models/" + modelId + "/fields";

        $http({
            method: 'GET',
            url: url
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        )


        return deferred.promise;
    }

    this.CreateScoringRequest = function(credentialId, scoringRequest) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/marketo/credentials/' + credentialId + '/scoring-requests',
            data: scoringRequest,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
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

    this.UpdateScoringRequest = function(credentialId, scoringRequest) {
        var deferred = $q.defer();

        $http({
            method: 'PUT',
            url: '/pls/marketo/credentials/' + credentialId + '/scoring-requests/' + scoringRequest.configId,
            data: scoringRequest
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
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

    this.GetScoringRequest = function(credentialId, configId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/marketo/credentials/' + credentialId + '/scoring-requests/' + configId
        }).then(
            function onSuccess(response) {
                deferred.resolve(response.data);
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

});
