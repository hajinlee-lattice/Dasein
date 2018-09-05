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
            url = "/score/" + modelId + "/fields"

        $http({
            method: 'GET',
            url: url
        }).then(
            function onSuccess(response) {
                console.log(response);
                // deferred.resolve(response.data);
                var test = {"modelId":"ms__a9a37730-65ab-44b9-9d97-dbbc60c9e359-PLSModel","fields":[{"fieldName":"Interest_esb__c","fieldType":"BOOLEAN","displayName":"Interest_esb__c","isPrimary":false},{"fieldName":"kickboxDisposable","fieldType":"BOOLEAN","displayName":"kickboxDisposable","isPrimary":false},{"fieldName":"Unsubscribed","fieldType":"BOOLEAN","displayName":"Unsubscribed","isPrimary":false},{"fieldName":"Email","fieldType":"STRING","displayName":"Email","isPrimary":true},{"fieldName":"Activity_Count_Interesting_Moment_Email","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Email","isPrimary":false},{"fieldName":"State","fieldType":"STRING","displayName":"State","isPrimary":true},{"fieldName":"CreatedDate","fieldType":"STRING","displayName":"CreatedDate","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_key_web_page","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_key_web_page","isPrimary":false},{"fieldName":"Activity_Count_Unsubscribe_Email","fieldType":"FLOAT","displayName":"Activity_Count_Unsubscribe_Email","isPrimary":false},{"fieldName":"Free_Email_Address__c","fieldType":"BOOLEAN","displayName":"Free_Email_Address__c","isPrimary":false},{"fieldName":"Id","fieldType":"STRING","displayName":"Id","isPrimary":true},{"fieldName":"Source_Detail__c","fieldType":"STRING","displayName":"Source_Detail__c","isPrimary":false},{"fieldName":"Activity_Count_Visit_Webpage","fieldType":"FLOAT","displayName":"Activity_Count_Visit_Webpage","isPrimary":false},{"fieldName":"Cloud_Plan__c","fieldType":"STRING","displayName":"Cloud_Plan__c","isPrimary":false},{"fieldName":"HasCEDownload","fieldType":"BOOLEAN","displayName":"HasCEDownload","isPrimary":false},{"fieldName":"CompanyName","fieldType":"STRING","displayName":"CompanyName","isPrimary":true},{"fieldName":"HasAnypointLogin","fieldType":"BOOLEAN","displayName":"HasAnypointLogin","isPrimary":false},{"fieldName":"Interest_tcat__c","fieldType":"BOOLEAN","displayName":"Interest_tcat__c","isPrimary":false},{"fieldName":"City","fieldType":"STRING","displayName":"City","isPrimary":true},{"fieldName":"Activity_Count_Interesting_Moment_Event","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Event","isPrimary":false},{"fieldName":"Activity_Count_Click_Email","fieldType":"FLOAT","displayName":"Activity_Count_Click_Email","isPrimary":false},{"fieldName":"Activity_Count_Email_Bounced_Soft","fieldType":"FLOAT","displayName":"Activity_Count_Email_Bounced_Soft","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Pricing","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Pricing","isPrimary":false},{"fieldName":"Activity_Count_Click_Link","fieldType":"FLOAT","displayName":"Activity_Count_Click_Link","isPrimary":false},{"fieldName":"Activity_Count_Fill_Out_Form","fieldType":"FLOAT","displayName":"Activity_Count_Fill_Out_Form","isPrimary":false},{"fieldName":"kickboxFree","fieldType":"BOOLEAN","displayName":"kickboxFree","isPrimary":false},{"fieldName":"HasEEDownload","fieldType":"BOOLEAN","displayName":"HasEEDownload","isPrimary":false},{"fieldName":"FirstName","fieldType":"STRING","displayName":"FirstName","isPrimary":false},{"fieldName":"PhoneNumber","fieldType":"STRING","displayName":"PhoneNumber","isPrimary":true},{"fieldName":"kickboxAcceptAll","fieldType":"BOOLEAN","displayName":"kickboxAcceptAll","isPrimary":false},{"fieldName":"Industry","fieldType":"STRING","displayName":"Industry","isPrimary":false},{"fieldName":"kickboxStatus","fieldType":"STRING","displayName":"kickboxStatus","isPrimary":false},{"fieldName":"LastName","fieldType":"STRING","displayName":"LastName","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Any","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Any","isPrimary":false},{"fieldName":"Title","fieldType":"STRING","displayName":"Title","isPrimary":false},{"fieldName":"Country","fieldType":"STRING","displayName":"Country","isPrimary":true},{"fieldName":"SICCode","fieldType":"STRING","displayName":"SICCode","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Webinar","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Webinar","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Search","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Search","isPrimary":false},{"fieldName":"Lead_Source_Asset__c","fieldType":"STRING","displayName":"Lead_Source_Asset__c","isPrimary":false},{"fieldName":"Activity_Count_Open_Email","fieldType":"FLOAT","displayName":"Activity_Count_Open_Email","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Multiple","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Multiple","isPrimary":false}],"validationExpression":"( Email || Website || CompanyName || DUNS ) && Id "};
                // var test = {"primaryFields":[{"fieldName":"Id","fieldType":"STRING","displayName":"Id","externalSystemName":null},{"fieldName":"Email","fieldType":"STRING","displayName":"Email","externalSystemName":null},{"fieldName":"City","fieldType":"STRING","displayName":"City","externalSystemName":null},{"fieldName":"State","fieldType":"STRING","displayName":"State","externalSystemName":null},{"fieldName":"PostalCode","fieldType":"STRING","displayName":"PostalCode","externalSystemName":null},{"fieldName":"Country","fieldType":"STRING","displayName":"Country","externalSystemName":null},{"fieldName":"PhoneNumber","fieldType":"STRING","displayName":"PhoneNumber","externalSystemName":null},{"fieldName":"Website","fieldType":"STRING","displayName":"Website","externalSystemName":null},{"fieldName":"CompanyName","fieldType":"STRING","displayName":"CompanyName","externalSystemName":null},{"fieldName":"DUNS","fieldType":"STRING","displayName":"DUNS","externalSystemName":null}],"validationExpression":{"expression":"( Email || Website || CompanyName || DUNS ) && Id "}};
                // return test;
                deferred.resolve(test);
            }, function onError(response) {
                var test = {"modelId":"ms__a9a37730-65ab-44b9-9d97-dbbc60c9e359-PLSModel","fields":[{"fieldName":"Interest_esb__c","fieldType":"BOOLEAN","displayName":"Interest_esb__c","isPrimary":false},{"fieldName":"kickboxDisposable","fieldType":"BOOLEAN","displayName":"kickboxDisposable","isPrimary":false},{"fieldName":"Unsubscribed","fieldType":"BOOLEAN","displayName":"Unsubscribed","isPrimary":false},{"fieldName":"Email","fieldType":"STRING","displayName":"Email","isPrimary":true},{"fieldName":"Activity_Count_Interesting_Moment_Email","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Email","isPrimary":false},{"fieldName":"State","fieldType":"STRING","displayName":"State","isPrimary":true},{"fieldName":"CreatedDate","fieldType":"STRING","displayName":"CreatedDate","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_key_web_page","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_key_web_page","isPrimary":false},{"fieldName":"Activity_Count_Unsubscribe_Email","fieldType":"FLOAT","displayName":"Activity_Count_Unsubscribe_Email","isPrimary":false},{"fieldName":"Free_Email_Address__c","fieldType":"BOOLEAN","displayName":"Free_Email_Address__c","isPrimary":false},{"fieldName":"Id","fieldType":"STRING","displayName":"Id","isPrimary":true},{"fieldName":"Source_Detail__c","fieldType":"STRING","displayName":"Source_Detail__c","isPrimary":false},{"fieldName":"Activity_Count_Visit_Webpage","fieldType":"FLOAT","displayName":"Activity_Count_Visit_Webpage","isPrimary":false},{"fieldName":"Cloud_Plan__c","fieldType":"STRING","displayName":"Cloud_Plan__c","isPrimary":false},{"fieldName":"HasCEDownload","fieldType":"BOOLEAN","displayName":"HasCEDownload","isPrimary":false},{"fieldName":"CompanyName","fieldType":"STRING","displayName":"CompanyName","isPrimary":true},{"fieldName":"HasAnypointLogin","fieldType":"BOOLEAN","displayName":"HasAnypointLogin","isPrimary":false},{"fieldName":"Interest_tcat__c","fieldType":"BOOLEAN","displayName":"Interest_tcat__c","isPrimary":false},{"fieldName":"City","fieldType":"STRING","displayName":"City","isPrimary":true},{"fieldName":"Activity_Count_Interesting_Moment_Event","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Event","isPrimary":false},{"fieldName":"Activity_Count_Click_Email","fieldType":"FLOAT","displayName":"Activity_Count_Click_Email","isPrimary":false},{"fieldName":"Activity_Count_Email_Bounced_Soft","fieldType":"FLOAT","displayName":"Activity_Count_Email_Bounced_Soft","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Pricing","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Pricing","isPrimary":false},{"fieldName":"Activity_Count_Click_Link","fieldType":"FLOAT","displayName":"Activity_Count_Click_Link","isPrimary":false},{"fieldName":"Activity_Count_Fill_Out_Form","fieldType":"FLOAT","displayName":"Activity_Count_Fill_Out_Form","isPrimary":false},{"fieldName":"kickboxFree","fieldType":"BOOLEAN","displayName":"kickboxFree","isPrimary":false},{"fieldName":"HasEEDownload","fieldType":"BOOLEAN","displayName":"HasEEDownload","isPrimary":false},{"fieldName":"FirstName","fieldType":"STRING","displayName":"FirstName","isPrimary":false},{"fieldName":"PhoneNumber","fieldType":"STRING","displayName":"PhoneNumber","isPrimary":true},{"fieldName":"kickboxAcceptAll","fieldType":"BOOLEAN","displayName":"kickboxAcceptAll","isPrimary":false},{"fieldName":"Industry","fieldType":"STRING","displayName":"Industry","isPrimary":false},{"fieldName":"kickboxStatus","fieldType":"STRING","displayName":"kickboxStatus","isPrimary":false},{"fieldName":"LastName","fieldType":"STRING","displayName":"LastName","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Any","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Any","isPrimary":false},{"fieldName":"Title","fieldType":"STRING","displayName":"Title","isPrimary":false},{"fieldName":"Country","fieldType":"STRING","displayName":"Country","isPrimary":true},{"fieldName":"SICCode","fieldType":"STRING","displayName":"SICCode","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Webinar","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Webinar","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Search","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Search","isPrimary":false},{"fieldName":"Lead_Source_Asset__c","fieldType":"STRING","displayName":"Lead_Source_Asset__c","isPrimary":false},{"fieldName":"Activity_Count_Open_Email","fieldType":"FLOAT","displayName":"Activity_Count_Open_Email","isPrimary":false},{"fieldName":"Activity_Count_Interesting_Moment_Multiple","fieldType":"FLOAT","displayName":"Activity_Count_Interesting_Moment_Multiple","isPrimary":false}],"validationExpression":"( Email || Website || CompanyName || DUNS ) && Id "};
                // var test = {"primaryFields":[{"fieldName":"Id","fieldType":"STRING","displayName":"Id","externalSystemName":null},{"fieldName":"Email","fieldType":"STRING","displayName":"Email","externalSystemName":null},{"fieldName":"City","fieldType":"STRING","displayName":"City","externalSystemName":null},{"fieldName":"State","fieldType":"STRING","displayName":"State","externalSystemName":null},{"fieldName":"PostalCode","fieldType":"STRING","displayName":"PostalCode","externalSystemName":null},{"fieldName":"Country","fieldType":"STRING","displayName":"Country","externalSystemName":null},{"fieldName":"PhoneNumber","fieldType":"STRING","displayName":"PhoneNumber","externalSystemName":null},{"fieldName":"Website","fieldType":"STRING","displayName":"Website","externalSystemName":null},{"fieldName":"CompanyName","fieldType":"STRING","displayName":"CompanyName","externalSystemName":null},{"fieldName":"DUNS","fieldType":"STRING","displayName":"DUNS","externalSystemName":null}],"validationExpression":{"expression":"( Email || Website || CompanyName || DUNS ) && Id "}};
                // return test;
                deferred.resolve(test);
                // if (!response.data) {
                //     response.data = {};
                // }

                // var errorMsg = response.data.errorMsg || 'unspecified error';
                // deferred.resolve(errorMsg);
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
            url: '/pls/marketo/credentials/' + credentialId + '/scoring-requests/' + scoringRequest.requestConfigId,
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

    this.GetScoringRequest = function(credentialId, modelId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/marketo/credentials/' + credentialId + '/scoring-requests/' + modelId
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
