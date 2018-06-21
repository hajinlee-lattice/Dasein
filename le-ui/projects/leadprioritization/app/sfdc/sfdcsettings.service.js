angular.module('lp.sfdc', [])
.service('SfdcStore', function(
    $q, $state, $stateParams,  $rootScope, SfdcService
){
    var SfdcStore = this;
    
    this.init = function() {
        this.accountids = [];
        this.orgs = [];
    }

    this.init();
    
    this.clear = function() {
        this.init();
    }

    this.getAccountIds = function() {
        var deferred = $q.defer();

        SfdcService.getAccountIds().then(function(data) {
            SfdcStore.setAccountIds(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }
    this.setAccountIds = function(accountids) {
        this.accountids = accountids;
    }

    this.getOrgs = function() {
        var deferred = $q.defer();

        SfdcService.getOrgs().then(function(data) {
            SfdcStore.setOrgs(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }
    this.setOrgs = function(orgs) {
        this.orgs = orgs;
    }

})
.service('SfdcService', function($q, $http, $state) {

    this.generateAuthToken = function(emailAddress, tenantId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/bisaccesstoken',
            params: {
                username: emailAddress,
                tenantId: tenantId
            },
            headers: {
                'Content-Type': 'application/json'
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

    this.getAccountIds = function() {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/lookup-id-mapping/available-lookup-ids?externalSystemType=CRM'
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

    this.getOrgs = function() {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/lookup-id-mapping/'
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

    this.saveOrgs = function(configid, org){
        var deferred = $q.defer();

        $http({
            method: 'PUT',
            url: '/pls/lookup-id-mapping/config/' + configid,
            data: org
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
