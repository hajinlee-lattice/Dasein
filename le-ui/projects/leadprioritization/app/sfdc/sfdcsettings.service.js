angular.module('lp.sfdc', [])
.service('sfdcstore', function(
    $q, $state, $stateParams,  $rootScope, sfdcservice
){
    var sfdcstore = this;
    
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

        sfdcservice.getAccountIds().then(function(data) {
            sfdcstore.setAccountIds(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }
    this.setAccountIds = function(accountids) {
        this.accountids = accountids;
    }

    this.getOrgs = function() {
        var deferred = $q.defer();

        sfdcservice.getOrgs().then(function(data) {
            sfdcstore.setOrgs(data);
            deferred.resolve(data);
        });

        return deferred.promise;
    }
    this.setOrgs = function(orgs) {
        this.orgs = orgs;
    }

})
.service('sfdcservice', function($q, $http, $state) {

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
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getAccountIds = function() {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/lookup-id-mapping/available-lookup-ids'
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.getOrgs = function() {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/lookup-id-mapping/'
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    }

    this.saveOrgs = function(configid, org){
        var deferred = $q.defer();

        $http({
            method: 'PUT',
            url: '/pls/lookup-id-mapping/config/' + configid,
            data: org
        }).then(function(response){
            deferred.resolve(response);
        });

        return deferred.promise;
    }


});
