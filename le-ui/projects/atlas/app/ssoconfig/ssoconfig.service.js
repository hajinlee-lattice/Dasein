angular.module('lp.ssoconfig')
.service('SSOConfigStore', function($q, SSOConfigService){
    var DeleteDataStore = this;

    this.SAMLconfig = null;

    this.init = function() {
    }

    this.init();

    this.clear = function() {
        this.init();
    }

    this.getSAMLConfig = function(cacheOnly) {
        var deferred = $q.defer();

        SSOConfigService.GetSAMLConfig().then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.getURIInfo = function(cacheOnly) {
        var deferred = $q.defer();

        SSOConfigService.GetURIInfo().then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.validateSAMLConfig = function(metadataObject) {
        var deferred = $q.defer();

        SSOConfigService.ValidateSAMLConfig(metadataObject).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.createSAMLConfig = function(metadataObject) {
        var deferred = $q.defer();

        SSOConfigService.CreateSAMLConfig(metadataObject).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    this.deleteSAMLConfig = function(configID) {
        var deferred = $q.defer();

        SSOConfigService.DeleteSAMLConfig(configID).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }
})
.service('SSOConfigService', function($q, $http) {

    this.GetSAMLConfig = function() {

        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/saml-config'
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.reject(error);
        });  

        return deferred.promise;  
    }

    this.GetURIInfo = function() {

        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/saml-config/sp-uri-info'
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        });  

        return deferred.promise;  
    }


    this.ValidateSAMLConfig = function(metadataObject) {

        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: '/pls/saml-config/validate',
            data: metadataObject
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        });  

        return deferred.promise;  
    }



    this.CreateSAMLConfig = function(metadataObject) {

        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: '/pls/saml-config',
            data: metadataObject,
            headers: {
                ErrorDisplayMethod: 'banner',
                ErrorDisplayOptions: '{"title": "", "message":"Failed! Your metadata has unexpected errors"}'
            }
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        });  

        return deferred.promise;  
    }

    this.DeleteSAMLConfig = function(configID) {
        var deferred = $q.defer();

        var deferred = $q.defer();
        $http({
            method: 'DELETE',
            url: '/pls/saml-config/' + configID
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        }); 

        return deferred.promise;
    }

    });
