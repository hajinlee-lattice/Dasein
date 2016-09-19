var app = angular.module("app.services.service.ServiceService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility'
]);

app.service('ServiceService', function($q, $http, $interval, _, SessionUtility){

    this.registeredServices = null;
    this.registeredServicesWithAssociatedProducts = null;

    function cacheServiceList(services) {
        this.registeredServices = services;
        $interval(function(){
            this.registeredServices = null;
        }, 60000);
    }

    function cacheServiceAndProductsList(servicesAndProducts) {
        this.registeredServicesWithAssociatedProducts = servicesAndProducts;
        $interval(function() {
            this.registeredServicesWithAssociatedProducts = null;
        }, 60000);
    }
    
    this.spaceConfigOptions = null;

    function cacheSpaceConfigOptions(services) {
        this.spaceConfigOptions = services;
        $interval(function(){
            this.spaceConfigOptions = null;
        }, 60000);
    }

    this.GetRegisteredServices = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        if (this.registeredServices === null) {
            $http({
                method: 'GET',
                url: '/admin/services'
            }).success(function (data) {
                cacheServiceList(data);
                result.resultObj = data;
                defer.resolve(result);
            }).error(function (err, status) {
                SessionUtility.handleAJAXError(err, status);
            });
        } else {
            result.resultObj = this.registeredServices;
            defer.resolve(result);
        }

        return defer.promise;
    };

    this.GetRegisteredServicesWithAssociatedProducts = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        if (this.registeredServicesWithAssociatedProducts === null) {
            $http({
                method: 'GET',
                url: '/admin/services/products'
            }).success(function (data) {
                cacheServiceAndProductsList(data);
                result.resultObj = data;
                defer.resolve(result);
            }).error(function (err, status) {
                SessionUtility.handleAJAXError(err, status);
            });
        } else {
            result.resultObj = this.registeredServicesWithAssociatedProducts;
            defer.resolve(result);
        }

        return defer.promise;
    };

    this.GetServiceDefaultConfig = function(service) {
        var defer = $q.defer();
        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/services/' + service + '/default'
        }).success(function(data){
            if (data !== null && data.hasOwnProperty("RootPath")) {
                data.Component = service;

                if (!data.hasOwnProperty("Nodes") || data.Nodes.length === 0) {
                    data.Message = data.Component + " does not need any configuration";
                }

                result.resultObj = data;
            } else {
                result.success = false;
                result.errMsg = "Could not load default configuration.";
            }
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

    this.GetSpaceConfigOptions = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        if (this.spaceConfigOptions === null) {
            $http({
                method: 'GET',
                url: '/admin/services/SpaceConfiguration/options?include_dynamic_opts=true'
            }).success(function (data) {
                cacheSpaceConfigOptions(data);
                result.resultObj = data;
                defer.resolve(result);
            }).error(function (err, status) {
                SessionUtility.handleAJAXError(err, status);
            });
        } else {
            result.resultObj = this.spaceConfigOptions;
            defer.resolve(result);
        }

        return defer.promise;
    };
    
    this.GetAvailableProducts = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/tenants/products'
        }).success(function (data) {
            result.resultObj = data;
            defer.resolve(result);
        }).error(function (err, status) {
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };
    
    this.GetFeatureFlagDefinitions = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/featureflags/'
        }).success(function (data) {
            result.resultObj = data;
            defer.resolve(result);
        }).error(function (err, status) {
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };
    

});
