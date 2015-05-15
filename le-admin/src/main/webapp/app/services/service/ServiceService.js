var app = angular.module("app.services.service.ServiceService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility'
]);

app.service('ServiceService', function($q, $http, $interval, _, SessionUtility){

    this.registeredServices = null;

    function cacheServiceList(services) {
        this.registeredServices = services;
        $interval(function(){
            this.registeredServices = null;
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
                if (data.Component === "Dante") {
                    data = {
                        Component: "Dante",
                        Invisible: true
                    };
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
                url: '/admin/services/dropdown_options?component=SpaceConfiguration'
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

});
