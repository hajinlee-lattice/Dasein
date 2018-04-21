angular.module('lp.delete')
.service('DeleteDataStore', function(){
    var DeleteDataStore = this;

    this.init = function() {
    }

    this.init();

    this.clear = function() {
        this.init();
    }
})
.service('DeleteDataService', function($q, $http) {

        this.cleanupByUpload = function(fileName, schemaInterpretation, cleanupType) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/cdl/cleanupbyupload',
                params: {
                    fileName: fileName,
                    schema: schemaInterpretation,
                    cleanupOperationType: cleanupType
                },
                headers: { 'Content-Type': 'application/json' }
            }).success(function(result, status) {
                deferred.resolve(result);
            }).error(function(error, status) {
                console.log(error);
                deferred.resolve(error);
            });

            return deferred.promise;
        };

        this.cleanupByDateRange = function(startTime, endTime, schemaInterpretation) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/cdl/cleanupbyrange',
                params: {
                    startTime: startTime,
                    endTime: endTime,
                    schema: schemaInterpretation
                },
                headers: { 'Content-Type': 'application/json' }
            }).success(function(result, status) {
                deferred.resolve(result);
            }).error(function(error, status) {
                console.log(error);
                deferred.resolve(error);
            });

            return deferred.promise;
        };

        this.cleanupAllData = function(schemaInterpretation) {
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/cdl/cleanupall',
                params: {
                    schema: schemaInterpretation,
                },
                headers: { 'Content-Type': 'application/json' }
            }).success(function(result, status) {
                deferred.resolve(result);
            }).error(function(error, status) {
                console.log(error);
                deferred.resolve(error);
            });

            return deferred.promise;
        };
    });
