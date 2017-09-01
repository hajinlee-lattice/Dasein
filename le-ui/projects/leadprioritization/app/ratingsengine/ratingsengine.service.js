angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function($q, $state){
    var RatingsEngineStore = this;

    this.init = function() {
        this.validation = {
            segment: true,
            attributes: true,
            rules: true
        }
        this.currentRating = {};
    }

    this.init();
    
    this.clear = function() {
        this.init();
    }

    this.setCurrentRating = function(rating) {
        this.currentRating = rating;
    }

    this.getCurrentRating = function() {
        return this.currentRating;
    }

    this.nextSaveGeneric = function(nextState) {
        $state.go(nextState, {rating_id: RatingsEngineStore.currentRating.id});
    }
})
.service('RatingsEngineService', function($q, $http, $state) {

    this.host = '/pls'; //default

    this.getRatings = function() {

        var deferred = $q.defer(),
            result,
            url = this.host + '/ratingengines';

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

    this.deleteRating = function(ratingName) {

        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines/' + ratingName;

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

    this.saveRating = function(opts) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/ratingengines',
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

});
