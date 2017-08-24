angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function($q, $state){
    var RatingsEngineStore = this;

    this.init = function() {
        this.validation = {
            segment: true,
            attributes: true
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
});
