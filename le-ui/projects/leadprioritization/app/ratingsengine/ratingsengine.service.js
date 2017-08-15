angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function($q, $state){
    var RatingsEngineStore = this;

    this.init = function() {
        this.foo = null;
    }

    this.init();
    
    this.clear = function() {
        this.init();
    }

})
.service('RatingsEngineService', function($q, $http, $state) {
    this.host = '/pls'; //default
});
