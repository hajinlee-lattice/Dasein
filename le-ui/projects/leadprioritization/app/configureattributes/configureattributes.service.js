angular.module('lp.configureattributes')
.service('ConfigureAttributesStore', function($q, $state){
    var configureattributesStore = this;

    this.init = function() {
    }

    this.init();

    this.clear = function() {
        this.init();
    }

})
.service('ConfigureAttributesService', function($q, $http, $state, ResourceUtility) {
});
