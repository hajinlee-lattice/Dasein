angular.module('lp.enrichment.leadenrichment')
.service('EnrichmentStore', function($q, EnrichmentService){
    var EnrichmentStore = this;
    this.enrichments = null;
    this.categories = null;
    this.selectedCount = null;

    this.getCategories = function(){
        var deferred = $q.defer();
        if (this.categories) {
            deferred.resolve(this.categories);
        } else {
            EnrichmentService.getCategories().then(function(response){
                EnrichmentStore.setCategories(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setCategories = function(item){
        this.categories = item;
    }

    this.getEnrichments = function(){
        var deferred = $q.defer();
        if (this.enrichments) {
            deferred.resolve(this.enrichments);
        } else {
            EnrichmentService.getEnrichments().then(function(response){
                EnrichmentStore.setEnrichments(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setEnrichments = function(item){
        this.enrichments = item;
    }

    this.getSelectedCount = function(){
        var deferred = $q.defer();
        if (this.selectedCount) {
            deferred.resolve(this.selectedCount);
        } else {
            EnrichmentService.getSelectedCount().then(function(response){
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }
})
.service('EnrichmentService', function($q, $http){
    this.getSelectedCount = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead/selectedattributes/count'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getCategories = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead/categories'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getEnrichments = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.setEnrichments = function(data){
        var deferred = $q.defer();
        $http({
            method: 'put',
            url: '/pls/enrichment/lead',
            data: data
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }
});
