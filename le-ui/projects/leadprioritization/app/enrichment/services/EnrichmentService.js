angular.module('lp.enrichment.leadenrichment')
.service('EnrichmentStore', function($q, EnrichmentService){
    var EnrichmentStore = this;
    this.enrichments = null;
    this.categories = null;
    this.subcategories = null;
    this.selectedCount = null;
    this.premiumSelectMaximum = null;
    this.metadata = {
        current: 1,
        toggle: {
            show: {
                selected: false,
                premium: false,
                internal: false
            },
            hide: {
                premium: false
            }
        }
    };

    var getObj = function(path, obj) {
        return path.split('.').reduce(function(obj, i) {
            return obj[i];
        }, obj);
    }

    var setObj = function (path, value, scope) {
        var levels = path.split('.'),
            max_level = levels.length - 1,
            target = scope;

        levels.some(function (level, i) {
            if (typeof level === 'undefined') {
                return true;
            }
            if (i === max_level) {
                target[level] = value;
            } else {
                var obj = target[level] || {};
                target[level] = obj;
                  target = obj;
            }
        });
    }

    this.getMetadata = function(name) {
        return getObj(name, this.metadata);
    }

    this.setMetadata = function(name, value) {
        setObj(name, value, this.metadata);
    }

    this.getPremiumSelectMaximum = function(){
        var deferred = $q.defer();
        if (this.premiumSelectMaximum) {
            deferred.resolve(this.premiumSelectMaximum);
        } else {
            EnrichmentService.getPremiumSelectMaximum().then(function(response){
                EnrichmentStore.setPremiumSelectMaximum(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setPremiumSelectMaximum = function(item){
        this.premiumSelectMaximum = item;
    }

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

    this.getSubcategories = function(category){
        var deferred = $q.defer();
        if (this.subcategories) {
            deferred.resolve(this.categories);
        } else {
            EnrichmentService.getSubcategories(category).then(function(response){
                EnrichmentStore.setSubcategories(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setSubcategories = function(item){
        this.subcategories = item;
    }

    this.getEnrichments = function(opts){
        var deferred = $q.defer();
        if (this.enrichments) {
            deferred.resolve(this.enrichments);
        } else {
            EnrichmentService.getEnrichments(opts).then(function(response){
            //EnrichmentStore.setEnrichments(response);
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
    this.getPremiumSelectMaximum = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            //ENVs: Default, QA, Production
            //url: '/Pods/<ENV>/Default/PLS/EnrichAttributeMaxNumber'
            url: '/pls/enrichment/lead/premiumattributeslimitation'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

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

    this.getSubcategories = function(category){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead/subcategories',
            params: {
                category: category
            }
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getEnrichments = function(opts){
        var deferred = $q.defer();
        var opts = opts || {},
            offset = opts.offset || 0,
            max = opts.max || null,
            onlySelectedAttributes = opts.onlySelectedAttributes || null;
        $http({
            method: 'get',
            url: '/pls/enrichment/lead',
            params: {
                offset: offset,
                max: max,
                onlySelectedAttributes: onlySelectedAttributes
            }
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
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }
});
