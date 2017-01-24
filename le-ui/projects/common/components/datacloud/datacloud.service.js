angular.module('common.datacloud')
.service('DataCloudStore', function($q, DataCloudService){
    var DataCloudStore = this;

    this.init = function() {
        this.enrichments = null;
        this.categories = null;
        this.subcategories = {};
        this.count = null;
        this.selectedCount = null;
        this.premiumSelectMaximum = null;
        this.topAttributes = null;
        this.metadata = {
            current: 1,
            toggle: {
                show: {
                    nulls: false,
                    selected: false,
                    premium: false,
                    internal: false
                },
                hide: {
                    premium: false
                }
            },
            lookupMode: false,
            generalSelectLimit: 0,
            generalSelectedTotal: 0,
            premiumSelectLimit: 0,
            premiumSelectedTotal: 0,
            enrichmentsTotal: 0
        };
    }

    this.init();

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
            DataCloudService.getPremiumSelectMaximum().then(function(response){
                DataCloudStore.setPremiumSelectMaximum(response);
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
            DataCloudService.getCategories().then(function(response){
                DataCloudStore.setCategories(response);
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
        if (this.subcategories[category]) {
            deferred.resolve(this.subcategories[category]);
        } else {
            DataCloudService.getSubcategories(category).then(function(response){
                DataCloudStore.setSubcategories(category, response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setSubcategories = function(category, item){
        this.subcategories[category] = item;
    }

    this.getEnrichments = function(opts){
        var deferred = $q.defer();
        if (this.enrichments) {
            deferred.resolve(this.enrichments);
        } else {
            DataCloudService.getEnrichments(opts).then(function(response){
            //DataCloudStore.setEnrichments(response);
            deferred.resolve(response);
        });
        }
        return deferred.promise;
    }

    this.setEnrichments = function(item){
        this.enrichments = item;
    }

    this.getCount = function(){
        var deferred = $q.defer();
        if (this.count) {
            deferred.resolve(this.count);
        } else {
            DataCloudService.getCount().then(function(response){
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.getSelectedCount = function(){
        var deferred = $q.defer();
        if (this.selectedCount) {
            deferred.resolve(this.selectedCount);
        } else {
            DataCloudService.getSelectedCount().then(function(response){
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.getTopAttributes = function(opts) {
        var deferred = $q.defer(),
        opts = opts || {};
        opts.category = opts.category || 'firmographics';
        opts.limit = opts.limit || 5;
        if (this.topAttributes) {
            deferred.resolve(this.topAttributes[opts.category]);
        } else {
            DataCloudService.getTopAttributes(opts).then(function(response) {
                for(var i in response.data.SubCategories) {
                    var items = response.data.SubCategories[i];
                    for(var j in items) {
                        var item = items[j],
                            attribute = _.findWhere(response.data.EnrichmentAttributes, {FieldName: item.Attribute});
                        item.DisplayName = (attribute ? attribute.DisplayName : null);
                    }
                }
                DataCloudStore.setTopAttributes(response, opts.category);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.getAllTopAttributes = function(opts) {
        var deferred = $q.defer(),
        opts = opts || {};
        opts.max = opts.max || 5;

        if (this.topAttributes) {
            deferred.resolve(this.topAttributes);
        } else {
            var vm = this;
            
            DataCloudService.getAllTopAttributes(opts).then(function(response) {
                vm.topAttributes = data = response.data;
                deferred.resolve(vm.topAttributes);
            });
        }

        return deferred.promise;
    }

    this.setTopAttributes = function(items, category) {
        this.topAttributes = this.topAttributes || [];
        this.topAttributes[category] = items;
    }

    this.getCube = function(){
        var deferred = $q.defer();
        DataCloudService.getCube().then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }
})
.service('DataCloudService', function($q, $http){
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

    this.getCount = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead/count'
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

    this.getTopAttributes = function(opts) {
        var deferred = $q.defer(),
        opts = opts || {};
        opts.category = opts.category || 'firmographics';
        opts.limit = opts.limit || 6;
        opts.loadEnrichmentMetadata = opts.loadEnrichmentMetadata || false;
        $http({
            method: 'get',
            url: '/pls/enrichment/stats/topn',
            params: {
                category: opts.category,
                limit: opts.limit,
                loadEnrichmentMetadata: opts.loadEnrichmentMetadata
            }
        }).then(function(response) {
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getAllTopAttributes = function(opts) {
        var deferred = $q.defer(),
        opts = opts || {};
        opts.category = opts.category || 'firmographics';
        opts.limit = opts.limit || 6;
        opts.loadEnrichmentMetadata = opts.loadEnrichmentMetadata || false;
        $http({
            method: 'get',
            url: '/pls/enrichment/stats/topn/all',
            params: {
                limit: opts.limit,
                loadEnrichmentMetadata: opts.loadEnrichmentMetadata
            }
        }).then(function(response) {
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getCube = function(opts){
        var deferred = $q.defer(),
            opts = opts || {},
            query = opts.query || '';
        $http({
            method: 'get',
            url: '/pls/enrichment/stats/cube',
            params: {
                q: query
            }
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }
});
