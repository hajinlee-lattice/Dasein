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
        this.topAttributes = this.topAttributes || null;;
        this.cube = this.cube || null;
        this.metadataSegments = this.metadataSegments || null;
        this.metadata = this.metadata || {
            current: 1,
            currentCategory: 1,
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
            enrichmentsTotal: 0,
            tabSection: 'browse',
            category: null,
            subcategory: null
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

    this.setHost = function(value) {
        DataCloudService.setHost(value);
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

    this.updateEnrichments = function(enrichments){
        this.enrichments.data = enrichments;
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
                response.data = angular.extend({}, response.data, DemoData.topn)
                //Object.assign(response.data, DemoData.topn); - didn't work on IE
                vm.topAttributes = response.data; // ben
                deferred.resolve(vm.topAttributes);
            });
        }

        return deferred.promise;
    }

    this.setTopAttributes = function(items, category) {
        this.topAttributes = this.topAttributes || [];
        this.topAttributes[category] = items;
    }

    this.getCube = function() {
        if (this.cube) {
            return this.cube;
        }

        var deferred = $q.defer();
        DataCloudService.getCube().then(function(response){
            deferred.resolve(response);
        });

        DataCloudStore.setCube(deferred.promise);
        return deferred.promise;
    }

    this.setCube = function(cubePromise) {
        this.cube = cubePromise;
    }

    this.getMetadataSegments = function() {
        var deferred = $q.defer();
        if (this.metadataSegments) {
            deferred.resolve(this.metadataSegments);
        } else {
            DataCloudService.getMetadataSegments().then(function(response){
                DataCloudStore.setMetadataSegments(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setMetadataSegments = function(items) {
        this.metadataSegments = items;
    }
})
.service('DataCloudService', function($q, $http, $state) {
    this.host = '/pls'; //default

    this.setHost = function(value) {
        this.host = value;
    }

    this.getPremiumSelectMaximum = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            //ENVs: Default, QA, Production
            //url: '/Pods/<ENV>/Default/PLS/EnrichAttributeMaxNumber'
            url: this.host + '/latticeinsights/insights/premiumattributeslimitation'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getCount = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: this.host + '/latticeinsights/insights/count'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getSelectedCount = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: this.host + '/latticeinsights/insights/selectedattributes/count'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getCategories = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: this.host + '/latticeinsights/insights/categories'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getSubcategories = function(category){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: this.host + '/latticeinsights/insights/subcategories',
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
            url: this.host + '/latticeinsights/insights',
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
            url: this.host + '/latticeinsights/insights',
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
            url: this.host + '/latticeinsights/stats/topn',
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
            url: this.host + '/latticeinsights/stats/topn/all',
            params: {
                max: 9999,
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
            url: this.host + '/latticeinsights/stats/cube',
            params: {
                q: query
            }
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.setFlags = function(opts, flags){
        var deferred = $q.defer(),
            opts = opts || {},
            fieldName = opts.fieldName || '',
            useCase = opts.useCase || 'CompanyProfile',
            flags = flags || {}; // json
        $http({
            method: 'POST',
            url: '/pls/attributes/flags/' + fieldName + '/' + useCase,
            data: flags
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.setFlag = function(opts, boolean){
        var deferred = $q.defer(),
            opts = opts || {},
            fieldName = opts.fieldName || '',
            useCase = opts.useCase || 'CompanyProfile',
            propertyName = opts.propertyName || '',
            boolean = boolean || false;
        $http({
            method: 'POST',
            url: '/pls/attributes/flags/' + fieldName + '/' + useCase + '/' + propertyName,
            data: boolean
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.setFlagsByCategory = function(opts, flags){
        var deferred = $q.defer(),
            opts = opts || {},
            categoryName = opts.categoryName || '',
            useCase = opts.useCase || 'CompanyProfile',
            flags = flags || {}; // json
        $http({
            method: 'POST',
            url: '/pls/attributes/categories/flags/' + useCase,
            params: {
                category: categoryName,
            },
            data: flags
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.setFlagsBySubcategory = function(opts, flags){
        var deferred = $q.defer(),
            opts = opts || {},
            categoryName = opts.categoryName || '',
            subcategoryName = opts.subcategoryName || '',
            useCase = opts.useCase || 'CompanyProfile',
            flags = flags || {}; // json
        $http({
            method: 'POST',
            url: '/pls/attributes/categories/subcategories/flags/' + useCase, //+ categoryName + '/' + subcategoryName + '/' + useCase,
            params: {
                category: categoryName,
                subcategory: subcategoryName
            },
            data: flags
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getMetadataSegments = function(opts){
        var deferred = $q.defer(),
            opts = opts || {};
        $http({
            method: 'get',
            url: this.host + '/metadatasegments/all'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }
});
