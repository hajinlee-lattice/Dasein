angular.module('common.datacloud')
.service('DataCloudStore', function($q, DataCloudService){
    var DataCloudStore = this;

    this.init = function() {
        this.enrichments = null;
        this.enrichmentsMap = {};
        this.categories = null;
        this.subcategories = {};
        this.count = null;
        this.selectedCount = null;
        this.premiumSelectMaximum = null;
        this.topAttributes = null;
        this.cube = null;
        this.metadata = {
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
        this.feedbackModal = {
            show: false,
            context: null
        };
    }

    this.init();

    this.getFeedbackModal = function() {
        return this.feedbackModal;
    }

    this.setFeedbackModal = function(bool, obj) {
        this.feedbackModal.context = obj;
        return this.feedbackModal.show = bool;
    }

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
        if (DataCloudStore.premiumSelectMaximum) {
            deferred.resolve(DataCloudStore.premiumSelectMaximum);
        } else {
            DataCloudService.getPremiumSelectMaximum().then(function(response){
                DataCloudStore.setPremiumSelectMaximum(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setPremiumSelectMaximum = function(item){
        DataCloudStore.premiumSelectMaximum = item;
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

    this.getAllEnrichmentsConcurrently = function(count){
        var deferred = $q.defer(),
            max = Math.ceil(count / 5), // six concurrent connections (IE only supports two)
            iterations = Math.ceil(count / max);
        
        this.concurrent = 0;

        if (this.enrichments && this.enrichments.length == count) {
            deferred.resolve(this.enrichments, true);
        } else {
            for (var j=0; j<iterations; j++) {
                this.getEnrichments({ max: max, offset: j * max }, true).then(function(result) {
                    DataCloudStore.concurrent++;

                    if (DataCloudStore.concurrent == iterations) {
                        deferred.resolve(DataCloudStore.enrichments);
                    }
                });
            }
        }

        return deferred.promise;
    }

    this.getEnrichments = function(opts, concatEnrichments){
        var deferred = $q.defer();
        if (this.enrichments) {
            deferred.resolve(this.enrichments);
        } else {
            DataCloudService.getEnrichments(opts).then(function(response){
                DataCloudStore.setEnrichments(response, concatEnrichments || false);
                
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setEnrichments = function(enrichments, concatEnrichments) {
        if (concatEnrichments) {
            this.enrichments = (this.enrichments || []).concat(enrichments.data);
        } else {
            this.enrichments = enrichments;
        }
    }

    this.setEnrichmentsMap = function(map) {
        this.enrichmentsMap = map;
    }

    this.getEnrichmentsMap = function(key) {
        return key ? this.enrichmentsMap[key] : this.enrichmentsMap;
    }

    this.updateEnrichments = function(enrichments){
        this.enrichments = enrichments;
    }

    this.getCount = function(){
        var deferred = $q.defer();
        if (DataCloudStore.count) {
            deferred.resolve(DataCloudStore.count);
        } else {
            DataCloudService.getCount().then(function(response){
                DataCloudStore.setCount(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setCount = function(count){
        DataCloudStore.count = count;
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
                            attribute = _.findWhere(response.data.EnrichmentAttributes, {ColumnId: item.Attribute});
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
                vm.topAttributes = response.data; // ben
                deferred.resolve(vm.topAttributes);
            });
        }

        return deferred.promise;
    }

    this.setTopAttributes = function(items, category) {
        this.topAttributes = this.topAttributes || {};
        this.topAttributes[category] = items;
    }

    this.getCube = function() {
        var deferred = $q.defer();
        if (DataCloudStore.cube) {
            deferred.resolve(DataCloudStore.cube);
        } else {
            DataCloudService.getCube().then(function(response){
                DataCloudStore.setCube(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }

    this.setCube = function(cube) {
        DataCloudStore.cube = cube;
    }

})
.service('DataCloudService', function($q, $http, $state, $stateParams, SegmentStore) {
    this.host = '/pls';
    this.paths = {
        'lattice': '/latticeinsights',
        'customer': '/datacollection'
    };
    this.path = this.paths['lattice'];

    this.setHost = function(value) {
        this.host = value;
    }

    this.inModel = function() {
        var name = $state.current.name.split('.');

        return name[1] == 'model';
    }

    this.inSegment = function() {
        return this.path == this.paths['customer'];
    }

    this.url = function(customerDataUrl, internalDataUrl) {
        return this.host + this.path + (this.inSegment() ? customerDataUrl : internalDataUrl);
    }

    this.getPremiumSelectMaximum = function(){
        var deferred = $q.defer();
        
        $http({
            method: 'get',
            url: this.host + '/latticeinsights/insights/premiumattributeslimitation'
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getCount = function(){
        var deferred = $q.defer(),
            url = this.url('/attributes','/insights') + '/count';
        
        $http({
            method: 'get',
            url: url
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getSelectedCount = function(){
        var deferred = $q.defer();
        
        $http({
            method: 'get',
            url: this.url('/attributes','/insights') + '/selectedattributes/count'
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getCategories = function(){
        var deferred = $q.defer();
        
        $http({
            method: 'get',
            url: this.url('/attributes','/insights') + '/categories'
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getSubcategories = function(category){
        var deferred = $q.defer();
        
        $http({
            method: 'get',
            url: this.url('/attributes','/insights') + '/subcategories',
            params: {
                category: category
            }
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getEnrichments = function(opts){
        var deferred = $q.defer(),
            opts = opts || {},
            offset = opts.offset || 0,
            max = opts.max || null,
            onlySelectedAttributes = opts.onlySelectedAttributes || null,
            url = this.url('/attributes','/insights');
        
        $http({
            method: 'get',
            url: url,
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
            url: this.host + this.path + '/insights',
            data: data
        }).then(function(response){
            deferred.resolve(response.data);
        }, function(response) { //on error
            deferred.resolve(response.data);
        });
        
        return deferred.promise;
    }

    this.getAllTopAttributes = function(opts) {
        var deferred = $q.defer(),
            url = this.url('/statistics/topn?topbkt=true','/stats/topn');
        
        $http({
            method: 'get',
            url: url
        }).then(function(response) {
            deferred.resolve(response);
        });
        
        return deferred.promise;
    }

    this.getCube = function(opts){
        var deferred = $q.defer(),
            url = this.url('/statistics/cube','/stats/cube');

        // FIXME: temporary disable Cube API in Data Cloud Explorer, bad data -lazarus
        // if (url == '/pls/latticeinsights/stats/cube') { 
        //     deferred.resolve({ data: {} }); 
        //     return deferred.promise; 
        // }

        $http({
            method: 'get',
            url: url
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
                category: categoryName
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
});
