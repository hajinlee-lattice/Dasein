angular.module('common.datacloud')
    .filter("sortByKey", function () {
        return function (array, key) {
            if (!angular.isArray(array)) return;
            var empty = [];
            var present = array.filter(function (item) {
                if (item[key]) {
                    return item;
                } else {
                    empty.push(item);
                }
            });
            return present.concat(empty);
        };
    })
    .service('DataCloudStore', function ($q, DataCloudService) {
        var DataCloudStore = this;

        this.cube = null;

        this.init = function () {
            this.enrichments = null;
            this.enrichmentsMap = {};
            this.categories = null;
            this.presentCategories = null;
            this.subcategories = {};
            this.count = null;
            this.selectedCount = null;
            this.premiumSelectMaximum = null;
            this.topAttributes = null;
            this.ratingsEngineAttributes = [];
            this.ratingIterationFilter = null;
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
                enrichmentsTotal: -1,
                tabSection: 'browse',
                category: null,
                subcategory: null,
                subheadercategory: null
            };
            this.feedbackModal = {
                show: false,
                context: null
            };
            this.attributesCount = null;
            this.validFreeTextTypes = ['alpha', 'enum', 'email', 'phone', 'uri']; //PLS-8870
            this.topCategories = [
                'Lattice Ratings',
                'Firmographics',
                'My Attributes',
                'Curated Account Attributes',
                'Contact Attributes',
                'Product Spend Profile',
                'Intent',
                'Website Profile',
                'Technology Profile',
                'Online Presence',
                'Growth Trends',
                'Website Keywords'
            ];
            this.warnings = {};
        };

        this.init();

        this.clear = function () {
            this.init();
        };

        var getObj = function (path, obj) {
            return path.split('.').reduce(function (obj, i) {
                return obj[i];
            }, obj);
        };

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
        };

        this.setWarnings = function (warnings) {
            this.warnings = warnings;
        }

        this.getWarnings = function () {
            return this.warnings;
        }

        this.getMetadata = function (name) {
            return getObj(name, this.metadata);
        };

        this.setMetadata = function (name, value) {
            setObj(name, value, this.metadata);
        };

        this.setHost = function (value) {
            DataCloudService.setHost(value);
        };

        this.setPath = function (value) {
            DataCloudService.setPath(value);
        };

        this.getPremiumSelectMaximum = function () {
            var deferred = $q.defer();
            if (DataCloudStore.premiumSelectMaximum) {
                deferred.resolve(DataCloudStore.premiumSelectMaximum);
            } else {
                DataCloudService.getPremiumSelectMaximum().then(function (response) {
                    DataCloudStore.setPremiumSelectMaximum(response);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };

        this.setPremiumSelectMaximum = function (item) {
            DataCloudStore.premiumSelectMaximum = item;
        };

        this.getSelectMaximum = function () {
            var deferred = $q.defer();
            if (DataCloudStore.selectMaximum) {
                deferred.resolve(DataCloudStore.selectMaximum);
            } else {
                DataCloudService.getSelectMaximum().then(function (response) {
                    var total = (response && response.data ? response.data['MaxEnrichAttributes'] : null);
                    DataCloudStore.setSelectMaximum(total);
                    deferred.resolve(total);
                });
            }
            return deferred.promise;
        };

        this.setSelectMaximum = function (item) {
            DataCloudStore.selectMaximum = item;
        };

        this.getCategories = function () {
            var deferred = $q.defer();
            if (this.categories) {
                deferred.resolve(this.categories);
            } else {
                DataCloudService.getCategories().then(function (response) {
                    DataCloudStore.setCategories(response);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };
        this.setCategories = function (item) {
            this.categories = item;
        };

        this.setRatingIterationFilter = function (type) {
            this.ratingIterationFilter = type;
        }
        this.getRatingIterationFilter = function () {
            return this.ratingIterationFilter;
        }

        this.setPresentCategories = function (categories) {
            this.presentCategories = categories;
        }
        this.getPresentCategories = function () {
            return this.presentCategories;
        }

        this.getSubcategories = function (category) {
            var deferred = $q.defer();
            if (this.subcategories[category]) {
                deferred.resolve(this.subcategories[category]);
            } else {
                DataCloudService.getSubcategories(category).then(function (response) {
                    DataCloudStore.setSubcategories(category, response);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };

        this.setSubcategories = function (category, item) {
            this.subcategories[category] = item;
        };

        this.getCategoriesSortOrder = function () {
            var categoriesIndices = {};
            this.topCategories.forEach(function (category, index) {
                categoriesIndices[category] = index;
            });
            return categoriesIndices;
        }

        this.getAllEnrichmentsConcurrently = function (opts, nocache) {
            var deferred = $q.defer(),
                /*  
                 *  Most browsers support 6 concurrent connections, older IE supports two
                 *  Setting this to 5 to make room for topn call -lazarus
                 */
                connections = 1;

            this.concurrent_count = 0;

            if (this.enrichments && !nocache/* && this.enrichments.length == total_count*/) {
                deferred.resolve(this.enrichments, true);
            } else {
                this.getEnrichments(opts, true, nocache).then(function (result) {
                    deferred.resolve(DataCloudStore.enrichments);
                });
            }

            return deferred.promise;
        };

        this.getEnrichments = function (opts, concatEnrichments, nocache) {
            var deferred = $q.defer(),
                nocache = nocache || false,
                ret;

            if (this.enrichments && !nocache) {
                ret = (this.enrichments && this.enrichments.data ? this.enrichments.data : this.enrichments);
                DataCloudStore.setMetadata('enrichmentsTotal', ret.length);
                deferred.resolve(ret);
            } else {
                DataCloudService.getEnrichments(opts).then(function (response) {
                    ret = (response && response.data ? response.data : response);
                    DataCloudStore.setEnrichments(ret, concatEnrichments || false);
                    DataCloudStore.setMetadata('enrichmentsTotal', ret.length);
                    deferred.resolve(ret);
                });
            }

            return deferred.promise;
        };

        this.setEnrichments = function (enrichments, concatEnrichments) {
            if (concatEnrichments) {
                this.enrichments = (this.enrichments || []).concat(enrichments);
            } else {
                this.enrichments = enrichments;
            }
        };

        this.setEnrichmentsMap = function (map) {
            this.enrichmentsMap = map;
        };

        this.getEnrichmentsMap = function (key) {
            return key ? this.enrichmentsMap[key] : this.enrichmentsMap;
        };

        this.updateEnrichments = function (enrichments) {
            this.enrichments = enrichments;
        };

        this.getCount = function () {
            var deferred = $q.defer();

            if (DataCloudStore.count) {
                deferred.resolve(DataCloudStore.count);
            } else {
                DataCloudService.getCount().then(function (response) {
                    DataCloudStore.setCount(response);
                    deferred.resolve(response);
                });
            }

            return deferred.promise;
        };

        this.setCount = function (count) {
            DataCloudStore.count = count;
        };

        this.getAttributesCount = function () {
            // This call is really a workaround for the fact that getCount() will first
            // return 0 for an empty tenant, then after a refresh, will return ~21,000. 
            var deferred = $q.defer();
            if (DataCloudStore.attributesCount) {
                deferred.resolve(DataCloudStore.attributesCount);
            } else {
                DataCloudService.getAttributesCount().then(function (response) {
                    DataCloudStore.setAttributesCount(response.data);
                    deferred.resolve(response);
                });
            }

            return deferred.promise;
        };

        this.setAttributesCount = function (count) {
            DataCloudStore.attributesCount = count;
        };

        this.getSelectedCount = function () {
            var deferred = $q.defer();
            if (this.selectedCount) {
                deferred.resolve(this.selectedCount);
            } else {
                DataCloudService.getSelectedCount().then(function (response) {
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };

        this.getTopAttributes = function (opts) {
            var deferred = $q.defer(),
                opts = opts || {};
            opts.category = opts.category || 'firmographics';
            opts.limit = opts.limit || 5;
            if (this.topAttributes) {
                deferred.resolve(this.topAttributes[opts.category]);
            } else {
                DataCloudService.getTopAttributes(opts).then(function (response) {
                    for (var i in response.data.SubCategories) {
                        var items = response.data.SubCategories[i];
                        for (var j in items) {
                            var item = items[j],
                                attribute = _.findWhere(response.data.EnrichmentAttributes, { ColumnId: item.Attribute });
                            item.DisplayName = (attribute ? attribute.DisplayName : null);
                        }
                    }
                    DataCloudStore.setTopAttributes(response, opts.category);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };

        this.getAllTopAttributes = function (opts, nocache) {
            var deferred = $q.defer(),
                opts = opts || {};

            opts.max = opts.max || 5;

            if (this.topAttributes && !nocache) {
                deferred.resolve(this.topAttributes);
            } else {
                var vm = this;

                DataCloudService.getAllTopAttributes(opts).then(function (response) {
                    vm.topAttributes = response.data;
                    deferred.resolve(vm.topAttributes);
                });
            }

            return deferred.promise;
        };

        this.setTopAttributes = function (items, category) {
            this.topAttributes = this.topAttributes || {};
            this.topAttributes[category] = items;
        };

        this.getCube = function (opts, nocache) {
            var deferred = $q.defer();
            if (DataCloudStore.cube && !nocache) {
                deferred.resolve(DataCloudStore.cube);
            } else {
                DataCloudService.getCube(opts).then(function (response) {
                    DataCloudStore.setCube(response);
                    deferred.resolve(response);
                });
            }
            return deferred.promise;
        };

        this.setCube = function (cube) {
            DataCloudStore.cube = cube;
        };

        this.getFeedbackModal = function () {
            return this.feedbackModal;
        };

        this.setFeedbackModal = function (bool, obj) {
            this.feedbackModal.context = obj;
            return this.feedbackModal.show = bool;
        };

        this.sendFeedback = function (report, type) {
            var deferred = $q.defer(),
                type = type || '';

            DataCloudService.sendFeedback(report, type).then(function (response) {
                deferred.resolve(response);
            });
            return deferred.promise;
        };

        var flattenRatingsEngineAttributes = function (ratingsEngine) {
            return (ratingsEngine && ratingsEngine[0] && ratingsEngine[0].rule && ratingsEngine[0].rule.selectedAttributes ? ratingsEngine[0].rule.selectedAttributes : []);
        };

        this.setRatingsEngineAttributes = function (attributes) {
            this.ratingsEngineAttributes = attributes;
        };

        this.getCurrentRatingsEngineAttributes = function () {
            return this.ratingsEngineAttributes;
        };

        this.getRatingsEngineAttributes = function (ratingsEngineId, ratingModelId) {
            var deferred = $q.defer();
            DataCloudService.getRatingsEngineAttributes(ratingsEngineId, ratingModelId).then(function (response) {
                DataCloudStore.setRatingsEngineAttributes(flattenRatingsEngineAttributes(response.data));
                deferred.resolve(response.data);
            });
            return deferred.promise;
        };

        this.setSelectedRatingsEngineAttribute = function (attribute_id) {
            var index = this.ratingsEngineAttributes.indexOf(attribute_id);
            if (index >= 0) {
                this.ratingsEngineAttributes.splice(index, 1);
            } else {
                this.ratingsEngineAttributes.push(attribute_id)
            }
        };

        this.setSelectedRatingsEngineAttributes = function (attributes) {
            if (attributes) {
                attributes.forEach(function (value) {
                    DataCloudStore.setSelectedRatingsEngineAttribute(value);
                });
            }
        };

        this.selectRatingsEngineAttributes = function (rating_id, rating_model_id, attributes) {
            var deferred = $q.defer();
            this.setSelectedRatingsEngineAttributes(attributes);
            DataCloudService.selectRatingsEngineAttributes(rating_id, rating_model_id, this.ratingsEngineAttributes).then(function (response) {
                DataCloudStore.setRatingsEngineAttributes(response);
                deferred.resolve(response);
            });
            return deferred.promise;
        };

    })
    .service('DataCloudService', function ($q, $http, $state, $stateParams, SegmentStore) {
        this.host = '/pls';
        this.paths = {
            'lattice': '/latticeinsights',
            'customer': '/datacollection'
        };

        this.path = this.paths['lattice'];

        this.setHost = function (value) {
            this.host = value;
        };

        this.setPath = function (value) {
            this.path = this.paths[value];
        };

        this.inModel = function () {
            var name = $state.current.name.split('.');

            return name[1] == 'model';
        };

        this.inSegment = function () {
            return this.path == this.paths['customer'];
        };

        this.url = function (customerDataUrl, internalDataUrl) {
            return this.host + this.path + (this.inSegment() ? customerDataUrl : internalDataUrl);
        };
        // /model/ms__090930bf-1e34-478a-a459-764334e2a74f-ai_x8nel/engine_tv575xleso61m1fypt8ahw/attributes
        // pls/ratingengines/ms__090930bf-1e34-478a-a459-764334e2a74f-ai_x8nel/ratingmodels/engine_tv575xleso61m1fypt8ahw/metadata
        // pls/ratingengines/ms__090930bf-1e34-478a-a459-764334e2a74f-ai_x8nel/ratingmodels/engine_tv575xleso61m1fypt8ahw/metadata/cube
        // pls/ratingengines/ms__090930bf-1e34-478a-a459-764334e2a74f-ai_x8nel/ratingmodels/engine_tv575xleso61m1fypt8ahw/metadata/topn
        this.getPremiumSelectMaximum = function () {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.host + '/latticeinsights/insights/premiumattributeslimitation'
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getSelectMaximum = function () {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.host + '/latticeinsights/insights/premiumattributeslimitationmap'
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getCount = function () {
            var deferred = $q.defer(),
                url = this.url('/attributes', '/insights') + '/count';

            $http({
                method: 'get',
                url: url
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getAttributesCount = function () {
            var deferred = $q.defer(),
                url = '/pls/datacollection/attributes/count';

            $http({
                method: 'get',
                url: url
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getSelectedCount = function () {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.url('/attributes', '/insights') + '/selectedattributes/count'
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getCategories = function () {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.url('/attributes', '/insights') + '/categories'
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getSubcategories = function (category) {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.url('/attributes', '/insights') + '/subcategories',
                params: {
                    category: category
                }
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getEnrichments = function (opts) {
            var deferred = $q.defer(),
                opts = opts || {},
                offset = opts.offset || 0,
                max = opts.max || null,
                onlySelectedAttributes = opts.onlySelectedAttributes || null,
                url = opts.url || this.url('/attributes', '/insights');
            /**
             * /datacollection/attributes
             *  - or - 
             * /datacollection/insights
             */
            $http({
                method: 'get',
                url: url,
                params: {
                    //offset: offset,
                    //max: max,
                    onlySelectedAttributes: onlySelectedAttributes
                }
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.setEnrichments = function (data) {
            var deferred = $q.defer();

            $http({
                method: 'put',
                url: this.host + this.path + '/insights',
                data: data
            }).then(function (response) {
                deferred.resolve(response.data);
            }, function (response) { //on error
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.getAllTopAttributes = function (opts) {
            var deferred = $q.defer(),
                opts = opts || {},
                url = opts.url || this.url('/statistics/topn?topbkt=true', '/stats/topn');

            $http({
                method: 'get',
                url: url
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.getCube = function (opts) {
            var deferred = $q.defer(),
                opts = opts || {},
                url = opts.url || this.url('/statistics/cubes', '/stats/cubes');

            $http({
                method: 'get',
                url: url
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        this.setFlags = function (opts, flags) {
            var deferred = $q.defer(),
                opts = opts || {},
                fieldName = opts.fieldName || '',
                useCase = opts.useCase || 'CompanyProfile',
                flags = flags || {}; // json

            $http({
                method: 'POST',
                url: '/pls/attributes/flags/' + fieldName + '/' + useCase,
                data: flags
            }).then(function (response) {
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.setFlag = function (opts, boolean) {
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
            }).then(function (response) {
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.setFlagsByCategory = function (opts, flags) {
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
            }).then(function (response) {
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.setFlagsBySubcategory = function (opts, flags) {
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
            }).then(function (response) {
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.sendFeedback = function (opts, type) {
            var deferred = $q.defer(),
                opts = opts || {},
                type = type || 'incorrectmatchedattrs'; // incorrectmatchedattrs | incorrectlookups

            $http({
                method: 'POST',
                headers: {
                    'ErrorDisplayMethod': 'none'
                },
                url: this.host + '/datacloud/customerreports/' + type,
                data: opts
            }).then(function (response) {
                deferred.resolve(response.data);
            });

            return deferred.promise;
        };

        this.getRatingsEngineAttributes = function (ratingsEngineId, ratingModelId) {
            var deferred = $q.defer();

            $http({
                method: 'get',
                url: this.host + '/ratingengines/' + ratingsEngineId + '/ratingmodels' + (ratingModelId ? '/' + ratingModelId : ''),
            }).then(function (response) {
                deferred.resolve(response);
            });

            return deferred.promise;
        };

        var flattenRatingsEngineAttributes = function (ratingsEngine) {
            return (ratingsEngine && ratingsEngine.rule && ratingsEngine.rule.selectedAttributes ? ratingsEngine.rule.selectedAttributes : []);
        };

        this.selectRatingsEngineAttributes = function (ratingsEngineId, ratingModelId, attributes) {
            var deferred = $q.defer(),
                save = {
                    rule: {
                        id: ratingsEngineId,
                        selectedAttributes: attributes
                    }
                };

            $http({
                method: 'POST',
                url: this.host + '/ratingengines/' + ratingsEngineId + '/ratingmodels' + (ratingModelId ? '/' + ratingModelId : ''),
                data: save
            }).then(function (response) {
                var saved_attributes = flattenRatingsEngineAttributes(response.data);
                deferred.resolve(saved_attributes);
            });

            return deferred.promise;
        };
    });