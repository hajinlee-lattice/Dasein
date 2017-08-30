angular
.module('common.datacloud', [
    'common.datacloud.explorer',
    'common.datacloud.lookup',
    'common.datacloud.explorertabs',
    'common.datacloud.analysistabs',
    'common.datacloud.query',
    'common.datacloud.query.advanced',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.run(function($rootScope, $state, DataCloudStore, DataCloudService) {
    $rootScope.$on('$stateChangeStart', function(event, toState, params, fromState, fromParams) {
        var states = {
            'home.segment.explorer': 'customer', 
            'home.segment.explorer.attributes': 'customer',
            'home.segment.explorer.query.advanced': 'customer',
            'home.model.analysis.explorer': 'customer',
            'home.model.analysis.explorer.attributes': 'customer',
            'home.model.analysis.explorer.query.advanced': 'customer',
            'home.datacloud.explorer': 'lattice',
            'home.datacloud.insights': 'lattice',
            'home.datacloud.lookup.form': 'lattice',
            'home.ratingsengine.wizard.segment.attributes': 'customer'
        };

        if (states[toState.name]) {
            if (DataCloudService.path != DataCloudService.paths[states[toState.name]]) {
                DataCloudService.path = DataCloudService.paths[states[toState.name]];

                // reset DataCloudStore if context changes
                DataCloudStore.init();
            }
        }
    });
})
.config(function($stateProvider) {
    var DataCloudResolve = {
        EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getCount().then(function(result) {
                DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                deferred.resolve(result.data);
            });

            return deferred.promise;
        }],
        Enrichments: ['$q', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', function($q, DataCloudStore, ApiHost, EnrichmentCount) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getAllEnrichmentsConcurrently(EnrichmentCount).then(function(result) {
                deferred.resolve(result);
            });

            return deferred.promise;
        }],
        EnrichmentTopAttributes: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getAllTopAttributes().then(function(result) {
                deferred.resolve(result['Categories'] || result || {});
            });

            return deferred.promise;
        }], 
        EnrichmentPremiumSelectMaximum: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getPremiumSelectMaximum().then(function(result) {
                deferred.resolve(result);
            });

            return deferred.promise;
        }],
        // below resolves are needed. Do not removed
        // override at child state when needed
        QueryRestriction: function() {
            return null;
        },
        CurrentConfiguration: function() {
            return null;
        }
    };

    $stateProvider
        .state('home.datacloud', {
            url: '/datacloud',
            resolve: DataCloudResolve,
            redirectTo: 'home.datacloud.explorer'
        })
        .state('home.datacloud.lookup', {
            url: '/lookup',
            redirectTo: 'home.datacloud.lookup.form'
        })
        .state('home.datacloud.lookup.form', {
            url: '/form',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Data Cloud Explorer'
            },
            views: {
                "summary@": {
                    controller: 'ExplorerTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/explorertabs.component.html'
                },
                "main@": {
                    controller: 'LookupController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/lookup/lookup.component.html'
                }
            }
        })
        .state('home.datacloud.lookup.tabs', {
            url: '/tabs',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Data Cloud Explorer'
            },
            resolve: {
                LookupResponse: function($q, LookupService, LookupStore, ApiHost) {
                    var deferred = $q.defer();

                    LookupService.submit(ApiHost).then(function(data) {
                        var current = new Date().getTime();
                        var old = LookupStore.get('timestamp');

                        LookupStore.add('elapsedTime', current - old);
                        LookupStore.add('response', data);

                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    controller: 'ExplorerTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/explorertabs.component.html'
                },
                "subsummary@": {
                    controller: function(LookupResponse, LookupStore, BrowserStorageUtility) {
                        LookupStore.add('count', 0);//Object.keys(LookupResponse.attributes).length;

                        this.store = LookupStore;
                        this.ldc_name = LookupResponse.companyInfo
                            ? LookupResponse.companyInfo.LDC_Name
                            : '';

                        this.elapsedTime = LookupStore.get('elapsedTime');

                        this.isInternalUser = false;
                        if (BrowserStorageUtility.getSessionDocument() != null && BrowserStorageUtility.getSessionDocument().User != null
                            && BrowserStorageUtility.getSessionDocument().User.AccessLevel != null) {
                            var accessLevel = BrowserStorageUtility.getSessionDocument().User.AccessLevel;

                            if (accessLevel == "INTERNAL_USER" || accessLevel == "INTERNAL_ADMIN" || accessLevel == "SUPER_ADMIN") {
                                this.isInternalUser = true;
                            }
                        }
                    },
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/lookup/tabs.component.html'
                }
            },
            redirectTo: 'home.datacloud.lookup.tabs.attr'
        })
        .state('home.datacloud.lookup.tabs.matching', {
            url: '/matching',
            views: {
                "main@": {
                    controller: function(LookupResponse, LookupStore) {
                        var vm = this;

                        angular.extend(vm, {
                            elapsedTime: LookupStore.get('elapsedTime'),
                            response: LookupResponse,
                            matchLogs: LookupStore.syntaxHighlight(LookupResponse.matchLogs)
                        });
                    },
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/lookup/matching.component.html'
                }
            }
        })
        .state('home.datacloud.lookup.tabs.attr', {
            url: '/attr/:category/:subcategory',
            params: {
                section: 'lookup',
                LoadingText: 'Looking up Company Profile data',
                category: {value: null, squash: true},
                subcategory: {value: null, squash: true}
            },
            views: {
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        })
        .state('home.datacloud.explorer', {
            url: '/explorer/:section/:category/:subcategory',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Data Cloud Explorer',
                LoadingText: 'Loading DataCloud Attributes',
                section: 'edit',
                category: {value: null, squash: true},
                subcategory: {value: null, squash: true}
            },
            resolve: {
                LookupResponse: function() {
                    return { attributes: null };
                }
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    controller: 'ExplorerTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/explorertabs.component.html'
                },
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        })
        .state('home.datacloud.insights', {
            url: '/tabs',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Data Cloud Explorer',
                LoadingText: 'Loading DataCloud Attributes',
                section: 'insights'
            },
            views: {
                "main@": {
                    resolve: {
                        LookupResponse: function($q, LookupService, LookupStore, ApiHost) {
                            var deferred = $q.defer();
                            
                            LookupService.submit(ApiHost).then(function(data) {
                                var current = new Date().getTime();
                                var old = LookupStore.get('timestamp');

                                LookupStore.add('elapsedTime', current - old);
                                LookupStore.add('response', data);

                                deferred.resolve(data);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        });

    var getState = function(type, overwrite) {
        var result = angular.extend({}, analysis[type], overwrite)

        return result;
    };

    var analysis = {
        main: {
            url: '/analysis/:segment',
            params: {
                segment: 'Create',
                reload: true
            },
            resolve: angular.extend({}, DataCloudResolve, {
                QueryRestriction: ['$stateParams', '$state', '$q', 'QueryStore', 'SegmentStore', function($stateParams, $state, $q, QueryStore, SegmentStore) {

                    var deferred = $q.defer(),
                        segmentName = $stateParams.segment,
                        modelId = $stateParams.modelId,
                        tenantName = $stateParams.tenantName;

                    // console.log("[resolve]     Restriction");

                    QueryStore.setupStore(null).then(function(){

                        if(segmentName === 'Create'){
                            deferred.resolve(QueryStore.getRestriction());
                        } else {
                            SegmentStore.getSegmentByName(segmentName).then(function(result) {
                                if (segmentName && !result) {
                                    if (modelId) {
                                        $state.go('home.model.segmentation', {modelId: modelId}, {notify: true, reload: true});
                                    } else {
                                        $state.go('home.segments', {tenantName: tenantName}, {notify: true, reload: true});
                                    }
                                } else {
                                    // console.log("[setup store]       ", result);

                                    return QueryStore.setupStore(result);
                                }
                            }).then(function() {
                                deferred.resolve(QueryStore.getRestriction());
                            });
                        }

                    });

                    return deferred.promise;                    
                }],
                Accounts: ['$q', '$stateParams', 'QueryStore', 'SegmentStore', function($q, $stateParams, QueryStore, SegmentStore) {
                    var deferred = $q.defer(),
                        segmentName = $stateParams.segment,
                        restriction = QueryStore.getRestriction();

                    if(segmentName === "Create"){
                        query = { 
                            'free_form_text_search': '',
                            'frontend_restriction': restriction,
                            'page_filter': {
                                'num_rows': 15,
                                'row_offset': 0
                            }
                        };
                        deferred.resolve( QueryStore.GetDataByQuery('accounts', query).then(function(data){ return data; }));
                    } else {
                        SegmentStore.getSegmentByName(segmentName).then(function(result) {
                            var segment = result;

                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': segment.frontend_restriction,
                                'page_filter': {
                                    'num_rows': 15,
                                    'row_offset': 0
                                }
                            };
                            deferred.resolve( QueryStore.GetDataByQuery('accounts', query, segment).then(function(data){ return data; }));
                        });
                    };

                    return deferred.promise;

                }],
                AccountsCount: ['$q', '$stateParams', 'QueryStore', 'SegmentStore', function($q, $stateParams, QueryStore, SegmentStore) {
                    
                    var deferred = $q.defer(),
                        segmentName = $stateParams.segment,
                        restriction = QueryStore.getRestriction();

                    if(segmentName === "Create"){
                        query = { 
                            'free_form_text_search': '',
                            'frontend_restriction': restriction,
                            'page_filter': {
                                'num_rows': 15,
                                'row_offset': 0
                            }
                        };
                        deferred.resolve( QueryStore.GetCountByQuery('accounts', query).then(function(data){ return data; }));
                    } else {
                        SegmentStore.getSegmentByName(segmentName).then(function(result) {
                            var segment = result;

                            // console.log("[resolve] AccountsCount", segment);

                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': segment.frontend_restriction,
                                'page_filter': {
                                    'num_rows': 15,
                                    'row_offset': 0
                                }
                            };
                            deferred.resolve( QueryStore.GetCountByQuery('accounts', query).then(function(data){ return data; }));
                        });
                    };
                        
                    return deferred.promise;
                }],
                CountWithoutSalesForce: [function(){
                    return null;
                }],
                Config: [function(){
                    return null;
                }],
            }),
            redirectTo: 'home.model.analysis.explorer',
            views: {
                "summary@": {
                    controller: 'AnalysisTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/analysistabs/analysistabs.component.html'
                }
            }
        },
        explorer: { // no view, just puts attributes and query under same parent state
            url: '/explorer',
            redirectTo: 'home.model.analysis.explorer.attributes'
        },
        attributes: {
            url: '/attributes/:category/:subcategory',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Analysis',
                section: 'segment.analysis',
                category: {value: null, squash: true},
                subcategory: {value: null, squash: true}
            },
            views: {
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        },
        query: {
            url: '/query',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Analysis',
                section: 'query'
            },
            views: {
                "main@": {
                    controller: 'QueryBuilderCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/builder/querybuilder.component.html'
                }
            }
        },
        advquery: {
            url: '/advanced/:state',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Analysis',
                state: 'tree1',
                section: 'query'

            },
            resolve: {
                Cube: ['$q', 'DataCloudStore', function($q, DataCloudStore){
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        if (result.data) {
                            deferred.resolve(result.data.Stats);
                        }
                    });
                    
                    return deferred.promise;
                }]
            },
            views: {
                "main@": {
                    controller: 'AdvancedQueryCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                }
            }
        },
        accounts: {
            url: '/accounts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Accounts'
            },
            resolve: {
                AccountsCount: ['$q', 'QueryStore', function($q, QueryStore) {
                    var deferred = $q.defer(),
                        segment = QueryStore.getSegment(),
                        restriction = QueryStore.getRestriction();

                        if (segment === null) {                     
                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': restriction,
                                'page_filter': {
                                    'num_rows': 10,
                                    'row_offset': 0
                                }
                            };
                        } else {
                            query = { 
                                'free_form_text_search': '',
                                'frontend_restriction': segment.frontend_restriction,
                                'page_filter': {
                                    'num_rows': 10,
                                    'row_offset': 0
                                }
                            };
                        };

                    deferred.resolve( QueryStore.GetCountByQuery('accounts', query, segment).then(function(data){ return data; }));
                    return deferred.promise;
                }],
                CountWithoutSalesForce: [function(){
                    return null;
                }],
                Config: [function(){
                    return null;
                }],
            },
            views: {
                "main@": {
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        },
        contacts: {
            url: '/contacts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Contacts'
            },
            views: {
                "main@": {
                    resolve: {
                        CountMetadata: ['$q', 'QueryStore', function($q, QueryStore) {
                            var deferred = $q.defer();
                            deferred.resolve(QueryStore.getCounts().contacts);
                            return deferred.promise;
                        }],
                        Config: [function(){
                            return null;
                        }],
                    },
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        },
        abstract: {
            abstract: true
        }
    };

    $stateProvider
        .state('home.model.analysis', getState('main'))
        .state('home.model.analysis.explorer', getState('explorer', {
            resolve: {
                CurrentConfiguration: ['$q', '$stateParams', 'ModelRatingsService', function($q, $stateParams, ModelRatingsService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.MostRecentConfiguration(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }],
                LookupResponse: [ function() {
                    return { attributes: null };
                }]
            }
        }))
        .state('home.model.analysis.explorer.attributes', getState('attributes'))
        .state('home.model.analysis.explorer.query', getState('query'))
        .state('home.model.analysis.explorer.query.advanced', getState('advquery'))
        .state('home.model.analysis.accounts', getState('accounts'))
        .state('home.model.analysis.contacts', getState('contacts'))
        .state('home.segment', getState('main', {
            url: '/segment/:segment',
            params: {
                section: 'segment.analysis'
            },
            redirectTo: 'home.segment.explorer'
        }))
        .state('home.segment.explorer', getState('explorer', {
            params: {
                section: 'segment.analysis'
            },
            redirectTo: 'home.segment.explorer.attributes'
        }))
        .state('home.segment.explorer.attributes', getState('attributes', {
            params: {
                segment: 'segment.name',
                pageTitle: 'My Data',
                pageIcon: 'ico-analysis',
                section: 'segment.analysis',
                category: { value: null, squash: true },
                subcategory: { value: null, squash: true }
            },
            resolve: {
                LookupResponse: [ function() {
                    return { attributes: null };
                }]
            }
        }))
        .state('home.segment.explorer.query', getState('query'))
        .state('home.segment.explorer.query.advanced', getState('advquery'))
        .state('home.segment.accounts', getState('accounts'))
        .state('home.segment.contacts', getState('contacts'));
});
