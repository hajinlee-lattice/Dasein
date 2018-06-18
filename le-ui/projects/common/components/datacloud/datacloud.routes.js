angular
.module('common.datacloud', [
    'common.datacloud.explorer',
    'common.datacloud.lookup',
    'common.datacloud.valuepicker',
    'common.datacloud.tabs.datacloud',
    'common.datacloud.tabs.mydata',
    'common.datacloud.tabs.subheader',
    'common.datacloud.targettabs',
    'common.datacloud.query',
    'common.datacloud.explorer.export'
])
.run(function($transitions) {
    var setMetadataApiContext = function(trans, context) {
        var service = trans.injector().get('DataCloudService'),
            store = trans.injector().get('DataCloudStore'),
            to = trans.$to();
        
        if (service.path !== service.paths[context]) {
            service.path = service.paths[context];
            store.init();
        }
    };

    var states = {
        'home.datacloud.*': 'lattice',
        'home.segment.*': 'customer',
        'home.segments*': 'customer',
        'home.ratingsengine.*': 'customer'
    };

    Object.keys(states).forEach(function(state) {
        var context = states[state];
        
        $transitions.onStart({ entering: state }, function(trans) {
            setMetadataApiContext(trans, context);
        });
    });
})
.provider('DataCloudResolves', function DataCloudResolvesProvider() {
    this.$get = function DataCloudResolvesFactory() {
        return {
            "main": {
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
                EnrichmentTopAttributes: ['$q', '$state', '$stateParams', 'DataCloudStore', 'ApiHost', 'EnrichmentCount', 'QueryStore', 'FeatureFlagService', function($q, $state, $stateParams, DataCloudStore, ApiHost, EnrichmentCount, QueryStore, FeatureFlagService) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    FeatureFlagService.GetAllFlags().then(function(result) {
                        var flags = FeatureFlagService.Flags();

                        if (FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL)) {
                            var query = {
                               "free_form_text_search":"",
                               "account_restriction":{
                                  "restriction":{
                                     "logicalRestriction":{
                                        "operator":"AND",
                                        "restrictions":[]
                                     }
                                  }
                               },
                               "contact_restriction":{
                                  "restriction":{
                                     "logicalRestriction":{
                                        "operator":"AND",
                                        "restrictions":[]
                                     }
                                  }
                               },
                               "restrict_without_sfdcid":false,
                               "page_filter":{
                                  "num_rows":10,
                                  "row_offset":0
                               }
                            };

                            QueryStore.getEntitiesCounts(query).then(function(result) {
                                if (result && (result.Account != 0 || result.Contact != 0)) {
                                    DataCloudStore.getAllTopAttributes().then(function(result) {
                                        deferred.resolve(result['Categories'] || result || {});
                                    });
                                } else {
                                    $state.go('home.nodata', { 
                                        tenantName: $stateParams.tenantName,
                                        segment: $stateParams.segment
                                    });
                                }
                            });
                        } else {

                            if (EnrichmentCount !== 0) { //PLS-5894
                                DataCloudStore.getAllTopAttributes().then(function(result) {
                                    deferred.resolve(result['Categories'] || result || {});
                                });
                            }
                        }
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
                EnrichmentSelectMaximum: ['$q', 'DataCloudStore', function($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getSelectMaximum().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }],
                // below resolves are needed. Do not removed
                // override at child state when needed
                LookupResponse: [function() {
                    return { attributes: null };
                }],
                QueryRestriction: [function() {
                    return null;
                }],
                CurrentConfiguration: [function() {
                    return null;
                }],
                RatingsEngineModels: [function() {
                    return null;
                }],
                RatingsEngineStore: [function() {
                    return null;
                }]
            }
        };
    };
})
.config(function($stateProvider, DataCloudResolvesProvider) {
    var DataCloudResolves = DataCloudResolvesProvider.$get().main;

    $stateProvider
        .state('home.segments', {
            url: '/segments',
            params: {
                pageTitle: 'Segments',
                pageIcon: 'ico-segments',
                edit: null
            },
            resolve: angular.extend({}, DataCloudResolves, {
                SegmentsList: ['$q', 'SegmentService', 'SegmentStore', function($q, SegmentService, SegmentStore) {
                    var deferred = $q.defer();

                    SegmentService.GetSegments().then(function(result) {
                        SegmentStore.setSegments(result);
                        deferred.resolve(result);
                    });

                    return deferred.promise;                  
                }],
                Cube: ['$q', 'DataCloudStore', function($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        if (result.data) {
                            deferred.resolve(result.data);
                        }
                    });
                
                    return deferred.promise;
                }]
            }),
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: 'SegmentationListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/segments/views/SegmentationListView.html'
                }
            }
        })
        .state('home.datacloud', {
            url: '/datacloud',
            resolve: DataCloudResolves,
            redirectTo: 'home.datacloud.explorer'
        })
        .state('home.datacloud.explorer', {
            url: '/explorer/:section/:category/:subcategory',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Data Cloud Explorer',
                LoadingText: 'Loading DataCloud Attributes',
                section: 'edit',
                category: { dynamic: true, value: '' },
                subcategory: { dynamic: true, value: '' }
            },
            resolve: {
                LookupResponse: function() {
                    return { attributes: null };
                }
            },
            views: {
                "summary@": {
                    controller: 'DataCloudTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/datacloud/datacloud.component.html'
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
                section: 'insights',
                category: { dynamic: true, value: '' },
                subcategory: { dynamic: true, value: '' }
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
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        })
        .state('home.segment', {
            url: '/segment/:segment',
            params: {
                section: 'segment.analysis',
                segment: 'Create',
                reload: true
            },
            onExit: ['DataCloudStore', 'QueryStore', function(DataCloudStore, QueryStore) {
                var enrichments = DataCloudStore.enrichments.filter(function (item) {
                    return item.SegmentChecked;
                });

                enrichments.forEach(function(item) {
                    delete item.SegmentChecked;
                });

                QueryStore.clear();
            }],
            resolve: angular.extend({}, DataCloudResolves, {
                QueryRestriction: ['$stateParams', '$state', '$q', 'QueryStore', 'SegmentStore', function($stateParams, $state, $q, QueryStore, SegmentStore) {
                    var resolveQueryRestriction = function() {
                        var accountRestriction = QueryStore.getAccountRestriction(),
                            contactRestriction = QueryStore.getContactRestriction();

                        deferred.resolve({
                            accountRestrictions: accountRestriction,
                            contactRestrictions: contactRestriction
                        });
                    };

                    var deferred = $q.defer(),
                        segmentName = $stateParams.segment,
                        modelId = $stateParams.modelId,
                        tenantName = $stateParams.tenantName;

                    QueryStore.setupStore(null);
                    
                    if (segmentName === 'Create') {
                        resolveQueryRestriction();
                    } else {
                        SegmentStore.getSegmentByName(segmentName).then(function(result) {
                            if (segmentName && !result) {
                                if (modelId) {
                                    $state.go('home.model.segmentation', {modelId: modelId}, {notify: true, reload: true});
                                } else {
                                    $state.go('home.segments', {tenantName: tenantName}, {notify: true, reload: true});
                                }
                            } else {
                                return QueryStore.setupStore(result);
                            }
                        }).then(function() {
                            resolveQueryRestriction();
                        });
                    }

                    return deferred.promise;                   
                }],
                AccountsCoverage: [function(){
                    return null;
                }],
                Config: [function(){
                    return null;
                }]
            }),
            views: {
                "summary@": {
                    controller: 'MyDataTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/mydata/mydata.component.html'
                },
                "subsummary@": {
                    controller: 'SubHeaderTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/subheader/subheader.component.html'
                }
            },
            redirectTo: 'home.segment.explorer'
        })
        .state('home.segment.explorer', {
            url: '/explorer',
            params: {
                section: 'segment.analysis'
            },
            redirectTo: 'home.segment.explorer.attributes'
        })
        .state('home.segment.explorer.attributes', {
            url: '/attributes/:category/:subcategory',
            params: {
                segment: 'segment.name',
                pageTitle: 'My Data',
                pageIcon: 'ico-analysis',
                section: 'segment.analysis',
                category: { dynamic: true, value: '' },
                subcategory: { dynamic: true, value: '' }
            },
            onEnter: ['$stateParams', 'SegmentStore', 'BackStore', function($stateParams, SegmentStore, BackStore) {
                var name = $stateParams.segment;

                BackStore.setBackState('home.segments');
                if('Create' === name){
                    BackStore.setBackLabel($stateParams.segment);
                    BackStore.setHidden(true);
                } else {
                    SegmentStore.getSegmentByName(name).then(function(result) {
                        BackStore.setBackLabel(result.display_name);
                        BackStore.setHidden(false);
                    });
                    
                }
            }],
            resolve: {
                LookupResponse: [ function() {
                    return { attributes: null };
                }],
                RerouteToNoData: ['$state', '$stateParams', 'EnrichmentCount', 'QueryService', 'QueryStore', function($state, $stateParams, EnrichmentCount, QueryService, QueryStore) {
                    var query = {};
                    if (EnrichmentCount == 0 && QueryStore.counts.accounts.value == 0 && QueryStore.counts.contacts.value == 0) {
                        QueryService.GetEntitiesCounts(query).then(function(result) {
                            if ((!result || (result.Account == 0 && result.Contact == 0))) {
                                $state.go('home.nodata', { 
                                    tenantName: $stateParams.tenantName,
                                    segment: $stateParams.segment
                                });
                            } else {
                                QueryStore.counts.accounts.value = result.Account;
                                QueryStore.counts.contacts.value = result.Contact;
                            }
                        });
                    }
                }]
            },
            views: {
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                },
                'header.back@': 'backNav'
            }
        })
        .state('home.nodata', {
            url: '/nodata',
            params: {
                pageTitle: 'My Data',
                pageIcon: 'ico-analysis'
            },
           resolve: {
                AttributesCount: ['$q', '$state', 'DataCloudStore', 'ApiHost', function($q, $state, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getAttributesCount().then(function(result) {
                        DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                        deferred.resolve(result.data);
                    });
                    return deferred.promise;
                }]
            },
            views: {
                "main@": {
                    templateUrl: '/components/datacloud/explorer/nodata/nodata.component.html'
                }
            }
        })
        .state('home.exportSegment', {            
            url: '/export/:exportID',
            params: {
                pageTitle: 'Export Segment',
                pageIcon: 'ico-analysis',
                section: 'segment.analysis'               
            },
            resolve: {
                SegmentExport: ['$q', '$stateParams', 'SegmentService', function($q, $stateParams, SegmentService) {
                    var deferred = $q.defer();

                    var exportId = $stateParams.exportID;

                    SegmentService.GetSegmentExportByExportId(exportId).then(function(result) {
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                }]
            },
            views: {
                "main@": {
                    controller: 'SegmentExportController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/segmentexport/segmentexport.component.html'
                }
            }
        });
});
