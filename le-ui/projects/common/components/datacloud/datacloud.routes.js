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
    'common.datacloud.explorer.export',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.run(function($rootScope, $state, DataCloudStore, DataCloudService) {
    $rootScope.$on('$stateChangeStart', function(event, toState, params, fromState, fromParams) {
        var states = {
            'home.segments': 'customer',
            'home.segment.explorer': 'customer', 
            'home.segment.explorer.attributes': 'customer',
            'home.segment.explorer.builder': 'customer',
            'home.segment.accounts': 'customer',
            'home.segment.contacts': 'customer',
            'home.model.analysis.explorer': 'customer',
            'home.model.analysis.explorer.attributes': 'customer',
            'home.model.analysis.explorer.builder': 'customer',
            'home.model.analysis.accounts': 'customer',
            'home.model.analysis.contacts': 'customer',
            'home.ratingsengine.rulesprospects.segment.attributes': 'customer',
            'home.ratingsengine.rulesprospects.segment.attributes.rules': 'customer',
            'home.ratingsengine.dashboard.segment.attributes': 'customer',
            'home.ratingsengine.dashboard.segment.attributes.add': 'customer',
            'home.ratingsengine.dashboard.segment.attributes.rules': 'customer',
            'home.datacloud.explorer': 'lattice',
            'home.datacloud.insights': 'lattice',
            'home.datacloud.lookup.form': 'lattice'
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
    }
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
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
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
                    controller: 'DataCloudTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/datacloud/datacloud.component.html'
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
                    controller: 'DataCloudTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/tabs/datacloud/datacloud.component.html'
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
                LoadingText: 'Looking up Company Profile data'
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
                section: 'edit'
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
                section: 'insights'
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
        });

    var getState = function(type, overwrite) {
        return angular.extend({}, analysis[type], overwrite);
    };

    var analysis = {
        main: {
            url: '/analysis/:segment',
            params: {
                segment: 'Create',
                reload: true
            },
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
            redirectTo: 'home.model.analysis.explorer'
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
                section: 'segment.analysis'
            },
            views: {
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        },
        builder: {
            url: '/builder',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Query Builder'
            },
            resolve: {
                Cube: ['$q', 'DataCloudStore', function($q, DataCloudStore){
                    var deferred = $q.defer();

                    DataCloudStore.getCube().then(function(result) {
                        if (result.data) {
                            deferred.resolve(result.data);
                        }
                    });
                    
                    return deferred.promise;
                }],
                RatingEngineModel: [function() {
                    return null;
                }],
                CurrentRatingEngine: [function() {
                    return null;
                }],
            },
            views: {
                "main@": {
                    controller: 'AdvancedQueryCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/advanced/advanced.component.html'
                }
            }
        },
        enumpicker: {
            url: '/picker/:entity/:fieldname',
            resolve: {
                PickerBuckets: ['$q', '$stateParams', 'QueryTreeService', 'DataCloudStore', function($q, $stateParams, QueryTreeService, DataCloudStore){
                    var deferred = $q.defer();
                    var entity = $stateParams.entity;
                    var fieldname = $stateParams.fieldname;

                    QueryTreeService.getPickerCubeData(entity, fieldname).then(function(result) {
                        deferred.resolve(result.data);
                    });
                    
                    return deferred.promise;
                }]
            },
            views: {
                "main@": {
                    controller: 'ValuePickerController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/picker/picker.component.html'
                }
            }
        },
        nodata: {
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
        },
        accounts: {
            url: '/accounts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Accounts'
            },
            resolve: {
                Accounts: ['$q', '$stateParams', 'QueryStore', 'SegmentStore', function($q, $stateParams, QueryStore, SegmentStore) {
                    var deferred = $q.defer(),
                        segmentName = $stateParams.segment,
                        accountRestriction = QueryStore.getAccountRestriction(),
                        contactRestriction = QueryStore.getContactRestriction();

                    if(segmentName === "Create"){
                        query = { 
                            'free_form_text_search': '',
                            'account_restriction': accountRestriction,
                            'contact_restriction': contactRestriction,
                            'preexisting_segment_name': segmentName,
                            'page_filter': {
                                'num_rows': 10,
                                'row_offset': 0
                            }
                        };

                        deferred.resolve( QueryStore.GetDataByQuery('accounts', query).then(function(data){ return data; }));
                    } else {

                        SegmentStore.getSegmentByName(segmentName).then(function(result) {

                            var segment = result;
                            query = { 
                                'free_form_text_search': '',
                                'account_restriction': segment.account_restriction,
                                'contact_restriction': segment.contact_restriction,
                                'preexisting_segment_name': segmentName,
                                'page_filter': {
                                    'num_rows': 10,
                                    'row_offset': 0
                                }
                            };
                            deferred.resolve( QueryStore.GetDataByQuery('accounts', query).then(function(data){ return data; }));
                        });
                    };

                    return deferred.promise;

                }],
                AccountsCoverage: [function(){
                    return null;
                }],
                NoSFIdsCount: [function(){
                    return null;
                }],
                Contacts: [function(){
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
                        Contacts: ['$q', '$stateParams', 'QueryStore', 'SegmentStore', function($q, $stateParams, QueryStore, SegmentStore) {
                            var deferred = $q.defer(),
                                segmentName = $stateParams.segment,
                                accountRestriction = QueryStore.getAccountRestriction(),
                                contactRestriction = QueryStore.getContactRestriction();

                            if(segmentName === "Create"){
                                query = { 
                                    'free_form_text_search': '',
                                    'account_restriction': accountRestriction,
                                    'contact_restriction': contactRestriction,
                                    'preexisting_segment_name': segmentName,
                                    'page_filter': {
                                        'num_rows': 10,
                                        'row_offset': 0
                                    }
                                };
                                deferred.resolve( QueryStore.GetDataByQuery('contacts', query).then(function(data){ return data.data; }));
                            } else {
                                SegmentStore.getSegmentByName(segmentName).then(function(result) {
                                    var segment = result;

                                    query = { 
                                        'free_form_text_search': '',
                                        'account_restriction': segment.account_restriction,
                                        'contact_restriction': segment.contact_restriction,
                                        'preexisting_segment_name': segmentName,
                                        'page_filter': {
                                            'num_rows': 10,
                                            'row_offset': 0
                                        }
                                    };
                                    deferred.resolve( QueryStore.GetDataByQuery('contacts', query).then(function(data){ return data.data; }));
                                });
                            };

                            return deferred.promise;

                        }],
                        Accounts: [function(){
                            return null;
                        }],
                        NoSFIdsCount: [function(){
                            return null;
                        }],
                        AccountsCoverage: [function(){
                            return null;
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
        },
        exportSegment: {            
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
        .state('home.model.analysis.explorer.builder', getState('builder'))
        .state('home.model.analysis.accounts', getState('accounts'))
        .state('home.model.analysis.contacts', getState('contacts'))
        .state('home.segment', getState('main', {
            url: '/segment/:segment',
            onExit: function(DataCloudStore) {
                var enrichments = DataCloudStore.enrichments.filter(function (item) {
                    return item.SegmentChecked;
                });
                enrichments.forEach(function(item) {
                    delete item.SegmentChecked;
                })
            },
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
                section: 'segment.analysis'
            },
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
            }
        })) 
        .state('home.segment.explorer.builder', getState('builder'))
        .state('home.segment.explorer.enumpicker', getState('enumpicker'))
        .state('home.nodata', getState('nodata'))
        .state('home.segment.accounts', getState('accounts'))
        .state('home.segment.contacts', getState('contacts'))
        .state('home.exportSegment', getState('exportSegment'));
});
