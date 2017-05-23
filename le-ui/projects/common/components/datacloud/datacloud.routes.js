angular
.module('common.datacloud', [
    'common.datacloud.explorer',
    'common.datacloud.lookup',
    'common.datacloud.explorertabs',
    'common.datacloud.analysistabs',
    'common.datacloud.query',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.config(function($stateProvider) {
    var DataCloudResolve = {
        EnrichmentCount: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getCount().then(function(result) {
                DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                deferred.resolve(result);
            });

            return deferred.promise;
        }],
        EnrichmentTopAttributes: ['$q', 'DataCloudStore', 'ApiHost', function($q, DataCloudStore, ApiHost) {
            var deferred = $q.defer();

            DataCloudStore.setHost(ApiHost);

            DataCloudStore.getAllTopAttributes().then(function(result) {
                deferred.resolve(result || {});
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
        SegmentServiceProxy: function() {
            return null;
        },
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
                segment: 'Create'
            },
            resolve: angular.extend({}, DataCloudResolve, {
                LoadStub: ['QueryStore', function(QueryStore) {
                    return QueryStore.loadData();
                }],
                QueryRestriction: ['$stateParams', '$state', '$q', 'QueryStore', 'SegmentStore', function($stateParams, $state, $q, QueryStore, SegmentStore) {
                    var deferred = $q.defer();

                    var segmentName = $stateParams.segment;
                    var isCreateNew = segmentName === 'Create';
                    var modelId = $stateParams.modelId;
                    var tenantName = $stateParams.tenantName;

                    if (isCreateNew) {
                        QueryStore.setupStore(null);
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
                                return QueryStore.setupStore(result);
                            }
                        }).then(function() {
                            deferred.resolve(QueryStore.getRestriction());
                        });
                    }

                    return deferred.promise;
                }],
                SegmentServiceProxy: ['SegmentService', 'QueryStore', function(SegmentService, QueryStore) {
                    var CreateOrUpdateSegment = function() {
                        var segment = QueryStore.getSegment();
                        if (segment === null) {
                            var ts = new Date().getTime();
                            segment = {
                                'name': 'segment' + ts,
                                'display_name': 'segment' + ts,
                            };
                        }
                        segment.simple_restriction = QueryStore.getRestriction();

                        return SegmentService.CreateOrUpdateSegment(segment);
                    };

                    return {
                        CreateOrUpdateSegment: CreateOrUpdateSegment
                    };
                }]
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
                section: 'query',
            },
            views: {
                "main@": {
                    controller: 'QueryBuilderCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/builder/querybuilder.component.html'
                }
            }
        },
        xaccounts: {
            url: '/x/accounts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Accounts'
            },
            views: {
                "main@": {
                    resolve: {
                        CountMetadata: ['$q', 'QueryStore', function($q, QueryStore) {
                            var deferred = $q.defer();
                            deferred.resolve(QueryStore.getCounts().accounts);
                            return deferred.promise;
                        }]
                    },
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        },
        xcontacts: {
            url: '/x/contacts',
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
                        }]
                    },
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        },
        accounts: {
            url: '/accounts',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Accounts'
            },
            views: {
                "main@": {
                    resolve: {
                        CountMetadata: ['$q', 'QueryStore', function($q, QueryStore) {
                            var deferred = $q.defer();
                            deferred.resolve(QueryStore.getCounts().accounts);
                            return deferred.promise;
                        }],
                        Columns: ['QueryStore', function(QueryStore) {
                            return QueryStore.columns.accounts;
                        }],
                        Records: ['QueryStore', function(QueryStore) {
                            return QueryStore.getRecordsForUiState('accounts');
                        }]
                    },
                    controller: 'QueryResultsStubCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/stub/queryresults.component.html'
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
                        Columns: ['QueryStore', function(QueryStore) {
                            return QueryStore.columns.contacts;
                        }],
                        Records: ['QueryStore', function(QueryStore) {
                            return QueryStore.getRecordsForUiState('contacts');
                        }]
                    },
                    controller: 'QueryResultsStubCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/stub/queryresults.component.html'
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
        .state('home.model.analysis.x', getState('abstract'))
        .state('home.model.analysis.x.accounts', getState('xaccounts'))
        .state('home.model.analysis.x.contacts', getState('xcontacts'))
        // stub for demo
        .state('home.model.analysis.accounts', getState('accounts'))
        .state('home.model.analysis.contacts', getState('contacts'))

        .state('home.segment', getState('main', {
            url: '/segment/:segment',
            redirectTo: 'home.segment.explorer'
        }))
        .state('home.segment.explorer', getState('explorer', {
            redirectTo: 'home.segment.explorer.attributes'
        }))
        .state('home.segment.explorer.attributes', getState('attributes', {
            params: {
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
        .state('home.segment.x', getState('abstract'))
        .state('home.segment.x.accounts', getState('xaccounts'))
        .state('home.segment.x.contacts', getState('xcontacts'))
        // stub for demo
        .state('home.segment.accounts', getState('accounts'))
        .state('home.segment.contacts', getState('contacts'));
});
