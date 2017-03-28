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
        }]
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
                    //var data = LookupStore.get('response');

                    LookupService.submit(ApiHost).then(function(data) {
                        //console.log('response', data);
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
                    resolve: {
                        EnrichmentAccountLookup: function($q, DataCloudStore, LookupResponse) {
                            var deferred = $q.defer();

                            deferred.resolve(LookupResponse.attributes || {});

                            return deferred.promise;
                        }
                    },
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
                section: 'edit',
                category: {value: null, squash: true},
                subcategory: {value: null, squash: true}
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
                    resolve: {
                        // Note: this is needed for Account Lookup, dont remove!
                        EnrichmentAccountLookup: function() {
                            return null;
                        },
                        AnalysisLookup: function() {
                            return null;
                        }
                    },
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
                        },
                        EnrichmentAccountLookup: function($q, DataCloudStore, LookupResponse) {
                            var deferred = $q.defer();

                            deferred.resolve(LookupResponse.attributes || {});

                            return deferred.promise;
                        },
                        AnalysisLookup: function() {
                            return null;
                        }
                    },
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        })
        .state('home.model.analysis', {
            url: '/analysis?segment&create',
            params: {
                segment: null,
                create: 'true'
            },
            resolve: angular.extend(DataCloudResolve, {
                QueryRestriction: ['$state', '$stateParams', '$q', 'QueryStore', 'SegmentStore', function($state, $stateParams, $q, QueryStore, SegmentStore) {
                    var deferred = $q.defer();
                    var segmentName = $stateParams.segment;
                    if (segmentName) {
                        SegmentStore.getSegmentByName(segmentName).then(function(result) {
                            QueryStore.setSegment(result);
                            deferred.resolve(QueryStore.getRestriction());
                        }).catch(function(error) {
                            $state.go('home.model.analysis', {segment: null, create: true});
                        });
                    } else {
                        QueryStore.setSegment(null);
                        deferred.resolve(QueryStore.getRestriction());
                    }

                    return deferred.promise;
                }],
                SegmentServiceProxy: ['SegmentService', 'QueryStore', function(SegmentService, QueryStore) {
                    var CreateOrUpdateSegment = function() {
                        var segment = QueryStore.getSegment();
                        if (segment === null) {
                            var ts = new Date().getTime();
                            segment = {
                                "name": "segment" + ts,
                                "display_name": "segment" + ts,
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
        })
        .state('home.model.analysis.explorer', { // no view, just puts attributes and query under same parent state
            url: '/explorer',
            redirectTo: 'home.model.analysis.explorer.attributes'
        })
        .state('home.model.analysis.explorer.attributes', {
            url: '/attributes/:category/:subcategory',
            params: {
                pageIcon: 'ico-performance',
                pageTitle: 'Analysis',
                section: 'segment.analysis',
                category: {value: null, squash: true},
                subcategory: {value: null, squash: true}
            },
            views: {
                "main@": {
                    resolve: {
                        // Note: this is needed for Account Lookup, dont remove!
                        EnrichmentAccountLookup: function() {
                            return null;
                        },
                        AnalysisLookup: function($q) {
                            return false;
                            // load default segment from API
                            var deferred = $q.defer();

                            //DataCloudStore.getCount().then(function(result) {
                                deferred.resolve(null);
                            //});

                            return deferred.promise;
                        }
                    },
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        })
        .state('home.model.analysis.explorer.query', {
            url: '/query',
            params: {
                pageIcon: 'ico-performance',
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
        })
        .state('home.model.analysis.accounts', {
            url: '/accounts',
            params: {
                pageIcon: 'ico-segment',
                pageTitle: 'Accounts'
            },
            views: {
                "main@": {
                    resolve: {
                        Columns: function ($q, QueryStore) {
                            var deferred = $q.defer();
                            deferred.resolve([]);
                            return deferred.promise;
                        },
                        Count: function ($q, QueryStore) {
                           var deferred = $q.defer();
                           deferred.resolve(QueryStore.getCount('accounts'));
                           return deferred.promise;
                        }
                    },
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        })
        .state('home.model.analysis.contacts', {
            url: '/contacts',
            params: {
                pageIcon: 'ico-segment',
                pageTitle: 'Contacts'
            },
            views: {
                "main@": {
                    resolve: {
                        Columns: function ($q, QueryStore) {
                            var deferred = $q.defer();
                            deferred.resolve([]);
                            return deferred.promise;
                        },
                        Count: function ($q, QueryStore) {
                           var deferred = $q.defer();
                           deferred.resolve(QueryStore.getCount('contacts'));
                           return deferred.promise;
                        }
                    },
                    controller: 'QueryResultsCtrl',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/query/results/queryresults.component.html'
                }
            }
        });
});
