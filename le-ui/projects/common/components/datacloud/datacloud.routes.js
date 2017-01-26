angular
.module('common.datacloud', [
    'common.datacloud.explorer',
    'common.datacloud.lookup',
    'common.datacloud.explorertabs'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.datacloud', {
            url: '/datacloud',
            resolve: {
                EnrichmentCount: function($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getCount().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                EnrichmentTopAttributes: function($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getAllTopAttributes().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                EnrichmentPremiumSelectMaximum: function($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getPremiumSelectMaximum().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
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
                pageTitle: 'Lattice Data Cloud'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    controller: 'ExplorerTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorertabs.component.html'
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
                pageTitle: 'Lattice Data Cloud'
            },
            resolve: {
                LookupResponse: function($q, LookupService, LookupStore) {
                    var deferred = $q.defer();
                    //var data = LookupStore.get('response');

                    LookupService.submit().then(function(data) {
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
                    templateUrl: '/components/datacloud/explorer/explorertabs.component.html'
                },
                "subsummary@": {
                    controller: function(LookupResponse, LookupStore) {
                        LookupStore.add('count', 0);//Object.keys(LookupResponse.enrichmentAttributeValues).length;
                        
                        this.store = LookupStore;
                        this.ldc_name = LookupResponse.companyInfo
                            ? LookupResponse.companyInfo.LDC_Name
                            : '';

                        this.elapsedTime = LookupStore.get('elapsedTime');
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
                section: 'lookup'
            },
            views: {
                "main@": {
                    resolve: {
                        EnrichmentAccountLookup: function($q, DataCloudStore, LookupResponse) {
                            var deferred = $q.defer();

                            deferred.resolve(LookupResponse.enrichmentAttributeValues || {});

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
                pageTitle: 'Lattice Data Cloud',
                section: 'browse'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    controller: 'ExplorerTabsController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorertabs.component.html'
                },
                "main@": {
                    resolve: {
                        // Note: this is needed for Account Lookup, dont remove!
                        EnrichmentAccountLookup: function() {
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
            url: '/insights/:category/:subcategory',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Lattice Data Cloud',
                section: 'insights'
            },
            views: {
                "main@": {
                    resolve: {
                        // Note: this is needed for Account Lookup, dont remove!
                        EnrichmentAccountLookup: function() {
                            return null;
                        }
                    },
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                } 
            }
        });
});