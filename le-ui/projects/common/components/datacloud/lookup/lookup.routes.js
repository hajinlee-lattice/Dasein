angular
.module('common.datacloud.lookup')
.config(function($stateProvider) {
    $stateProvider
        .state('home.datacloud.lookup', {
            url: '/lookup',
            onExit: ['LookupStore', function(LookupStore) {
                LookupStore.reset();
            }],
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
            onExit: ['DataCloudStore', 'Enrichments', function(DataCloudStore, Enrichments) {
                DataCloudStore.setEnrichments(Enrichments, false);
            }],
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

                        this.hideLookupAttributesCount = LookupStore.hideLookupResponse(LookupResponse);

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
                category: '',
                subcategory: ''
            },
            views: {
                "main@": {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
            }
        });
});