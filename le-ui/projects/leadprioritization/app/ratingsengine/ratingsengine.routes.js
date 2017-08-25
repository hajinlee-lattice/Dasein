angular
.module('lp.ratingsengine', [
    'common.wizard',
    'lp.ratingsengine.ratingslist',
    'lp.ratingsengine.dashboard',
    'lp.ratingsengine.wizard.segment',
    'lp.ratingsengine.wizard.attributes'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine', {
            url: '/ratings_engine',
            redirectTo: 'home.ratingsengine.ratingslist'
        })
        .state('home.ratingsengine.ratingslist', {
            url: '/ratings',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Ratings Engine'
            },
            views: {
                "main@": {
                    controller: 'RatingsEngineRatingsList',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingslist/ratingslist.component.html'
                }
            }
        })
        .state('home.ratingsengine.dashboard', {
            url: '/dashboard/:rating_id',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Ratings Engine'
            },
            views: {
                "navigation@": {
                    controller: function($scope, $stateParams, $state, $rootScope) {
                        $scope.stateName = function() {
                            return $state.current.name;
                        }
                        $rootScope.$broadcast('header-back', { 
                            path: '^home.rating.dashboard',
                            displayName: 'Rating name',
                            sref: 'home.ratingsengine'
                        });
                    },
                    templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                },
                'main@': {
                    controller: 'RatingsEngineDashboard',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/dashboard/dashboard.component.html'
                }
            }
        })
        .state('home.ratingsengine.wizard', {
            url: '/wizard/:rating_id',
            resolve: {
                WizardValidationStore: function(RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function() {
                    return 'ratingsengine';
                },
                WizardProgressItems: function(RatingsEngineStore) {
                    return [
                        { label: 'Segment', state: 'segment' },
                        { label: 'Attributes', state: 'segment.attributes' }
                    ];
                }
            },
            views: {
                'summary@': {
                    controller: function($scope, RatingsEngineStore) {
                        $scope.$on('$destroy', function () {
                            RatingsEngineStore.clear();
                        });
                    }
                },
                'main@': {
                    resolve: {
                        WizardHeaderTitle: function() {
                            return 'Create Rating Engine';
                        },
                        WizardContainerId: function() {
                            return 'ratingsengine';
                        }
                    },
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                },
                'wizard_progress@home.ratingsengine.wizard': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls@home.ratingsengine.wizard': {
                    resolve: {
                        WizardControlsOptions: function() {
                            return { backState: 'home.ratingsengine', nextState: 'home.ratingsengine' };
                        }
                    },
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.ratingsengine.wizard.segment'
        })
        .state('home.ratingsengine.wizard.segment', {
            url: '/segment',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Ratings Engine',
                section: 'wizard.segment'
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'RatingsEngineSegment',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/segment/segment.component.html'
                }
            },
        })
        .state('home.ratingsengine.wizard.segment.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Create Ratings Engine',
                section: 'wizard.attributes',
                gotoNonemptyCategory: true
            },
            resolve: {
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
                SegmentServiceProxy: function() {
                    return null;
                },
                QueryRestriction: function() {
                    return null;
                },
                CurrentConfiguration: function() {
                    return null;
                },
                LookupResponse: function() {
                    return { attributes: null };
                }
            },
            views: {
                'wizard_content@home.ratingsengine.wizard': {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/explorer/explorer.component.html'
                }
                // 'wizard_content@home.ratingsengine.wizard': {
                //     controller: 'RatingsEngineAttributes',
                //     controllerAs: 'vm',
                //     templateUrl: 'app/ratingsengine/content/attributes/attributes.component.html'
                // }
            },
        });
});