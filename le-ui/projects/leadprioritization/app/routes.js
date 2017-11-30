angular
.module('mainApp')
.service('StateHistory', function() {
    this.states = {
        from: [],
        fromParams: [],
        to: [],
        toParams: []
    };

    this.lastTo = function() {
        return this.states.to[this.states.to.length - 1];
    }

    this.lastFrom = function() {
        return this.states.from[this.states.from.length - 1];
    }

    this.isTo = function(name) {
        return this.lastTo().name == name;
    }

    this.isFrom = function(name) {
        return this.lastFrom().name == name;
    }

    this.setTo = function(state, params) {
        this.states.to.push(state);
        this.states.toParams.push(params);
    }

    this.setFrom = function(state, params) {
        this.states.from.push(state);
        this.states.fromParams.push(params);
    }
})
.run(function($rootScope, $state, ServiceErrorUtility, LookupStore, StateHistory) {
    var self = this;

    $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams) {
        StateHistory.setTo(toState, toParams);
        StateHistory.setFrom(fromState, fromParams);

        // when user hits browser Back button after app instantiate, send back to login
        if (fromState.name == 'home.models' && toState.name == 'home') {
            event.preventDefault();
            window.open("/login", "_self");
        }

        var views = toState.views || {},
            toParams = toState.toParams,
            LoadingText = toParams ? (toParams.LoadingText || '') : '',
            split, view, hasMain = false;

        for (view in views) {
            split = view.split('@');

            if (split[0] == 'main') {
                hasMain = true;
            }
        }

        if (hasMain && toParams && toParams.LoadingSpinner !== false) {
            ShowSpinner(LoadingText);
        }

        if (toState.redirectTo) {
            event.preventDefault();
            $state.go(toState.redirectTo, toParams);
        }

        ServiceErrorUtility.hideBanner();
    });

    $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) {
        var from = fromState.name;
        var to = toState.name;

        // clear LookupStore data when leaving Data-Cloud section
        if ((from.indexOf('home.lookup') > -1 || from.indexOf('home.data-cloud') > -1) &&
            !(to.indexOf('home.lookup') > -1 || to.indexOf('home.data-cloud') > -1)) {

            LookupStore.reset();
        }
    });

    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error) {
        console.error('$stateChangeError', event, toState, toParams, fromState, fromParams, error);
        if ($state.current.name != toState.name) {
            $state.reload();
        }
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/tenant/');

    $stateProvider
        .state('home', {
            url: '/tenant/:tenantName',
            resolve: {
                ClientSession: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getClientSession();
                },
                Tenant: function(ClientSession) {
                    return ClientSession.Tenant;
                },
                WidgetConfig: function($q, ConfigService) {
                    var deferred = $q.defer();

                    ConfigService.GetWidgetConfigDocument().then(function(result) {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                FeatureFlags: function($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function() {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                ResourceStrings: function($q, ResourceStringsService, ClientSession) {
                    var deferred = $q.defer();

                    ResourceStringsService.GetInternalResourceStringsForLocale(ClientSession.Locale).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ApiHost: function() {
                    return '/pls'; // don't remove this. -Lazarus
                },    
                EnrichmentCount: function($q, $state, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.setHost(ApiHost);

                    DataCloudStore.getCount().then(function(result) {
                        DataCloudStore.setMetadata('enrichmentsTotal', result.data);
                        deferred.resolve(result.data);
                    });
                    
                    return deferred.promise;
                }
            },
            views: {
                "header": {
                    controller: 'HeaderController',
                    templateUrl: 'app/navigation/header/views/MainHeaderView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "navigation": {
                    controller: function($scope, $rootScope, $stateParams, $state, Tenant, FeatureFlagService) {
                        var tenantName = $stateParams.tenantName;

                        if (tenantName != Tenant.DisplayName) {
                            $rootScope.tenantName = window.escape(Tenant.DisplayName);
                            $rootScope.tenantId = window.escape(Tenant.Identifier);

                            FeatureFlagService.GetAllFlags().then(function(result) {
                                var flags = FeatureFlagService.Flags();

                                if(FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL)){
                                    $state.go('home.segment.explorer.attributes', {
                                        tenantName: Tenant.DisplayName,
                                        segment: 'Create'
                                    });
                                } else {
                                    $state.go('home.models', {
                                        tenantName: Tenant.DisplayName
                                    });
                                }
                            });
                        }
                    },
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                }
            }
        })
        .state('home.models', {
            url: '/models',
            params: {
                pageTitle: 'Models',
                pageIcon: 'ico-model'
            },
            resolve: {
                ModelList: function($q, ModelStore) {
                    var deferred = $q.defer();

                    ModelStore.getModels(true).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                "main@": {
                    controller: 'ModelListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/ModelListView.html'
                }
            }
        })
        .state('home.models.history', {
            url: '/history',
            params: {
                pageIcon: 'ico-model',
                pageTitle: 'Models > Creation History'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                "main@": {
                    templateUrl: 'app/models/views/ModelCreationHistoryView.html'
                }
            }
        })
        .state('home.model', {
            url: '/model/:modelId',
            resolve: {
                Model: function($q, $stateParams, ModelStore) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelStore.getModel(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                IsPmml: function(Model) {
                    return Model.ModelDetails.ModelType == 'PmmlModel';
                },
                loadAlaSQL: function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/alasql.min.js');
                },
                HasRatingsAvailable: function($q, $stateParams, ModelRatingsService){

                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.HistoricalABCDBuckets(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    controller: function($scope, $rootScope, Model, IsPmml, FeatureFlagService, HasRatingsAvailable) {
                        $scope.IsPmml = IsPmml;
                        $scope.sourceType = Model.ModelDetails.SourceSchemaInterpretation;
                        $scope.Uploaded = Model.ModelDetails.Uploaded;
                        $scope.HasRatingsAvailable = HasRatingsAvailable;

                        if(JSON.stringify(HasRatingsAvailable) != "{}"){
                            $scope.HasRatingsAvailable = true;
                        } else {
                            $scope.HasRatingsAvailable = false;
                        }

                        FeatureFlagService.GetAllFlags().then(function() {
                            var flags = FeatureFlagService.Flags();
                            $scope.canRemodel = FeatureFlagService.FlagIsEnabled(flags.VIEW_REMODEL) && !$scope.IsPmml && !$scope.Uploaded;
                            $scope.showModelSummary = FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE) || FeatureFlagService.UserIs('EXTERNAL_ADMIN');
                            $scope.showAlerts = 0; // disable for all (PLS-1670) FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
                            $scope.showRefineAndClone = FeatureFlagService.FlagIsEnabled(flags.VIEW_REFINE_CLONE);
                            $scope.showReviewModel = FeatureFlagService.FlagIsEnabled(flags.REVIEW_MODEL);
                            $scope.showSampleLeads = FeatureFlagService.FlagIsEnabled(flags.VIEW_SAMPLE_LEADS);
                        });

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    templateUrl: 'app/navigation/sidebar/ModelView.html'
                },
                "summary@": {
                    controller: 'ModelDetailController',
                    template: '<div id="ModelDetailsArea"></div>'
                },
                "main@": {
                    template: ''
                }
            }
        })
        .state('home.model.segmentation', {
            url: '/segmentation',
            params: {
                pageTitle: 'Segments',
                pageIcon: 'ico-segments'
            },
            views: {
                "summary@": {
                    controller: function($rootScope, Model) {
                        $rootScope.$broadcast('model-details', {
                            displayName: Model.ModelDetails.DisplayName
                        });
                    },
                    template: ''
                },
                "main@": {
                    resolve: {
                        SegmentsList: function($q, SegmentService, SegmentStore) {
                            var deferred = $q.defer();

                            SegmentService.GetSegments().then(function(result) {
                                SegmentStore.setSegments(result);
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'SegmentationListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/SegmentationListView.html'
                }
            }
        })
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
                    resolve: {
                        SegmentsList: function($q, SegmentService, SegmentStore) {
                            var deferred = $q.defer();

                            SegmentService.GetSegments().then(function(result) {
                                SegmentStore.setSegments(result);
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'SegmentationListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/SegmentationListView.html'
                }
            }
        })
        .state('home.segment.import', {
            url: '/mydata',
            params: {
                pageIcon: 'ico-attributes',
                pageTitle: 'My Data'
            },
            views: {
                "main@": {
                    templateUrl: 'app/create/mydata/UploadMyDataView.html'
                }
            }
        })
        .state('home.model.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-attributes',
                pageTitle: 'Attributes'
            },
            views: {
                "main@": {
                    controller: function($scope, $compile, $rootScope, Model, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsAttributesTab" class="tab-content" data-top-predictor-widget></div>'))($scope);

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('home.model.performance', {
            url: '/performance',
            params: {
                pageIcon: 'ico-performance',
                pageTitle: 'Performance'
            },
            views: {
                "main@": {
                    controller: function($scope, $compile, $rootScope, Model, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="performanceTab" class="tab-content" data-performance-tab-widget></div>'))($scope);

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('home.model.ratings', {
            url: '/ratings',
            resolve: {
                CurrentConfiguration: function($q, $stateParams, ModelRatingsService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.MostRecentConfiguration(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                RatingsSummary: function($q, $stateParams, ModelRatingsService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.GetBucketedScoresSummary(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                HistoricalABCDBuckets: function($q, $stateParams, ModelRatingsService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.HistoricalABCDBuckets(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            params: {
                pageIcon: 'ico-ratings',
                pageTitle: 'Ratings'
            },
            views: {
                "main@": {
                    controller: 'ModelRatingsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/ModelRatingsView.html'
                }
            }
        })
        .state('home.model.ratings.history', {
            url: '/history',
            resolve: {
                HistoricalABCDBuckets: function($q, $stateParams, ModelRatingsService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.HistoricalABCDBuckets(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            params: {
                pageIcon: 'ico-ratings',
                pageTitle: 'History'
            },
            views: {
                "summary@": {
                    controller: '',
                    template: ''
                },
                "main@": {
                    controller: 'ModelRatingsHistoryController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/ModelRatingsHistoryView.html'
                }
            }
        })
        .state('home.model.ratings-demo', {
            url: '/ratings-demo',
            params: {
                pageIcon: 'ico-ratings',
                pageTitle: 'Ratings'
            },
            views: {
                "summary@": {
                    controller: '',
                    template: ''
                },
                "main@": {
                    templateUrl: 'app/models/views/ModelRatingsDemoView.html'
                }
            }
        })
        .state('home.model.leads', {
            url: '/leads',
            params: {
                pageIcon: 'ico-leads',
                pageTitle: 'Leads'
            },
            views: {
                "main@": {
                    controller: function($scope, $compile, $rootScope, Model, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsLeadsTab" class="tab-content" data-leads-tab-widget></div>'))($scope);

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    template: '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('home.model.summary', {
            url: '/summary',
            params: {
                pageIcon: 'ico-datatable',
                pageTitle: 'Summary'
            },
            views: {
                "main@": {
                    controller: function($scope, $compile, $rootScope, Model, ModelStore, IsPmml) {
                        $scope.data = ModelStore.data;
                        $scope.IsPmml = IsPmml;

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    templateUrl: 'app/AppCommon/widgets/adminInfoSummaryWidget/AdminInfoSummaryWidgetTemplate.html'
                }
            }
        })
        .state('home.model.alerts', {
            url: '/alerts',
            params: {
                pageIcon: 'ico-alerts',
                pageTitle: ''
            },
            views: {
                "main@": {
                    resolve: {
                        ModelAlertsTmp: function($q, Model, ModelService) {
                            var deferred = $q.defer(),
                                data = Model,
                                id = data.ModelDetails.ModelID,
                                result = {};

                            var suppressedCategories = data.SuppressedCategories;

                            ModelService.GetModelAlertsByModelId(id).then(function(result) {
                                if (result !== null && result.success === true) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = suppressedCategories;
                                    deferred.resolve(result);
                                } else if (result !== null && result.success === false) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = null;
                                    deferred.reject('nope');
                                }
                            });

                            return deferred.promise;
                        }
                    },
                    controller: function($scope, $rootScope, Model, ModelStore) {
                        $scope.data = ModelStore.data;

                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });

                    },
                    templateUrl: 'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
                }
            }
        })
        .state('home.model.refine', {
            url: '/refine',
            params: {
                pageIcon: 'ico-refine',
                pageTitle: 'Refine'
            },
            resolve: {
                loadKendo: ['$ocLazyLoad', function($ocLazyLoad) {
                    return $ocLazyLoad.load('lib/js/kendo.all.min.js');
                }]
            },
            views: {
                "main@": {
                    controller: 'ManageFieldsController',
                    templateUrl: 'app/setup/views/ManageFieldsView.html'
                }
            }
        })
        .state('home.model.review', {
            url: '/review',
            params: {
                pageIcon: 'ico-datatable',
                pageTitle: ''
            },
            resolve: {
                ReviewData: function($q, $stateParams, $http, Model, ModelReviewStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId,
                        eventTableName = Model.EventTableProvenance.EventTableName;

                    ModelReviewStore.GetReviewData(modelId, eventTableName).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            }
            /*
            views: {
                "summary@": {
                    controller: 'RefineModelSummaryController',
                    controllerAs: 'vm',
                    templateUrl: 'app/navigation/summary/RefineModelSummaryView.html'
                },
                "main@": {
                    controller: 'ModelReviewRowController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/RefineModelRowsView.html'
                }
            }
            */
        })
        .state('home.model.review.columns', {
            url: '/columns',
            params: {
                pageIcon: 'ico-datatable',
                pageTitle: ''
            },
            views: {
                "summary@": {
                    controller: 'RefineModelSummaryController',
                    controllerAs: 'vm',
                    templateUrl: 'app/navigation/summary/RefineModelSummaryView.html'
                },
                "main@": {
                    controller: 'ModelReviewColumnController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/RefineModelColumnsView.html'
                }
            }
        })
        .state('home.model.remodel', {
            url: '/remodel',
            params: {
                pageIcon: 'ico-remodel',
                pageTitle: 'Remodel'
            },
            resolve: {
                DataRules: function($q, $stateParams, $http, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    RemodelStore.GetModelReviewDataRules(modelId).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                Attributes: function ($q, $stateParams, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    RemodelStore.GetModelReviewAttributes(modelId).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    controller: function ($rootScope, Model) {
                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
                    },
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: 'RemodelController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/RemodelView.html'
                }
            }
        })
        .state('home.model.notes', {
            url: '/notes',
            resolve: {
                Notes: function($q, $stateParams, NotesService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    NotesService.GetNotes(id).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            params: {
                pageIcon: 'ico-notes',
                pageTitle: 'Notes'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: 'NotesController',
                    controllerAs: 'vm',
                    templateUrl: 'app/notes/NotesView.html'
                }
            }
        })
        .state('home.campaigns', {
            url: '/campaigns',
            params: {
                pageTitle: 'Campaigns',
                pageIcon: 'ico-campaign'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "main@": {
                    resolve: {
                        Campaigns: function($q, CampaignService) {
                            var deferred = $q.defer();

                            CampaignService.GetCampaigns().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'CampaignListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/campaigns/views/CampaignListView.html'
                }
            }
        })
        .state('home.campaigns.models', {
            url: '/models/{campaignId}',
            params: {
                pageTitle: 'Campaigns',
                pageIcon: 'ico-model'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "main@": {
                    controller: 'CampaignModelsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/campaigns/views/CampaignModelsView.html'
                }
            }
        })
        .state('home.marketosettings', {
            url: '/marketosettings',
            redirectto: 'home.marketosettings.apikey',
            resolve: {
                urls: function($q, $http) {
                    var deferred = $q.defer();

                    $http({
                        'method': "GET",
                        'url': "/pls/sureshot/urls",
                        'params': {
                            'crmType': "marketo"
                        }
                    }).then(
                        function onSuccess(response) {
                            if (response.data.Success) {
                                deferred.resolve(response.data.Result);
                            } else {
                                deferred.reject(response.data.Errors);
                            }
                        }, function onError(response) {
                            deferred.reject(response.data.Errors);
                        }
                    );

                    return deferred.promise;
                }
            }
        })
        .state('home.marketosettings.apikey', {
            url: '/apikey',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            resolve: {
                FeatureFlags: function($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function() {
                        deferred.resolve();
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    controller: function($scope, $state, FeatureFlagService, ApiHost, DataCloudStore) {
                        DataCloudStore.setHost(ApiHost);

                        FeatureFlagService.GetAllFlags().then(function() {

                            var flags = FeatureFlagService.Flags();
                            $scope.latticeIsEnabled = FeatureFlagService.FlagIsEnabled(flags.LATTICE_MARKETO_PAGE);

                            if ($scope.latticeIsEnabled !== true) {
                                $state.go('home.marketosettings.credentials');
                            }

                        });
                    },
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    resolve: {
                        MarketoCredentials: function($q, MarketoService) {
                            var deferred = $q.defer();

                            MarketoService.GetMarketoCredentials().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'MarketoCredentialsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/marketo/views/MarketoCredentialsView.html'
                }
            }
        })
        .state('home.marketosettings.create', {
            url: '/create',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles > Create New Marketo Profile'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    controller: function($scope, $state, ResourceUtility) {
                        $scope.isCreateForm = true;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    controller: 'MarketoCredentialSetupController',
                    controllerAs: 'vm',
                    templateUrl: 'app/marketo/views/AddCredentialFormView.html'
                }
            }
        })
        .state('home.marketosettings.edit', {
            url: '/edit/{id}',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles > Edit Profile',
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    controller: function($scope, $stateParams, $state, ResourceUtility) {
                        $scope.state = 'home.marketosettings.edit';
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    resolve: {
                        MarketoCredential: function($q, $stateParams, MarketoService) {
                            var deferred = $q.defer();
                            var id = $stateParams.id;

                            MarketoService.GetMarketoCredentials(id).then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'MarketoCredentialsEditController',
                    controllerAs: 'vm',
                    templateUrl: 'app/marketo/views/AddCredentialFormView.html'
                }
            }
        })
        .state('home.marketosettings.enrichment', {
            url: '/enrichment/{id}',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles > Enrichment',
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    controller: function($scope, $stateParams, $state, ResourceUtility) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    resolve: {
                        EnrichmentData: function($q, DataCloudStore) {
                            var deferred = $q.defer();

                            DataCloudStore.getEnrichments({onlySelectedAttributes: true}).then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        MarketoCredential: function($q, $stateParams, MarketoService) {
                            var deferred = $q.defer();
                            var id = $stateParams.id;

                            MarketoService.GetMarketoCredentials(id).then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        MarketoMatchFields: function($q, MarketoService, MarketoCredential) {
                            var deferred = $q.defer();

                            MarketoService.GetMarketoMatchFields(MarketoCredential).then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'MarketoEnrichmentController',
                    controllerAs: 'vm',
                    templateUrl: 'app/marketo/views/MarketoEnrichmentView.html'
                }
            }
        })
        .state('home.marketosettings.models', {
            url: '/models/{id}',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_MODELS';
                        }
                    },
                    controller: function($scope, $stateParams, $state, ResourceUtility) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    controller: function(urls, $scope, $stateParams) {
                        $scope.id = $stateParams.id;
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.scoring_settings_url + '&credentialId=' + $scope.id + '"></iframe>');

                        changeIframeHeight();

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }

                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.marketosettings.credentials', {
            url: '/credentials',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    controller: function($scope, $state) {
                        $scope.state = 'home.marketosettings.edit';
                    },
                    templateUrl: 'app/navigation/summary/SureShotTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.creds_url + '"></iframe>');

                        changeIframeHeight();

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.marketosettings.activemodels', {
            url: '/activemodels',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_MODELS';
                        }
                    },
                    controller: function($scope, $stateParams, $state) {
                        $scope.state = $state.current.name;
                    },
                    templateUrl: 'app/navigation/summary/SureShotTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.scoring_settings_url + '"></iframe>');

                        changeIframeHeight();

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.eloquasettings', {
            url: '/eloquasettings',
            params: {
                pageIcon: 'ico-eloqua',
                pageTitle: 'Eloqua Settings'
            },
            redirectto: 'eloquasettings.apikey',
            resolve: {
                urls: function($q, $http) {
                    var deferred = $q.defer();

                    $http({
                        'method': "GET",
                        'url': "/pls/sureshot/urls",
                        'params': {
                            'crmType': "eloqua"
                        }
                    }).then(
                        function onSuccess(response) {
                            if (response.data.Success) {
                                deferred.resolve(response.data.Result);
                            } else {
                                deferred.reject(response.data.Errors);
                            }
                        }, function onError(response) {
                            deferred.reject(response.data.Errors);
                        }
                    );

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    //templateUrl: 'app/navigation/sidebar/EloquaSettingsView.html'
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    template: ''
                },
                "main@": {
                    template: ''
                }
            }
        })
        .state('home.eloquasettings.apikey', {
            url: '/apikey',
            params: {
                pageIcon: 'ico-eloqua',
                pageTitle: 'Eloqua Settings'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_ELOQUA_APIKEY';
                        }
                    },
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        if(urls && urls.creds_url) {
                            $('#sureshot_iframe_container')
                                .html('<iframe src="' + urls.creds_url + '"></iframe>');

                            changeIframeHeight();
                        }

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.eloquasettings.models', {
            url: '/models',
            params: {
                pageIcon: 'ico-eloqua',
                pageTitle: 'Eloqua Settings'
            },
            views: {
                "summary@": {
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_ELOQUA_MODELS';
                        }
                    },
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        if(urls && urls.scoring_settings_url) {
                            $('#sureshot_iframe_container')
                                .html('<iframe src="' + urls.scoring_settings_url + '"></iframe>');

                            changeIframeHeight();
                        }

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.eloquasettings.enrichment', {
            url: '/enrichment',
            params: {
                pageIcon: 'ico-eloqua',
                pageTitle: 'Eloqua Settings'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        if(urls && urls.enrichment_settings_url) {
                            $('#sureshot_iframe_container')
                                .html('<iframe src="' + urls.enrichment_settings_url + '"></iframe>');
                            changeIframeHeight();
                        }

                        function changeIframeHeight(){
                            var if_height;

                            window.addEventListener("message", function (event){
                                // verify the origin is sureshot, if not just return
                                var origin = event.origin || event.originalEvent.origin;
                                //if (origin != "{sureshot_iframe_origin}")
                                //return false;

                                if (!event.data.contentHeight) {
                                    return;
                                }

                                var h = event.data.contentHeight;

                                if ( !isNaN( h ) && h > 0 && h !== if_height ) {
                                    if_height = h;

                                    $("#sureshot_iframe_container iframe").height(h);
                                }
                                return true;
                            }, false);
                        }

                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.sfdcsettings', {
            url: '/sfdcsettings',
            params: {
                pageIcon: 'ico-salesforce',
                pageTitle: 'Salesforce Settings'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: 'sfdcCredentialsController',
                    templateUrl: 'app/sfdc/views/SFDCCredentialsView.html'
                }
            }
        })
        .state('home.apiconsole', {
            url: '/apiconsole',
            params: {
                pageIcon: 'ico-api-console',
                pageTitle: 'API Console'
            },
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    templateUrl: 'app/apiConsole/views/APIConsoleView.html'
                }
            }
        })
        .state('home.signout', {
            url: '/signout',
            views: {
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    controller: function(LoginService) {
                        ShowSpinner('Logging Out...');
                        LoginService.Logout();
                    }
                }
            }
        })
        // .state('home.updatepassword', {
        //     url: '/updatepassword',
        //     params: {
        //         pageIcon: 'ico-user',
        //         pageTitle: 'User Settings'
        //     },
        //     views: {
        //         "navigation@": {
        //             templateUrl: 'app/navigation/sidebar/RootView.html'
        //         },
        //         "summary@": {
        //             templateUrl: 'app/navigation/summary/BlankLine.html'
        //         },
        //         "main@": {
        //             templateUrl: 'app/login/views/UpdatePasswordView.html'
        //         }
        //     }
        // })
        // .state('passwordsuccess', {
        //     url: '/passwordsuccess',
        //     views: {
        //         "summary@": {
        //             templateUrl: 'app/navigation/summary/BlankLine.html'
        //         },
        //         "main@": {
        //             templateUrl: 'app/login/views/UpdatePasswordSuccessView.html'
        //         }
        //     }
        // })
        // .state('home.deploymentwizard', {
        //     url: '/deploymentwizard',
        //     views: {
        //         "navigation@": {
        //             templateUrl: 'app/navigation/sidebar/RootView.html'
        //         },
        //         "summary@": {
        //             templateUrl: 'app/navigation/summary/BlankLine.html'
        //         },
        //         "main@": {
        //             controller: 'DeploymentWizardController',
        //             templateUrl: 'app/setup/views/DeploymentWizardView.html'
        //         }
        //     }
        // })
        // .state('home.activate', {
        //     url: '/activate',
        //     views: {
        //         "navigation@": {
        //             templateUrl: 'app/navigation/sidebar/RootView.html'
        //         },
        //         "summary@": {
        //             resolve: {
        //                 ResourceString: function() {
        //                     return 'ACTIVATE_MODEL_TITLE';
        //                 }
        //             },
        //             controller: 'OneLineController',
        //             templateUrl: 'app/navigation/summary/OneLineView.html'
        //         },
        //         "main@": {
        //             templateUrl: 'app/models/views/ActivateModelView.html'
        //         }
        //     }
        // })
        .state('home.users', {
            url: '/users',
            params: {
                pageIcon: 'ico-user',
                pageTitle: 'Manage Users'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                "main@": {
                    resolve: {
                        UserList: function($q, UserManagementService) {
                            var deferred = $q.defer();

                            UserManagementService.GetUsers().then(function(result) {
                                if (result.Success) {
                                    deferred.resolve(result.ResultObj);
                                } else {
                                    deferred.reject(result);
                                }

                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'UserManagementWidgetController',
                    templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
                }
            }
        })
        // .state('home.setup', {
        //     url: '/setup',
        //     views: {
        //         "navigation@": {
        //             templateUrl: 'app/navigation/sidebar/RootView.html'
        //         },
        //         "summary@": {
        //             resolve: {
        //                 ResourceString: function() {
        //                     return 'SYSTEM_SETUP_TITLE';
        //                 }
        //             },
        //             controller: 'OneLineController',
        //             templateUrl: 'app/navigation/summary/OneLineView.html'
        //         },
        //         "main@": {
        //             templateUrl: 'app/config/views/ManageCredentialsView.html'
        //         }
        //     }
        // })
        // .state('home.fields', {
        //     url: '/fields',
        //     views: {
        //         "navigation@": {
        //             templateUrl: 'app/navigation/sidebar/RootView.html'
        //         },
        //         "summary@": {
        //             resolve: {
        //                 ResourceString: function() {
        //                     return 'SETUP_NAV_NODE_MANAGE_FIELDS';
        //                 }
        //             },
        //             controller: 'OneLineController',
        //             templateUrl: 'app/navigation/summary/OneLineView.html'
        //         },
        //         "main@": {
        //             controller: 'SetupController',
        //             templateUrl: 'app/setup/views/SetupView.html'
        //         }
        //     }
        // })
        .state('home.insights', {
            url: '/insights',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'BIS Insights iFrame Testing',
                iframe: true
            },
            views: {
                "summary@": {
                    template: '<br><br>'
                },
                "main@": {
                    controller: 'LookupController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/lookup/lookup.component.html'
                }
            }
        })
        .state('home.insights.iframe', {
            url: '/iframe',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'BIS Insights Test'
            },
            views: {
                "summary@": {
                    template: '<br><div class="lookup-summary ten columns offset-one"><div class="lookup-back" ui-sref="home.insights"><ico class="fa fa-arrow-left"></ico>NEW LOOKUP</div></div></div>'
                },
                "main@": {
                    controller: function($scope, LookupStore, $stateParams) {
                        var host = "/insights/";

                        $('#sureshot_iframe_container')
                            .html('<iframe id="insights_iframe" src="' + host + '" style="border: 1px inset"></iframe>');

                        var childWindow = document.getElementById('insights_iframe').contentWindow;

                        window.addEventListener('message', handleMessage, false);

                        function handleMessage(event){
                            console.log('receiving from Insights:', event.data);
                            if (event.data == 'init') {
                                var json = {};

                                json.Authentication = LookupStore.get('Authentication');
                                json.request = LookupStore.get('request');
                                //json.request.record = $stateParams.record;

                                console.log('posting to Insights:', json);
                                childWindow.postMessage(json,'*');
                            }
                        }

                        $scope.$on('$destroy', function() {
                            window.removeEventListener('message', handleMessage);
                        });
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        });
});
function ShowSpinner(LoadingString, type) {
    // state change spinner
    var element = $('#mainContentView'),
        LoadingString = LoadingString || '',
        type = type || 'lattice';

    // jump to top of page during state change
    angular.element(window).scrollTop(0,0);

    element
        .children()
            .addClass('inactive-disabled');

    element
        .css({
            position:'relative'
        })
        .prepend(
            $(
                '<section class="loading-spinner ' + type + '">' +
                '<h2 class="text-center">' + LoadingString + '</h2>' +
                '<div class="meter"><span class="indeterminate"></span></div>' +
                '</section>'
            )
        );

    setTimeout(function() {
        $('section.loading-spinner').addClass('show-spinner');
    }, 1);
}
