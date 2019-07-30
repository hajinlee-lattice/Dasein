import NgState from "atlas/ng-state";
export default function ($stateProvider, $urlRouterProvider, $locationProvider) {
    'ngInject';

    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/tenant/');

    $stateProvider
        .state('home', {
            url: '/tenant/:tenantName',
            onEnter: function ($state, SidebarStore) {
                SidebarStore.set(null);
                NgState.setAngularState($state);
            },
            params: {
                tenantName: { dynamic: true, value: '' }
            },
            resolve: {
                ClientSession: function (BrowserStorageUtility) {
                    return BrowserStorageUtility.getClientSession();
                },
                WidgetConfig: function ($q, ConfigService) {
                    var deferred = $q.defer();

                    ConfigService.GetWidgetConfigDocument().then(function (
                        result
                    ) {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                FeatureFlags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function () {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                ResourceStrings: function (
                    $q,
                    ResourceStringsService,
                    ClientSession
                ) {
                    var deferred = $q.defer();

                    ResourceStringsService.GetInternalResourceStringsForLocale(
                        ClientSession.Locale
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                // placeholder for when this is set to /ulysses in insights iframe
                ApiHost: function () {
                    return '/pls'; // don't remove this. -Lazarus
                },
                CollectionStatus: function (
                    $q,
                    FeatureFlags,
                    FeatureFlagService,
                    QueryStore
                ) {
                    var deferred = $q.defer(),
                        flags = FeatureFlagService.Flags();

                    if (FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL)) {
                        QueryStore.getCollectionStatus().then(function (result) {
                            deferred.resolve(result);
                        });

                        return deferred.promise;
                    }
                }
            },
            views: {
                sidebar: {
                    templateUrl: 'app/navigation/sidebar/sidebar.component.html'
                },
                'navigation@home': {
                    templateUrl:
                        'app/navigation/sidebar/root/root.component.html'
                },
                header: {
                    controller: 'HeaderController',
                    templateUrl:
                        'app/navigation/header/views/MainHeaderView.html'
                },
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                banner: 'bannerMessage',
                notice: 'noticeMessage'
            }
        })
        .state('home.models', {
            url: '/models',
            onEnter: function ($state, FilterService) {
                if (['home.models'].indexOf($state.current.name) < 0) {
                    FilterService.clear();
                }
            },
            params: {
                pageTitle: 'Models',
                pageIcon: 'ico-model'
            },
            resolve: {
                ModelList: function ($q, ModelStore) {
                    var deferred = $q.defer();

                    ModelStore.getModels(true).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: 'ModelListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                'main@': {
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
                'summary@': {
                    controller: 'ModelListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                'main@': {
                    templateUrl:
                        'app/models/views/ModelCreationHistoryView.html'
                }
            }
        })
        .state('home.model', {
            url: '/model/:modelId/:rating_id/:aiModel',
            params: {
                modelId: '',
                rating_id: '',
                aiModel: '',
                viewingIteration: false,
                useSelectedIteration: false
            },
            onEnter: [
                '$stateParams',
                'IsCdl',
                'Model',
                'RatingEngine',
                'BackStore',
                function ($stateParams, IsCdl, Model, RatingEngine, BackStore) {
                    if ($stateParams.viewingIteration) {
                        var backState = 'home.ratingsengine.dashboard',
                            backParams = {
                                rating_id: $stateParams.rating_id,
                                modelId: $stateParams.modelId,
                                viewingIteration: false
                            },
                            displayName = 'View Model';

                        BackStore.setBackLabel(displayName);
                        BackStore.setBackState(backState);
                        BackStore.setBackParams(backParams);
                    } else {
                        var displayName = Model.ModelDetails.DisplayName;
                        var backState = 'home.models';

                        if (IsCdl === true) {
                            backState = 'home.ratingsengine';
                            displayName = RatingEngine.displayName;
                        }
                        BackStore.setBackLabel(displayName);
                        BackStore.setBackState(backState);
                    }
                }
            ],
            resolve: {
                IsCdl: function (FeatureFlagService) {
                    var flags = FeatureFlagService.Flags();
                    var cdl = FeatureFlagService.FlagIsEnabled(
                        flags.ENABLE_CDL
                    );
                    return cdl;
                },
                RatingEngine: function (
                    $q,
                    $stateParams,
                    RatingsEngineStore,
                    IsCdl
                ) {
                    if (IsCdl) {
                        var deferred = $q.defer(),
                            id = $stateParams.rating_id;

                        RatingsEngineStore.getRating(id).then(function (engine) {
                            deferred.resolve(engine);
                        });

                        return deferred.promise;
                    } else {
                        return null;
                    }
                },
                Model: function ($q, $stateParams, ModelStore) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelStore.getModel(id).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                IsRatingEngine: function (Model) {
                    return Model.ModelDetails.Name.substring(0, 2) == 'ai';
                },
                IsPmml: function (Model) {
                    return Model.ModelDetails.ModelType == 'PmmlModel';
                },
                HasRatingsAvailable: function (
                    $q,
                    $stateParams,
                    ModelRatingsService
                ) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.HistoricalABCDBuckets(id).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'navigation@home': {
                    controller: 'SidebarModelController',
                    controllerAs: 'vm',
                    templateUrl:
                        'app/navigation/sidebar/model/model.component.html'
                },
                'summary@': {
                    controller: 'ModelDetailController',
                    template: '<div id="ModelDetailsArea"></div>'
                },
                'main@': {
                    template: ''
                },
                'header.back@': 'backNav'
            }
        })
        .state('home.segment.import', {
            url: '/mydata',
            params: {
                pageIcon: 'ico-attributes',
                pageTitle: 'My Data'
            },
            views: {
                'main@': {
                    templateUrl: 'app/create/mydata/UploadMyDataView.html'
                }
            }
        })
        .state('home.model.datacloud', {
            url: '/datacloud/:aiModel',
            onExit: function(DataCloudStore) {
                DataCloudStore.clear();
            },
            params: {
                section: 're.model_iteration',
                pageIcon: 'ico-view-model',
                pageTitle: 'Attribute List',
                gotoNonemptyCategory: true,
                viewingIteration: true
            },
            resolve: {
                ReviewData: function (
                    $q,
                    $stateParams,
                    Model,
                    ModelReviewStore,
                    DataCloudStore
                ) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    ModelReviewStore.GetReviewData(
                        modelId
                    ).then(function (result) {
                        var warnings = {};
                        result.forEach(item => {
                            warnings[item.name] = item;
                        });
                        DataCloudStore.setWarnings(warnings);
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                Enrichments: function ($q, $stateParams, DataCloudStore, ApiHost) {
                    var deferred = $q.defer(),
                        ratingId = $stateParams['rating_id'],
                        aiModel = $stateParams['aiModel'],
                        opts = {
                            url: `/pls/ratingengines/${ratingId}/ratingmodels/${aiModel}/metadata`
                        };

                    // clear DataCloudStore (tried this in onEnter, didn't work right)                   
                    DataCloudStore.clear();
                    DataCloudStore.setRatingIterationFilter('all');

                    DataCloudStore.getAllEnrichmentsConcurrently(opts, true).then((result) => {

                        // console.log(result.find(x => x.ImportanceOrdering));
                        // console.log(result.find(x => x.PredictivePower));

                        result.forEach((item) => {
                            item.Entity = "Account";
                            // if (!item.PredictivePower) {
                            //     item.PredictivePower = 0;
                            // }
                            if (item.IsCoveredByMandatoryRule || item.IsCoveredByOptionalRule) {
                                item.HasWarnings = true;
                            }
                        })
                        
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                EnrichmentTopAttributes: function ($q, $stateParams, DataCloudStore, ApiHost) {
                    var deferred = $q.defer(),
                        ratingId = $stateParams['rating_id'],
                        aiModel = $stateParams['aiModel'],
                        opts = {
                            url: `/pls/ratingengines/${ratingId}/ratingmodels/${aiModel}/metadata/topn`
                        };

                    DataCloudStore.getAllTopAttributes(opts, true).then((result) => {
                        deferred.resolve(
                            result['Categories'] || result || {}
                        );
                    });

                    return deferred.promise;
                },
                EnrichmentPremiumSelectMaximum: function ($q, DataCloudStore, ApiHost) {
                    var deferred = $q.defer();

                    DataCloudStore.getPremiumSelectMaximum().then((result) => {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                EnrichmentSelectMaximum: function ($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getSelectMaximum().then((result) => {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                LookupResponse: () => { return { attributes: null } },
                QueryRestriction: () => { return null },
                WorkingBuckets: () => { return null },
                RatingsEngineModels: () => { return null }
            },
            views: {
                'main@': {
                    controller: 'DataCloudController',
                    controllerAs: 'vm',
                    templateUrl:
                        '/components/datacloud/explorer/explorer.component.html'
                },
                'subsummary@': {
                    controller: 'SubHeaderTabsController',
                    controllerAs: 'vm',
                    templateUrl:
                        '/components/datacloud/tabs/subheader/subheader.component.html'
                }
            }
        })
        .state('home.model.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-donut',
                pageTitle: 'Attribute Analysis'
            },
            views: {
                'main@': {
                    controller: function (
                        $scope,
                        $compile,
                        ModelStore
                    ) {
                        $scope.data = ModelStore.data;
                        $compile(
                            $('#modelDetailContainer').html(
                                '<div id="modelDetailsAttributesTab" class="tab-content" data-top-predictor-widget></div>'
                            )
                        )($scope);
                    },
                    template:
                        '<div id="modelDetailContainer" class="model-details"></div>'
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
                'main@': {
                    controller: function ($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile(
                            $('#modelDetailContainer').html(
                                '<div id="performanceTab" class="tab-content" data-performance-tab-widget></div>'
                            )
                        )($scope);
                    },
                    template:
                        '<div id="modelDetailContainer" class="model-details"></div>'
                },
                'header.back@': 'backNav'
            }
        })
        .state('home.model.ratings', {
            url: '/ratings',
            resolve: {
                WorkingBuckets: function (
                    $q,
                    $stateParams,
                    ModelRatingsService
                ) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.MostRecentConfiguration(id).then(
                        function (result) {
                            deferred.resolve(result);
                        }
                    );

                    return deferred.promise;
                },
                RatingsSummary: function (
                    $q,
                    $stateParams,
                    ModelRatingsService
                ) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.GetBucketedScoresSummary(id).then(
                        function (result) {
                            deferred.resolve(result);
                        }
                    );

                    return deferred.promise;
                },
                HistoricalABCDBuckets: function (
                    $q,
                    $stateParams,
                    ModelRatingsService
                ) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    ModelRatingsService.HistoricalABCDBuckets(id).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            params: {
                pageIcon: 'ico-ratings',
                pageTitle: 'Ratings',
                section: null
            },
            views: {
                'main@': {
                    controller: 'ModelRatingsController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/ModelRatingsView.html'
                }
            }
        })
        .state('home.model.ratings.history', {
            url: '/history',
            resolve: {
                ScoringHistory: function (
                    $q,
                    $stateParams,
                    ModelRatingsService
                ) {
                    var deferred = $q.defer(),
                        id = $stateParams.rating_id;

                    ModelRatingsService.ScoringHistory(id).then(function (
                        result
                    ) {
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
                'main@': {
                    controller: 'ModelRatingsHistoryController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/ModelRatingsHistoryView.html'
                }
            }
        })
        .state('home.model.ratings-demo', {
            url: '/ratings-demo',
            resolve: {
                FeatureFlags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function (result) {
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
                'main@': {
                    controller: function (FeatureFlags) {
                        this.cdlIsEnabled = FeatureFlags.EnableCdl; //vm.cdlIsEnabled
                    },
                    controllerAs: 'vm',
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
                'main@': {
                    controller: function ($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile(
                            $('#modelDetailContainer').html(
                                '<div id="modelDetailsLeadsTab" class="tab-content" data-leads-tab-widget></div>'
                            )
                        )($scope);
                    },
                    template:
                        '<div id="modelDetailContainer" class="model-details"></div>'
                }
            }
        })
        .state('home.model.summary', {
            url: '/summary',
            params: {
                pageIcon: 'ico-datatable',
                pageTitle: 'Model Summary'
            },
            views: {
                'main@': {
                    controller: function ($scope, $compile, ModelStore, IsPmml) {
                        $scope.data = ModelStore.data;
                        $scope.IsPmml = IsPmml;
                    },
                    templateUrl:
                        'app/AppCommon/widgets/adminInfoSummaryWidget/AdminInfoSummaryWidgetTemplate.html'
                }
            }
        })
        .state('home.model.alerts', {
            url: '/alerts',
            params: {
                pageIcon: 'ico-alerts',
                pageTitle: ''
            },
            resolve: {
                ModelAlertsTmp: function ($q, Model, ModelService) {
                    var deferred = $q.defer(),
                        data = Model,
                        id = data.ModelDetails.ModelID,
                        result = {};

                    var suppressedCategories = data.SuppressedCategories;

                    ModelService.GetModelAlertsByModelId(id).then(function (
                        result
                    ) {
                        if (result !== null && result.success === true) {
                            data.ModelAlerts = result.resultObj;
                            data.SuppressedCategories = suppressedCategories;
                            deferred.resolve(result);
                        } else if (
                            result !== null &&
                            result.success === false
                        ) {
                            data.ModelAlerts = result.resultObj;
                            data.SuppressedCategories = null;
                            deferred.reject('nope');
                        }
                    });

                    return deferred.promise;
                }
            },
            views: {
                'main@': {
                    controller: function ($scope, Model, ModelStore) {
                        $scope.data = ModelStore.data;
                    },
                    templateUrl:
                        'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
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
                ReviewData: function (
                    $q,
                    $stateParams,
                    $http,
                    Model,
                    ModelReviewStore
                ) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId,
                        eventTableName =
                            Model.EventTableProvenance.EventTableName;

                    ModelReviewStore.GetReviewData(
                        modelId,
                        eventTableName
                    ).then(function (result) {
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
                'summary@': {
                    controller: 'RefineModelSummaryController',
                    controllerAs: 'vm',
                    templateUrl:
                        'app/navigation/summary/RefineModelSummaryView.html'
                },
                'main@': {
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
                DataRules: function ($q, $stateParams, $http, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    if (modelId != undefined) {
                        RemodelStore.GetModelReviewDataRules(modelId).then(function (
                            result
                        ) {
                            deferred.resolve(result);
                        });
                    } else {
                        deferred.reject();
                    }

                    return deferred.promise;
                },
                Attributes: function ($q, $stateParams, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    RemodelStore.GetModelReviewAttributes(modelId).then(
                        function (result) {
                            deferred.resolve(result);
                        }
                    );

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: 'RemodelController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/RemodelView.html'
                }
            }
        })
        .state('home.model.notes', {
            url: '/notes',
            resolve: {
                Notes: function ($q, $stateParams, NotesService) {
                    var deferred = $q.defer(),
                        id = $stateParams.modelId;

                    NotesService.GetNotes(id).then(function (result) {
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
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: 'NotesController',
                    controllerAs: 'vm',
                    templateUrl: 'app/notes/NotesView.html'
                }
            }
        })
        .state('home.marketosettings', {
            url: '/marketosettings',
            redirectto: 'home.marketosettings.apikey',
            resolve: {
                urls: function ($q, $http) {
                    var deferred = $q.defer();

                    $http({
                        method: 'GET',
                        url: '/pls/sureshot/urls',
                        params: {
                            crmType: 'marketo'
                        }
                    }).then(
                        function onSuccess(response) {
                            if (response.data.Success) {
                                deferred.resolve(response.data.Result);
                            } else {
                                deferred.reject(response.data.Errors);
                            }
                        },
                        function onError(response) {
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
                FeatureFlags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function () {
                        deferred.resolve();
                    });

                    return deferred.promise;
                },
                MarketoCredentials: function ($q, MarketoService) {
                    var deferred = $q.defer();

                    MarketoService.GetMarketoCredentials().then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'navigation@home': {
                    controller: function (
                        $scope,
                        $state,
                        FeatureFlagService,
                        ApiHost,
                        DataCloudStore
                    ) {
                        DataCloudStore.setHost(ApiHost);

                        FeatureFlagService.GetAllFlags().then(function () {
                            var flags = FeatureFlagService.Flags();
                            $scope.latticeIsEnabled = FeatureFlagService.FlagIsEnabled(
                                flags.LATTICE_MARKETO_PAGE
                            );

                            if ($scope.latticeIsEnabled !== true) {
                                $state.go('home.marketosettings.credentials');
                            }
                        });
                    },
                    templateUrl:
                        'app/navigation/sidebar/root/root.component.html'
                },
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
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
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_APIKEY';
                }
            },
            views: {
                'summary@': {
                    controller: function ($scope, $state, ResourceUtility) {
                        $scope.isCreateForm = true;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': {
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
                pageTitle: 'Marketo Profiles > Edit Profile'
            },
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_APIKEY';
                },
                FeatureFlags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                MarketoCredential: function ($q, $stateParams, MarketoService) {
                    var deferred = $q.defer();
                    var id = $stateParams.id;

                    MarketoService.GetMarketoCredentials(id).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility
                    ) {
                        $scope.state = 'home.marketosettings.edit';
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': {
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
                pageTitle: 'Marketo Profiles > Enrichment'
            },
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_APIKEY';
                },
                EnrichmentData: function ($q, DataCloudStore) {
                    var deferred = $q.defer();

                    DataCloudStore.getEnrichments().then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                MarketoCredential: function ($q, $stateParams, MarketoService) {
                    var deferred = $q.defer();
                    var id = $stateParams.id;

                    MarketoService.GetMarketoCredentials(id).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                MarketoMatchFields: function (
                    $q,
                    MarketoService,
                    MarketoCredential
                ) {
                    var deferred = $q.defer();

                    MarketoService.GetMarketoMatchFields(
                        MarketoCredential
                    ).then(function (result) {
                        deferred.resolve(result.data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility
                    ) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': {
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
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_MODELS';
                },
                FeatureFlags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            onEnter: [
                'FeatureFlags',
                '$state',
                '$stateParams',
                function (FeatureFlags, $state, $stateParams) {
                    var useMarketoLatticeIntegration =
                        FeatureFlags.LatticeMarketoScoring;
                    if (useMarketoLatticeIntegration) {
                        console.log('credential', $stateParams.id);
                        $state.go('home.marketosettings.scoring', {
                            credentialId: $stateParams.id
                        });
                    }
                }
            ],
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility,
                        FeatureFlags
                    ) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.id;
                        $scope.ResourceUtility = ResourceUtility;
                        $scope.useMarketoLatticeIntegration =
                            FeatureFlags.LatticeMarketoScoring;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': {
                    controller: function (urls, $scope, $stateParams) {
                        $scope.id = $stateParams.id;
                        $('#sureshot_iframe_container').html(
                            '<iframe src="' +
                            urls.scoring_settings_url +
                            '&credentialId=' +
                            $scope.id +
                            '"></iframe>'
                        );

                        changeIframeHeight();

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.marketosettings.scoring', {
            url: '/{credentialId}/scoring',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            resolve: {
                ActiveModels: function ($q, MarketoStore, MarketoService) {
                    var deferred = $q.defer();

                    var storedActiveModels = MarketoStore.getActiveModels();
                    if (!storedActiveModels || storedActiveModels.length == 0) {
                        MarketoService.GetActiveModels().then(function (result) {
                            deferred.resolve(result);
                        });
                    } else {
                        deferred.resolve(storedActiveModels);
                    }

                    return deferred.promise;
                },
                ScoringRequestSummaries: function (
                    $q,
                    $stateParams,
                    MarketoStore
                ) {
                    var deferred = $q.defer();

                    MarketoStore.getScoringRequestList(
                        $stateParams.credentialId,
                        false
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility
                    ) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.credentialId;
                        $scope.ResourceUtility = ResourceUtility;
                        $scope.useMarketoLatticeIntegration = true;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': 'marketoActiveModels'
            }
        })
        .state('home.marketosettings.setup', {
            url: '/{credentialId}/setup/{modelUuid}',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            resolve: {
                MarketoCredentials: function (
                    $q,
                    $stateParams,
                    MarketoStore,
                    MarketoService
                ) {
                    var deferred = $q.defer();

                    MarketoStore.getMarketoCredential(
                        $stateParams.credentialId
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ScoringRequestSummaries: function (
                    $q,
                    $stateParams,
                    MarketoStore
                ) {
                    var deferred = $q.defer();

                    MarketoStore.getScoringRequestList(
                        $stateParams.credentialId,
                        false
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                PrimaryAttributeFields: function (
                    $q,
                    MarketoStore,
                    MarketoService
                ) {
                    var deferred = $q.defer();

                    MarketoStore.getPrimaryAttributeFields().then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                MarketoFields: function (
                    $q,
                    MarketoService,
                    MarketoCredentials
                ) {
                    var deferred = $q.defer();

                    var params = {
                        soap_endpoint: MarketoCredentials.soap_endpoint,
                        soap_user_id: MarketoCredentials.soap_user_id,
                        soap_encryption_key:
                            MarketoCredentials.soap_encryption_key
                    };

                    MarketoService.GetMarketoMatchFields(params).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ScoringFields: function ($q, $stateParams, MarketoService) {
                    var deferred = $q.defer();

                    MarketoService.GetScoringFields(
                        $stateParams.modelUuid
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ExistingScoringRequest: function (
                    $q,
                    $stateParams,
                    MarketoStore,
                    ScoringRequestSummaries
                ) {
                    var deferred = $q.defer();

                    var existingScoringRequest = ScoringRequestSummaries
                        ? ScoringRequestSummaries.find(function (x) {
                            return x.modelUuid === $stateParams.modelUuid;
                        })
                        : null;

                    if (existingScoringRequest) {
                        MarketoStore.getScoringRequest(
                            false,
                            $stateParams.credentialId,
                            existingScoringRequest.configId
                        ).then(function (result) {
                            deferred.resolve(result);
                        });
                    } else {
                        deferred.resolve(null);
                    }

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility
                    ) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.credentialId;
                        $scope.ResourceUtility = ResourceUtility;
                        $scope.useMarketoLatticeIntegration = true;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': 'marketoSetupModel'
            }
        })
        .state('home.marketosettings.webhook', {
            url: '/{credentialId}/webhook/{configId}',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            resolve: {
                MarketoCredentials: function (
                    $q,
                    $stateParams,
                    MarketoStore,
                    MarketoService
                ) {
                    var deferred = $q.defer();

                    MarketoStore.getMarketoCredential(
                        $stateParams.credentialId
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ScoringRequest: function (
                    $q,
                    $stateParams,
                    MarketoStore,
                    StateHistory
                ) {
                    var deferred = $q.defer(),
                        useCache = false;

                    if (StateHistory.isFrom('home.marketosettings.setup')) {
                        useCache = true;
                    }

                    MarketoStore.getScoringRequest(
                        useCache,
                        $stateParams.credentialId,
                        $stateParams.configId
                    ).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                Model: function ($q, $stateParams, ScoringRequest, ModelStore) {
                    var deferred = $q.defer();

                    ModelStore.getModel(ScoringRequest.modelUuid).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    controller: function (
                        $scope,
                        $stateParams,
                        $state,
                        ResourceUtility
                    ) {
                        $scope.state = $state.current.name;
                        $scope.id = $stateParams.credentialId;
                        $scope.ResourceUtility = ResourceUtility;
                        $scope.useMarketoLatticeIntegration = true;
                    },
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                'main@': 'marketoWebhookSummary'
            }
        })
        .state('home.marketosettings.activemodels', {
            url: '/activemodels',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Profiles'
            },
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_MODELS';
                }
            },
            views: {
                'summary@': {
                    controller: function ($scope, $stateParams, $state) {
                        $scope.state = $state.current.name;
                    },
                    templateUrl: 'app/navigation/summary/SureShotTabs.html'
                },
                'main@': {
                    controller: function (urls) {
                        $('#sureshot_iframe_container').html(
                            '<iframe src="' +
                            urls.scoring_settings_url +
                            '"></iframe>'
                        );

                        changeIframeHeight();

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
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
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_MARKETO_APIKEY';
                }
            },
            views: {
                'summary@': {
                    controller: function ($scope, $state) {
                        $scope.state = 'home.marketosettings.edit';
                    },
                    templateUrl: 'app/navigation/summary/SureShotTabs.html'
                },
                'main@': {
                    controller: function (urls) {
                        $('#sureshot_iframe_container').html(
                            '<iframe src="' + urls.creds_url + '"></iframe>'
                        );

                        changeIframeHeight();

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
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
                urls: function ($q, $http) {
                    var deferred = $q.defer();

                    $http({
                        method: 'GET',
                        url: '/pls/sureshot/urls',
                        params: {
                            crmType: 'eloqua'
                        }
                    }).then(
                        function onSuccess(response) {
                            if (response.data.Success) {
                                deferred.resolve(response.data.Result);
                            } else {
                                deferred.reject(response.data.Errors);
                            }
                        },
                        function onError(response) {
                            deferred.reject(response.data.Errors);
                        }
                    );

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    template: ''
                },
                'main@': {
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
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_ELOQUA_APIKEY';
                }
            },
            views: {
                'summary@': {
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                'main@': {
                    controller: function (urls) {
                        if (urls && urls.creds_url) {
                            $('#sureshot_iframe_container').html(
                                '<iframe src="' + urls.creds_url + '"></iframe>'
                            );

                            changeIframeHeight();
                        }

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
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
            resolve: {
                ResourceString: function () {
                    return 'SUMMARY_ELOQUA_MODELS';
                }
            },
            views: {
                'summary@': {
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                'main@': {
                    controller: function (urls) {
                        if (urls && urls.scoring_settings_url) {
                            $('#sureshot_iframe_container').html(
                                '<iframe src="' +
                                urls.scoring_settings_url +
                                '"></iframe>'
                            );

                            changeIframeHeight();
                        }

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
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
                'summary@': {
                    templateUrl: 'app/navigation/summary/EloquaTabs.html'
                },
                'main@': {
                    controller: function (urls) {
                        if (urls && urls.enrichment_settings_url) {
                            $('#sureshot_iframe_container').html(
                                '<iframe src="' +
                                urls.enrichment_settings_url +
                                '"></iframe>'
                            );
                            changeIframeHeight();
                        }

                        function changeIframeHeight() {
                            var if_height;

                            window.addEventListener(
                                'message',
                                function (event) {
                                    // verify the origin is sureshot, if not just return
                                    var origin =
                                        event.origin ||
                                        event.originalEvent.origin;
                                    //if (origin != "{sureshot_iframe_origin}")
                                    //return false;

                                    if (!event.data.contentHeight) {
                                        return;
                                    }

                                    var h = event.data.contentHeight;

                                    if (!isNaN(h) && h > 0 && h !== if_height) {
                                        if_height = h;

                                        $(
                                            '#sureshot_iframe_container iframe'
                                        ).height(h);
                                    }
                                    return true;
                                },
                                false
                            );
                        }
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.sfdcsettings', {
            url: '/salesforce-settings',
            params: {
                pageIcon: 'ico-salesforce',
                pageTitle: ''
            },
            resolve: {
                featureflags: function ($q, FeatureFlagService) {
                    var deferred = $q.defer();

                    FeatureFlagService.GetAllFlags().then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                externaltypes: function ($q, SfdcStore) {
                    var deferred = $q.defer();

                    SfdcStore.getExternalTypes().then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                accountids: function ($q, SfdcStore, externaltypes) {
                    var deferred = $q.defer();

                    SfdcStore.getAccountIds(externaltypes).then(function (
                        result
                    ) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                orgs: function (
                    $q,
                    SfdcService,
                    SfdcStore,
                    accountids,
                    externaltypes
                ) {
                    var deferred = $q.defer(),
                        orgs = [];

                    SfdcStore.getOrgs().then(function (result) {
                        externaltypes.forEach(function (type) {
                            if (result[type] != undefined) {
                                orgs = orgs.concat(result[type]);
                            }
                        });
                        deferred.resolve(orgs);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': 'sales'
            }
        })
        .state('home.apiconsole', {
            url: '/apiconsole',
            params: {
                pageIcon: 'ico-api-console',
                pageTitle: 'API Console'
            },
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    templateUrl: 'app/apiConsole/views/APIConsoleView.html'
                }
            }
        })
        .state('home.signout', {
            url: '/signout',
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: function (LoginService) {
                        ShowSpinner('Logging Out...');
                        LoginService.Logout();
                    }
                }
            }
        })
        .state('home.users', {
            url: '/users',
            params: {
                pageIcon: 'ico-user',
                pageTitle: 'Manage Users'
            },
            resolve: {
                UserList: function ($q, UserManagementService) {
                    var deferred = $q.defer();

                    UserManagementService.GetUsers().then(function (result) {
                        if (result.Success) {
                            deferred.resolve(result.ResultObj);
                        } else {
                            deferred.reject(result);
                        }
                    });

                    return deferred.promise;
                }
            },
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: 'UserManagementWidgetController',
                    templateUrl:
                        'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
                }
            }
        })
        .state('home.insights', {
            url: '/insights',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'BIS Insights iFrame Testing',
                iframe: true
            },
            views: {
                'summary@': {
                    template: '<br><br>'
                },
                'main@': {
                    controller: 'LookupController',
                    controllerAs: 'vm',
                    templateUrl:
                        '/components/datacloud/lookup/lookup.component.html'
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
                'summary@': {
                    template:
                        '<br><div class="lookup-summary ten columns offset-one"><div class="lookup-back" ui-sref="home.insights"><ico class="fa fa-arrow-left"></ico>NEW LOOKUP</div></div></div>'
                },
                'main@': {
                    controller: function ($scope, LookupStore, $stateParams) {
                        var host = '/insights/';

                        $('#sureshot_iframe_container').html(
                            '<iframe id="insights_iframe" src="' +
                            host +
                            '" style="border: 1px inset"></iframe>'
                        );

                        var childWindow = document.getElementById(
                            'insights_iframe'
                        ).contentWindow;

                        window.addEventListener(
                            'message',
                            handleMessage,
                            false
                        );

                        function handleMessage(event) {
                            console.log('receiving from Insights:', event.data);
                            if (event.data == 'init') {
                                var json = {};

                                json.Authentication = LookupStore.get(
                                    'Authentication'
                                );
                                json.request = LookupStore.get('request');
                                //json.request.record = $stateParams.record;

                                console.log('posting to Insights:', json);
                                childWindow.postMessage(json, '*');
                            }
                        }

                        $scope.$on('$destroy', function () {
                            window.removeEventListener(
                                'message',
                                handleMessage
                            );
                        });
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        })
        .state('home.dante', {
            url: '/dante',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'BIS Dante iFrame Testing',
                iframe: true
            },
            views: {
                'summary@': {
                    template: '<br><br>'
                },
                'main@': {
                    controller: 'LookupController',
                    controllerAs: 'vm',
                    templateUrl:
                        '/components/datacloud/lookup/lookup.component.html'
                }
            }
        })
        .state('home.dante.iframe', {
            url: '/iframe',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Dante Migration Testbed'
            },
            views: {
                'summary@': {
                    template: '<br>'
                },
                'main@': {
                    controller: function ($scope, LookupStore, $stateParams) {
                        // var sin  = '?sin=33b905c6-faa8-42f8-af3a-4e2eaf64ca61';
                        // var surl = '&serverurl=https://internal-public-lpi-b-507116299.us-east-1.elb.amazonaws.com';
                        // var rec  = '&Recommendation=df0b96b0-4f22-4854-9b95-e8e98e379fc6';
                        // var ulnk = '&userlink=ACCT0002';
                        // var hsp  = '&HasSalesprism=false&CustomSettings=';
                        // var settings = {
                        //     ShowScore: false,
                        //     ShowLift: false,
                        //     ShowPurchaseHistory: false,
                        //     NoPlaysMessage: 'No Plays Found.',
                        //     NoDataMessage: 'No Data Found.',
                        //     hideNavigation: true,
                        //     HideTabs: true,
                        //     HideHeader: true,
                        //     DefaultTab: '',
                        //     SupportEmail: 'smeng@lattice-engines.com'
                        // };

                        // var host = '/dante' + sin + surl + rec + ulnk + hsp +
                        //                     JSON.stringify(settings);

                        var host =
                            '/dante?sin=806262c0-3053-458e-8fb8-0128416e7c82&serverurl=https://testapi.lattice-engines.com&Directory=salesforce&userlink=00561000002sfm8AAA&Recommendation=76b4a228-ba12-4c47-801d-dc8aeb3365fd&HasSalesprism=false&CustomSettings=%7B"SupportEmail"%3A"pliu%40lattice-engines.com"%2C"ShowScore"%3A%20false%2C"ShowLift"%3A%20false%2C"ShowPurchaseHistory"%3A%20true%2C"NoPlaysMessage"%3A"No%20Plays%20Found."%2C"NoDataMessage"%3A"No%20Data%20Found."%2C"hideNavigation"%3A%20false%2C"HideTabs"%3A%20false%2C"HideHeader"%3A%20true%2C"DefaultTab"%3A"TalkingPoints"%7D&PurchaseHistoryAccount=0016100001RU35QAAT';
                        //var host = '/dante?sin=806262c0-3053-458e-8fb8-0128416e7c82&serverurl=https://testapi.lattice-engines.com&Directory=salesforce&userlink=00561000002sfm8AAA&Recommendation=cf5132a2-6212-4f81-ba43-1783be7f695c&HasSalesprism=false&CustomSettings=%7B%22SupportEmail%22%3A%22pliu%40lattice-engines.com%22%2C%22ShowScore%22%3A%20false%2C%22ShowLift%22%3A%20false%2C%22ShowPurchaseHistory%22%3A%20true%2C%22NoPlaysMessage%22%3A%22No%20Plays%20Found.%22%2C%22NoDataMessage%22%3A%22No%20Data%20Found.%22%2C%22hideNavigation%22%3A%20false%2C%22HideTabs%22%3A%20false%2C%22HideHeader%22%3A%20false%2C%22DefaultTab%22%3A%22TalkingPoints%22%7D&PurchaseHistoryAccount=0016100001RU35QAAT';
                        var host =
                            '/dante/?sin=e95f1069-09b5-4cf4-8f9d-9de2c5f62d19&serverurl=https://testapi.lattice-engines.com&Directory=salesforce&userlink=00561000000hjkMAAQ&HasSalesprism=false&CustomSettings=%7B%22SupportEmail%22%3A%22pliu%40lattice-engines.com%22%2C%22ShowScore%22%3A%20false%2C%22ShowLift%22%3A%20false%2C%22ShowPurchaseHistory%22%3A%20true%2C%22NoPlaysMessage%22%3A%22No%20Plays%20Found.%22%2C%22NoDataMessage%22%3A%22No%20Data%20Found.%22%2C%22hideNavigation%22%3A%20true%2C%22HideTabs%22%3A%20true%2C%22HideHeader%22%3A%20true%2C%22DefaultTab%22%3A%22TalkingPoints%22%7D&PurchaseHistoryAccount=hierarchy_acct3';

                        var host =
                            '/dante/?sin=7877bc5e-4a27-4374-9ddd-7197228a6d5b&serverurl=https://testapi.lattice-engines.com&Directory=salesforce&userlink=005f4000000eArMAAU&Recommendation=c4b5f07a-b7cf-43d3-9625-5895d589c3a7&HasSalesprism=false&CustomSettings=%7B%22SupportEmail%22%3A%22pgavade%40lattice-engines.com%22%2C%22ShowScore%22%3A%20false%2C%22ShowLift%22%3A%20false%2C%22ShowPurchaseHistory%22%3A%20true%2C%22NoPlaysMessage%22%3A%22No%20Plays%20Found.%22%2C%22NoDataMessage%22%3A%22No%20Data%20Found.%22%2C%22hideNavigation%22%3A%20true%2C%22HideTabs%22%3A%20false%2C%22HideHeader%22%3A%20false%2C%22DefaultTab%22%3A%22TalkingPoints%22%7D&PurchaseHistoryAccount=001f4000006IDmxAAG';

                        $('#sureshot_iframe_container').html(
                            "<iframe id='dante_iframe' src='" +
                            host +
                            "'></iframe>"
                        );

                        var childWindow = document.getElementById(
                            'dante_iframe'
                        ).contentWindow;

                        window.addEventListener(
                            'message',
                            handleMessage,
                            false
                        );

                        function handleMessage(event) {
                            var split = event.data.split('=');
                            console.log('receiving from Dante:', split);
                            // setTimeout(function() {
                            //     console.log(
                            //         'posting to Dante:',
                            //         'CrmTabSelectedEvent=TalkingPoints'
                            //     );
                            //     childWindow.postMessage(
                            //         'CrmTabSelectedEvent=TalkingPoints',
                            //         '*'
                            //     );
                            // }, 5000);
                            if (split[0] == 'IFrameResizeEvent') {
                                document.getElementById(
                                    'dante_iframe'
                                ).style.height = split[1] + 'px';
                            }
                        }

                        $scope.$on('$destroy', function () {
                            window.removeEventListener(
                                'message',
                                handleMessage
                            );
                        });
                    },
                    templateUrl: 'app/marketo/views/SureshotTemplateView.html'
                }
            }
        });
}
