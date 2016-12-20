angular
.module('mainApp')
.run(function($rootScope, $state, ResourceUtility, ServiceErrorUtility) {
    $rootScope.$on('$stateChangeStart', function(evt, toState, params, fromState, fromParams) {
        // when user hits browser Back button after app instantiate, send back to login
        if (fromState.name == 'home.models' && toState.name == 'home') {
            evt.preventDefault();
            window.open("/login", "_self");
        }

        var LoadingString = ResourceUtility.getString("");

        if (toState.redirectTo) {
            evt.preventDefault();
            $state.go(toState.redirectTo, params);
        }

        ShowSpinner(LoadingString);
        ServiceErrorUtility.hideBanner();
    });

    $rootScope.$on('$stateChangeSuccess', function(evt, toState, params) {

    });

    $rootScope.$on('$stateChangeError', function(evt, toState, params) {
        if ($state.current.name != toState.name) {
            console.log('-!- error; could not load '+toState.name);
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
                }
            },
            views: {
                "header": {
                    controller: 'HeaderController',
                    templateUrl: 'app/navigation/header/views/MainHeaderView.html'
                },
                "navigation": {
                    controller: function($rootScope, $stateParams, $state, Tenant) {
                        var tenantName = $stateParams.tenantName;

                        if (tenantName != Tenant.DisplayName) {
                            $rootScope.tenantName = window.escape(Tenant.DisplayName);
                            $rootScope.tenantId = window.escape(Tenant.Identifier);
                            
                            $state.go('home.models', { 
                                tenantName: Tenant.DisplayName
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
                }
            },
            views: {
                "navigation@": {
                    controller: function($scope, $rootScope, Model, IsPmml, FeatureFlagService) {
                        $scope.IsPmml = IsPmml;
                        $scope.sourceType = Model.ModelDetails.SourceSchemaInterpretation;
                        $scope.Uploaded = Model.ModelDetails.Uploaded;

                        FeatureFlagService.GetAllFlags().then(function() {
                            var flags = FeatureFlagService.Flags();
                            $scope.showModelSummary = FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE);
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
        .state('home.model.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-attributes',
                pageTitle: ''
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
                pageTitle: ''
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
        .state('home.model.leads', {
            url: '/leads',
            params: {
                pageIcon: 'ico-leads',
                pageTitle: ''
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
                pageTitle: ''
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
                pageTitle: ''
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
                pageTitle: ''
            },
            resolve: {
                DataRules: function($q, $stateParams, $http, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    RemodelStore.GetModelReviewDataRules(modelId).then(function(result) {
                        deferred.resolve(result);
                    }).catch(function(error) {
                        deferred.reject(error);
                    });

                    return deferred.promise;
                },
                Attributes: function ($q, $stateParams, RemodelStore) {
                    var deferred = $q.defer(),
                        modelId = $stateParams.modelId;

                    RemodelStore.GetModelReviewAttributes(modelId).then(function(result) {
                        deferred.resolve(result);
                    }).catch(function(error) {
                        deferred.reject(error);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    controller: function ($rootScope, Model) {
                        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
                    }
                },
                "main@": {
                    controller: 'RemodelController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/RemodelView.html'
                }
            }
        })
        .state('home.campaigns', {
            url: '/campaigns',
            params: {
                pageTitle: 'Campaigns',
                pageIcon: 'ico-campaign'
            },
            resolve: {
                CampaignList: function($q, CampaignStore) {
                    var deferred = $q.defer();

                    CampaignStore.getCampaigns(true).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "main@": {
                    controller: 'CampaignListController',
                    controllerAs: 'vm',
                    templateUrl: 'app/campaigns/views/CampaignListView.html'
                }
            }
        })
        .state('home.campaigns.models', {
            url: '/models',
            params: {
                pageTitle: 'Campaigns',
                pageIcon: 'ico-model'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "main@": {
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
                    // -- ben::bookmark 
                    // templateUrl: 'app/navigation/sidebar/MarketoSettingsView.html'
                    controller: function($scope, $state, FeatureFlagService) {
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
                        EnrichmentData: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getEnrichments({onlySelectedAttributes: true}).then(function(result) {
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
                    resolve: {
                        ResourceString: function() {
                            return 'SUMMARY_ELOQUA_ENRICHMENTS';
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
                        if(urls && urls.enrichment_settings_url) {
                            $('#sureshot_iframe_container')
                                .html('<iframe src="' + urls.enrichment_settings_url + '"></iframe>');
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
                    resolve: {
                        ResourceString: function() {
                            return 'SFDC_ACCESS_TOKEN';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneTabView.html'
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
                "main@": {
                    templateUrl: 'app/apiConsole/views/APIConsoleView.html'
                }
            }
        })
        .state('home.signout', {
            url: '/signout', 
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_SIGNOUT';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneTabView.html'
                },
                "main@": {
                    controller: function(LoginService) {
                        ShowSpinner('Logging Out...');
                        LoginService.Logout();
                    }
                }
            }
        })
        .state('home.updatepassword', {
            url: '/updatepassword',
            params: {
                pageIcon: 'ico-user',
                pageTitle: 'User Settings'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    template: ''
                },
                "main@": {
                    templateUrl: 'app/login/views/UpdatePasswordView.html'
                }
            }
        })
        .state('passwordsuccess', {
            url: '/passwordsuccess',
            views: {
                "main@": {
                    templateUrl: 'app/login/views/UpdatePasswordSuccessView.html'
                }
            }
        })
        .state('home.deploymentwizard', {
            url: '/deploymentwizard',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return '';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'DeploymentWizardController',
                    templateUrl: 'app/setup/views/DeploymentWizardView.html'
                }
            }
        })
        .state('home.activate', {
            url: '/activate',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'ACTIVATE_MODEL_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/models/views/ActivateModelView.html'
                }
            }
        })
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
                    resolve: { 
                        ResourceString: function() {
                            return 'USER_MANAGEMENT_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneTabView.html'
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
        .state('home.setup', {
            url: '/setup',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SYSTEM_SETUP_TITLE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/config/views/ManageCredentialsView.html'
                }   
            }
        })
        .state('home.fields', {
            url: '/fields',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SETUP_NAV_NODE_MANAGE_FIELDS';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'SetupController',
                    templateUrl: 'app/setup/views/SetupView.html'
                }   
            }
        })
        .state('home.lookup', {
            url: '/lookup',
            redirectTo: 'home.lookup.form'
        })
        .state('home.lookup.form', {
            url: '/form',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Lattice Data Cloud'
            },
            resolve: {
                Models: function($q, ModelStore) {
                    var deferred = $q.defer();

                    ModelStore.getModels().then(function(data) {
                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {

                },
                "main@": {
                    controller: 'LookupFormController',
                    controllerAs: 'vm',
                    templateUrl: 'app/lookup/form/FormView.html'
                }
            }
        })
        .state('home.lookup.tabs', {
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

                        deferred.resolve(data);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    controller: function(LookupResponse) {
                        if (LookupResponse.enrichmentAttributeValues) {
                            this.count = Object.keys(LookupResponse.enrichmentAttributeValues).length;
                        } else {
                            this.count = 0;
                        }
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/lookup/tabs/TabsView.html'
                }
            },
            redirectTo: 'home.lookup.tabs.response'
        })
        .state('home.lookup.tabs.response', {
            url: '/response',
            views: {
                "main@": {
                    controller: function(LookupResponse, LookupStore) {
                        var vm = this;

                        angular.extend(vm, {
                            elapsedTime: LookupStore.get('elapsedTime'),
                            response: LookupResponse
                        });
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/lookup/response/ResponseView.html'
                }
            }
        })
        .state('home.lookup.tabs.matching', {
            url: '/matching',
            views: {
                "main@": {
                    controller: function(LookupResponse, LookupStore) {
                        var vm = this;

                        angular.extend(vm, {
                            elapsedTime: LookupStore.get('elapsedTime'),
                            response: LookupResponse
                        });
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/lookup/matching/MatchingView.html'
                }
            }
        })
        .state('home.lookup.tabs.attr', {
            url: '/attr',
            views: {
                "main@": {
                    resolve: {
                        EnrichmentCount: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getCount().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentCategories: function($q, EnrichmentStore) {
                            return false;
                            var deferred = $q.defer();

                            EnrichmentStore.getCategories().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentPremiumSelectMaximum: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getPremiumSelectMaximum().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentAccountLookup: function($q, EnrichmentStore, LookupResponse) {
                            var deferred = $q.defer();

                            //EnrichmentStore.getPremiumSelectMaximum().then(function(result) {
                                deferred.resolve(LookupResponse.enrichmentAttributeValues);
                            //});

                            return deferred.promise;
                        }
                    },
                    controller: 'EnrichmentWizardController',
                    controllerAs: 'vm',
                    templateUrl: 'app/enrichment/views/EnrichmentWizardView.html'
                }
            }
        })
        .state('home.enrichment-new', {
            url: '/enrichment-new',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Lattice Data Cloud'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'LEAD_ENRICHMENT_SETUP_TITLE';
                        }
                    },
                    controller: function($scope, EnrichmentStore) {
                        $scope.metadata = EnrichmentStore.metadata;
                        $scope.$watch('metadata.current', function(newVal, oldVal){
                            if(newVal !== oldVal) {
                                angular.element(window).scrollTop(0,0);
                            }
                        });
                        $scope.selectToggle = function(bool) {
                            EnrichmentStore.setMetadata('toggle.show.selected', bool);
                            EnrichmentStore.setMetadata('current', 1);
                        }
                    },
                    //templateUrl: 'app/navigation/summary/EnrichmentTabs.html'
                },
                "main@": {
                    resolve: {
                        EnrichmentCount: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getCount().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentCategories: function($q, EnrichmentStore) {
                            return false;
                            var deferred = $q.defer();

                            EnrichmentStore.getCategories().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentPremiumSelectMaximum: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getPremiumSelectMaximum().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentAccountLookup: function() {
                            return null;
                        }
                    },
                    controller: 'EnrichmentWizardController',
                    controllerAs: 'vm',
                    templateUrl: 'app/enrichment/views/EnrichmentWizardView.html'
                }   
            }
        })
        .state('home.enrichment', {
            url: '/enrichment',
            params: {
                pageIcon: 'ico-enrichment',
                pageTitle: 'Enrichment'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'LEAD_ENRICHMENT_SETUP_TITLE';
                        }
                    },
                    controller: function($scope, EnrichmentStore) {
                        $scope.metadata = EnrichmentStore.metadata;
                        $scope.$watch('metadata.current', function(newVal, oldVal){
                            if(newVal !== oldVal) {
                                angular.element(window).scrollTop(0,0);
                            }
                        });
                        $scope.selectToggle = function(bool) {
                            EnrichmentStore.setMetadata('toggle.show.selected', bool);
                            EnrichmentStore.setMetadata('current', 1);
                        }
                    }//,
                    //templateUrl: 'app/navigation/summary/EnrichmentTabs.html'
                },
                "main@": {
                    resolve: {
                        EnrichmentCategories: function($q, EnrichmentStore) {
                            return false;
                            var deferred = $q.defer();

                            EnrichmentStore.getCategories().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentPremiumSelectMaximum: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getPremiumSelectMaximum().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        }
                    },
                    controller: 'EnrichmentController',
                    controllerAs: 'vm',
                    templateUrl: 'app/enrichment/views/EnrichmentView.html'
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
