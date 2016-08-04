angular
.module('mainApp')
.run(function($rootScope, $state, ResourceUtility, ServiceErrorUtility) {
    $rootScope.$on('$stateChangeStart', function(evt, to, params) {
        var LoadingString = ResourceUtility.getString("");

        if (to.redirectTo) {
            evt.preventDefault();
            $state.go(to.redirectTo, params)
        }

        ShowSpinner(LoadingString);
        ServiceErrorUtility.hideBanner();
    });
    
    $rootScope.$on('$stateChangeSuccess', function(evt, to, params) {

    });
    
    $rootScope.$on('$stateChangeError', function(evt, to, params) {
        if ($state.current.name != to.name) {
            console.log('-!- error; could not load '+to.name);
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
                ResourceStrings: function($q, BrowserStorageUtility, ResourceStringsService) {
                    var deferred = $q.defer(),
                        session = BrowserStorageUtility.getClientSession();

                    ResourceStringsService.GetInternalResourceStringsForLocale(session.Locale).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "navigation": {
                    controller: function($rootScope, $stateParams, $state, BrowserStorageUtility) {
                        var tenantName = $stateParams.tenantName,
                            ClientSession = BrowserStorageUtility.getClientSession(),
                            Tenant = ClientSession ? ClientSession.Tenant : null;

                        if (tenantName != Tenant.DisplayName) {
                            $rootScope.tenantName = window.escape(Tenant.DisplayName);
                            
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
                pageTitle: 'My Models',
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
                pageTitle: 'My Models'
            },
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
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

                        FeatureFlagService.GetAllFlags().then(function() {
                            var flags = FeatureFlagService.Flags();
                            $scope.showModelSummary = FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE);
                            $scope.showAlerts = 0;// disable for all (PLS-1670) FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
                            $scope.showReviewAndClone = FeatureFlagService.FlagIsEnabled(flags.REVIEW_CLONE);
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
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsAttributesTab" class="tab-content" data-top-predictor-widget></div>'))($scope);
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
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="performanceTab" class="tab-content" data-performance-tab-widget></div>'))($scope);
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
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#modelDetailContainer').html('<div id="modelDetailsLeadsTab" class="tab-content" data-leads-tab-widget></div>'))($scope);
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
                    controller: function($scope, $compile, ModelStore, IsPmml) {
                        $scope.data = ModelStore.data;
                        $scope.IsPmml = IsPmml;
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
                                if (result != null && result.success === true) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = suppressedCategories;
                                    deferred.resolve(result);
                                } else if (result != null && result.success === false) {
                                    data.ModelAlerts = result.resultObj;
                                    data.SuppressedCategories = null;
                                    deferred.reject('nope');
                                }
                            });

                            return deferred.promise;
                        }
                    },
                    controller: function($scope, ModelStore) {
                        $scope.data = ModelStore.data;
                    },
                    templateUrl: 'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
                }   
            }
        })
        .state('home.model.scoring', {
            url: '/scoring',
            redirectto: 'model.scoring.import',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Import a Report to Score';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    resolve: {
                        RequiredFields: function($q, $http, $stateParams) {
                            var deferred = $q.defer(),
                                modelId = $stateParams.modelId;

                            $http({
                                'method': "GET",
                                'url': '/pls/modelsummaries/metadata/required/' + modelId
                            }).then(function(response) {
                                deferred.resolve(response.data);
                            });

                            return deferred.promise; 
                        }
                    },
                    controller: 'csvBulkUploadController',
                    controllerAs: 'vm',
                    templateUrl: 'app/models/views/BulkScoringImportData.html'
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
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                        $compile($('#manageFieldsPageContainer').html('<div id=manageFieldsTab" class="tab-content" data-manage-fields></div>'))($scope);
                    },
                    template: '<div id="manageFieldsPageContainer" class="manage-fields container"></div>'
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
        .state('home.marketosettings', {
            url: '/marketosettings',
            redirectto: 'marketosettings.apikey',
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
            },
            views: {
                "navigation@": {
                    // -- ben::bookmark 
                    // templateUrl: 'app/navigation/sidebar/MarketoSettingsView.html'
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
        .state('home.marketosettings.apikey', {
            url: '/apikey',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Settings'
            },
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    controller: function(urls) {
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.creds_url + '"></iframe>');
                    },
                    template: '<div id="sureshot_iframe_container"></div>'
                }   
            }
        })
        .state('home.marketosettings.models', {
            url: '/models',
            params: {
                pageIcon: 'ico-marketo',
                pageTitle: 'Marketo Settings'
            },
            views: { 
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_MODELS';
                        }
                    },
                    /*
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                    -- ben::bookmark
                    */
                    templateUrl: 'app/navigation/summary/MarketoTabs.html'
                },
                "main@": {
                    controller: function(urls) { 
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.scoring_settings_url + '"></iframe>');
                    },
                    template: '<div id="sureshot_iframe_container"></div>'
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
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.creds_url + '"></iframe>');
                    },
                    template: '<div id="sureshot_iframe_container"></div>'
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
                        $('#sureshot_iframe_container')
                            .html('<iframe src="' + urls.scoring_settings_url + '"></iframe>');
                    },
                    template: '<div id="sureshot_iframe_container"></div>'
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
                    templateUrl: 'app/userManagement/views/UserManagementView.html'
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
        .state('home.dashboard', {
            url: '/dashboard',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'UNDER CONSTRUCTION';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    template: 'dashboard'
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
                            EnrichmentStore.setMetadata('selectedToggle', bool);
                            EnrichmentStore.setMetadata('current', 1);
                        }
                    },
                    templateUrl: 'app/navigation/summary/EnrichmentTabs.html'
                },
                "main@": {
                    resolve: {
                        EnrichmentData: function($q, EnrichmentStore) {
                            var deferred = $q.defer();

                            EnrichmentStore.getEnrichments().then(function(result) {
                                deferred.resolve(result);
                            });

                            return deferred.promise;
                        },
                        EnrichmentCategories: function($q, EnrichmentStore) {
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

function ShowSpinner(LoadingString) {
    // state change spinner
    $('#mainContentView').html(
        '<section id="main-content" class="container">' +
        '<div class="row twelve columns"><div class="loader"></div>' +
        '<h2 class="text-center">' + LoadingString + '</h2></div></section>');
}
