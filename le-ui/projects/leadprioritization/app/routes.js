angular
.module('mainApp')

// add ability to redirect with redirectTo
.run(['$rootScope', '$state', 'ResourceUtility', function($rootScope, $state, ResourceUtility) {

    $rootScope.$on('$stateChangeStart', function(evt, to, params) {
        var LoadingString = ResourceUtility.getString("GENERAL_LOADING");

        if (to.redirectTo) {
            evt.preventDefault();
            $state.go(to.redirectTo, params)
        }

        ShowSpinner(LoadingString);
    });
    /*
    $rootScope.$on('$stateChangeSuccess', function(evt, to, params) {
        console.log('END');
    });
    */
}])

// define routes for PD application.
.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    var ModelDependencies = {
            Model: function($q, $stateParams, ModelStore) {
                var deferred = $q.defer(),
                    id = $stateParams.modelId;
                
                ModelStore.getModel(id).then(function(result) {
                    deferred.resolve(result);
                });

                return deferred.promise;
            },
            loadAlaSQL: ['$ocLazyLoad', function($ocLazyLoad) {
                return $ocLazyLoad.load('lib/js/alasql.min.js');
            }]
        };

    $urlRouterProvider.otherwise('/tenant/');

    $stateProvider
        .state('home', {
            url: '/tenant/:tenantId',
            resolve: {
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
                    controller: function($stateParams, $state, BrowserStorageUtility) {
                        var tenantId = $stateParams.tenantId,
                            ClientSession = BrowserStorageUtility.getClientSession(),
                            Tenant = ClientSession ? ClientSession.Tenant : null;

                        if (tenantId != Tenant.DisplayName) {
                            $state.go('home.models', { tenantId: window.escape(Tenant.DisplayName) });
                        }
                    },
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                }
            }
        })
        .state('home.models', {
            url: '/models',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    templateUrl: 'app/navigation/summary/ModelListView.html'
                },
                "main@": {
                    templateUrl: 'app/models/views/ModelListView.html'
                }   
            }
        })
        .state('home.models.import', {
            url: '/import',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_IMPORT_MODEL_CSV_UPLOAD';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/create/views/CSVImportView.html'
                }   
            }
        })
        .state('home.models.import.columns', {
            url: '/:csvFileName/columns',
            resolve: {
                csvMetaData: function($stateParams, csvImportStore) {
                    return csvImportStore.Get($stateParams.csvFileName);
                },
                csvUnknownColumns: function($q, csvImportService, csvMetaData) {
                    var deferred = $q.defer();

                    csvImportService.GetUnknownColumns(csvMetaData).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_IMPORT_MODEL_CSV_COLUMNS';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: function($state, $stateParams, csvMetaData, csvUnknownColumns, csvImportService) {
                        this.errors = csvUnknownColumns.ResultErrors;
                        this.data = csvUnknownColumns.Result;

                        this.csvSubmitColumns = function($event) {
                            ShowSpinner('Saving Changes...');

                            csvImportService.SetUnknownColumns(csvMetaData, this.data).then(function(result) {
                                csvImportService.StartModeling(csvMetaData).then(function(result) {
                                    $state.go('home.jobs.status', {'jobCreationSuccess': result.Success });
                                });
                            });
                        }
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/create/views/CustomFieldsView.html'
                }   
            }
        })
        .state('home.models.create', {
            url: '/create',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_IMPORT_MODEL_CSV_READY';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/create/views/CreateModelView.html'
                }   
            }
        })
        .state('home.model', {
            url: '/model/:modelId',
            resolve: ModelDependencies,
            views: {
                "navigation@": {
                    controller: function($scope, Model, FeatureFlagService) {
                        $scope.name = Model.ModelDetails.Name;
                        $scope.displayName = Model.ModelDetails.DisplayName;
                        FeatureFlagService.GetAllFlags().then(function() {
                            var flags = FeatureFlagService.Flags();
                            $scope.showModelSummary = FeatureFlagService.FlagIsEnabled(flags.ADMIN_PAGE);
                            $scope.showAlerts = FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
                        });
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
            views: {
                "main@": {
                    controller: function($scope, $compile, ModelStore) {
                        $scope.data = ModelStore.data;
                    },
                    templateUrl: 'app/AppCommon/widgets/adminInfoSummaryWidget/AdminInfoSummaryWidgetTemplate.html'
                }   
            }
        })
        .state('home.model.alerts', {
            url: '/alerts',
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
                            return 'Import testing set';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/models/views/BulkScoringImportData.html'
                }   
            }
        })
        .state('home.model.refine', {
            url: '/refine',
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
                    templateUrl: 'app/navigation/sidebar/MarketoSettingsView.html'
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
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_APIKEY';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
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
            views: { 
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_MARKETO_MODELS';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
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
        .state('home.apiconsole', {
            url: '/apiconsole',
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
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: function(LoginService) {
                        ShowSpinner('Signing Out...');
                        LoginService.Logout();
                    }
                }
            }
        })
        .state('home.updatepassword', {
            url: '/updatepassword',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'SUMMARY_PASSWORD_UPDATE';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
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
                    templateUrl: 'app/navigation/summary/OneLineView.html'
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
        .state('home.history', {
            url: '/history',
            views: {
                "navigation@": {
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'MODEL_LIST_CREATION_HISTORY';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    templateUrl: 'app/models/views/ModelCreationHistoryView.html'
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
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: 'LeadEnrichmentController',
                    templateUrl: 'app/setup/views/LeadEnrichmentView.html'
                }   
            }
        });
}]);

function ShowSpinner(LoadingString) {
    // state change spinner
    $('#mainContentView').html(
        '<section id="main-content" class="container">' +
        '<div class="row twelve columns"><div class="loader"></div>' +
        '<h2 class="text-center">' + LoadingString + '</h2></div></section>');
}
