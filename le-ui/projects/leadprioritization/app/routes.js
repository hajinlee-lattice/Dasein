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
    var UnderConstruction = '<div style="text-align:center;margin-top:5em;"><img src="/assets/images/headbang.gif" /></div>',
        ModelDependencies = {
            Model: function($q, $stateParams, ModelStore) {
                var deferred = $q.defer(),
                    id = $stateParams.modelId;
                
                ModelStore.getModel(id).then(function(result) {
                    deferred.resolve(result);
                });

                return deferred.promise;
            }
        };

    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('home', {
            url: '/', // '/:tenantId',
            /* eventually... detect tenantId/choose tenantId
            resolve: {
                Tenant: function($q, $stateParams, $scope, $controller) {
                    var deferred = $q.defer(),
                        tenantId = $stateParams.tenantId,
                        LoginCtrl = $controller('LoginController', {});

                    if (tenantId)
                        deferred.resolve(true);
                    
                    console.log('hi',deferred,tenantId,LoginCtrl);
                    return deferred.promise;
                }
            },
            */
            views: {
                "navigation": {
                    controller: 'SidebarRootController',
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                }
            },
            redirectTo: 'models'
        })
        .state('models', {
            url: '/models',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('models.import', {
            url: '/import',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'First, setup the model.';
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
        .state('models.import.columns', {
            url: '/:csvFileName/columns',
            resolve: {
                csvMetaData: function($stateParams, csvImportStore) {
                    return csvImportStore.Get($stateParams.csvFileName);
                },
                csvUnknownColumns: function($q, csvImportService, csvMetaData) {
                    var deferred = $q.defer();

                    csvImportService.GetUnknownColumns(csvMetaData).then(function(result) {
                        console.log(result);
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Specify Unknown Column Types';
                        }
                    },
                    controller: 'OneLineController',
                    templateUrl: 'app/navigation/summary/OneLineView.html'
                },
                "main@": {
                    controller: function($state, $stateParams, csvMetaData, csvUnknownColumns, csvImportService) {
                        console.log(csvUnknownColumns);
                        this.errors = csvUnknownColumns.ResultErrors;
                        this.data = csvUnknownColumns.Result;

                        this.csvSubmitColumns = function($event) {
                            ShowSpinner('Saving Changes...');

                            csvImportService.SetUnknownColumns(csvMetaData, this.data).then(function(result) {
                                csvImportService.StartModeling(csvMetaData).then(function(result) {
                                    $state.go('jobs.status');
                                });
                            });
                        }
                    },
                    controllerAs: 'vm',
                    templateUrl: 'app/create/views/CustomFieldsView.html'
                }   
            }
        })
        .state('models.create', {
            url: '/create',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'The model is ready to be created.';
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
        .state('model', {
            url: '/model/:modelId',
            resolve: ModelDependencies,
            views: {
                "navigation@": {
                    controller: function($scope, Model, FeatureFlagService) {
                        $scope.name = Model.ModelDetails.Name;
                        FeatureFlagService.GetAllFlags().then(function() {
                            var flags = FeatureFlagService.Flags();
                            $scope.showAlerts = FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
                            $scope.showRefine = FeatureFlagService.FlagIsEnabled(flags.SETUP_PAGE);
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
        .state('model.attributes', {
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
        .state('model.performance', {
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
        .state('model.leads', {
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
        .state('model.summary', {
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
        .state('model.alerts', {
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
        .state('model.scoring', {
            url: '/scoring',
            views: {
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
                    template: UnderConstruction
                }   
            }
        })
        .state('model.refine', {
            url: '/refine',
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
        .state('marketosettings', {
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
                            console.log(response);
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
        .state('marketosettings.apikey', {
            url: '/apikey',
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Marketo API Key';
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
        .state('marketosettings.models', {
            url: '/models',
            views: { 
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Enable Lattice Models in Marketo';
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
        .state('signout', {
            url: '/signout', 
            views: {
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Shutting Down';
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
        .state('updatepassword', {
            url: '/updatepassword',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'Update Your Password';
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
        .state('deploymentwizard', {
            url: '/deploymentwizard',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
                    templateUrl: 'app/navigation/sidebar/RootView.html'
                },
                "summary@": {
                    resolve: { 
                        ResourceString: function() {
                            return 'hi';
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
        .state('activate', {
            url: '/activate',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('users', {
            url: '/users',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('setup', {
            url: '/setup',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('history', {
            url: '/history',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('fields', {
            url: '/fields',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
        .state('dashboard', {
            url: '/dashboard',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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
                    template: UnderConstruction
                }   
            }
        })
        .state('enrichment', {
            url: '/enrichment',
            views: {
                "navigation@": {
                    controller: 'SidebarRootController',
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