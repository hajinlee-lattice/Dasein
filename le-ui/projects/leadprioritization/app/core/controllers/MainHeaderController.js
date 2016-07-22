angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.FeatureFlagService'
])
.controller('MainHeaderController', function ($scope, $rootScope, $state, ResourceUtility, BrowserStorageUtility, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;

    var clientSession = BrowserStorageUtility.getClientSession();
    
    if (clientSession != null) {
        var Tenant = clientSession ? clientSession.Tenant : null;

        $scope.userDisplayName = clientSession.DisplayName;
        $scope.tenantName = window.escape(Tenant.DisplayName);
    }

    $scope.showProfileNav = false;
    
    var StateToPageTitleMap = {
        'home.models': {
            icon: 'ico-model',
            title: 'Model Workshop'
        },
        'home.models.history': {
            icon: 'ico-history',
            title: 'Model Creation History'
        },
        'home.models.import': {
            icon: 'ico-model',
            title: 'Create Model - CSV Training File'
        },
        'home.models.import.columns': {
            icon: 'ico-model',
            title: 'Create Model - CSV Training File - Field Mapping'
        },
        'home.models.import.job': {
            icon: 'ico-model',
            title: 'Create Model - CSV Training File - Build'
        },
        'home.models.pmml': {
            icon: 'ico-model',
            title: 'Create Model - PMML Import'
        },
        'home.models.pmml.job': {
            icon: 'ico-model',
            title: 'Create Model - PMML Import - Build'
        },
        'home.model.attributes': {
            icon: 'ico-attributes',
            title: ''
        },
        'home.model.performance': {
            icon: 'ico-performance',
            title: ''
        },
        'home.model.leads': {
            icon: 'ico-leads',
            title: ''
        },
        'home.model.summary': {
            icon: 'ico-datatable',
            title: ''
        },
        'home.model.alerts': {
            icon: 'ico-alerts',
            title: ''
        },
        'home.model.jobs': {
            icon: 'ico-scoring',
            title: ''
        },
        'home.model.review': {
            icon: 'ico-datatable',
            title: ''
        },
        'home.model.review.columns': {
            icon: 'ico-datatable',
            title: ''
        },
        'home.model.refine': {
            icon: 'ico-refine',
            title: ''
        },
        'home.users': {
            icon: 'ico-user',
            title: 'Manage Users'
        },
        'home.enrichment': {
            icon: 'ico-history',
            title: 'Lead Enrichment'
        },
        'home.jobs.status': {
            icon: 'ico-cog',
            title: 'Jobs Status'
        },
        'home.jobs.status.ready': {
            icon: 'ico-cog',
            title: 'View Report'
        },
        'home.jobs.status.csv': {
            icon: 'ico-cog',
            title: 'View Report'
        },
        'home.marketosettings.apikey': {
            icon: 'ico-marketo',
            title: 'Marketo Settings'
        },
        'home.eloquasettings.apikey': {
            icon: 'ico-eloqua',
            title: 'Eloqua Settings'
        }, 
        'home.sfdcsettings': {
            icon: 'ico-salesforce',
            title: 'Salesforce Settings'
        }, 
        'home.apiconsole': {
            icon: 'ico-api-console',
            title: 'API Console'
        },
        'home.updatepassword': {
            icon: 'ico-user',
            title: 'User Settings'
        }
    }

    $rootScope.$on('$stateChangeSuccess', function(e, toState, toParams, fromState, fromParams) {
        if (StateToPageTitleMap[toState.name]) { 
            $scope.pageDisplayIcon = StateToPageTitleMap[toState.name].icon;
            $scope.pageDisplayName = StateToPageTitleMap[toState.name].title;
        } else {
            $scope.pageDisplayIcon = null;
            $scope.pageDisplayName = null;
        }

        if (isModelDetailState(fromState.name) && ! isModelDetailState(toState.name)) {
            $scope.isModelDetailsPage = false;
        }
    });

    function isModelDetailState(stateName) {
        var stateNameArr = stateName.split('.');
        if (stateNameArr[0] == 'home' && stateNameArr[1] == 'model') {
            return true;
        }
        return false;
    }

    $scope.$on('model-details', function(event, args) {
        $scope.isModelDetailsPage = true;
        $scope.modelDisplayName = args.displayName;
    });
    
    checkBrowserWidth();

    $(window).resize(checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        $("body").toggleClass("open-nav");
        $("body").addClass("controlled-nav");  // indicate the user toggled the nav
    }

    $(document.body).click(function() {
        if ($scope.showProfileNav) {
            $scope.showProfileNav = false;
            $scope.$apply();
        }
    });

    $scope.headerClicked = function($event) {
        $scope.showProfileNav = !$scope.showProfileNav;
        $event.stopPropagation();
    };

    function checkBrowserWidth(){
      // if the user has closed the nav, leave it closed when increasing size
      if (window.matchMedia("(min-width: 1200px)").matches && !$("body").hasClass("controlled-nav")) {
        if (typeof(sessionStorage) !== 'undefined') {
            if(sessionStorage.getItem('open-nav') === 'true') {
                $("body").addClass('open-nav');
            } else {
                $("body").removeClass('open-nav');
            }
        }
      } else {
        if($("body").hasClass("open-nav")) {
          // if the nav is open when scaling down close it but allow it to re-open by removing our user controlled class indicator
          $("body").removeClass("controlled-nav");
        }
        $("body").removeClass("open-nav");
      }
    }
});
