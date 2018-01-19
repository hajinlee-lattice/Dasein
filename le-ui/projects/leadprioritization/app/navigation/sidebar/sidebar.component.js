angular
.module('pd.navigation.sidebar', [
    'pd.navigation.sidebar.root',
    'pd.navigation.sidebar.model',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.FeatureFlagService',
    'common.datacloud'
])
.controller('SidebarController', function($rootScope) {
    var vm = this;

    angular.extend(vm, {});

    vm.init = function() {
        if (typeof(sessionStorage) !== 'undefined') {
            if (sessionStorage.getItem('open-nav') === 'true' || !sessionStorage.getItem('open-nav')) {
                $("body").addClass('open-nav');
            } else {
                $("body").removeClass('open-nav');
            }
        }
    }

    vm.handleSidebarToggle = function($event) {
        var target = angular.element($event.target),
            collapsable_click = !target.parents('.menu').length;

        if (collapsable_click) {
            $('body').toggleClass('open-nav');
            $('body').addClass('controlled-nav');  // indicate the user toggled the nav

            if (typeof(sessionStorage) !== 'undefined'){
                sessionStorage.setItem('open-nav', $('body').hasClass('open-nav'));
            }

            $rootScope.$broadcast('sidebar:toggle');
        }
    }

    vm.init();
})
.service('SidebarStore', function(
    $state, $stateParams, StateHistory, ResourceUtility, 
    FeatureFlagService, DataCloudStore
) {
    var store = this;

    this.initialized = false;
    this.isDataAvailable = true;
    this.state = $state;
    this.stateParams = $stateParams;
    this.StateHistory = StateHistory;

    this.MyDataStates = [
        'home.nodata',
        'home.segment.explorer.attributes',
        'home.segment.explorer.builder',
        'home.segment.accounts',
        'home.segment.contacts'
    ];

    this.items = {
        root: [],
        model: []
    }

    this.init = function() {
        this.isDataAvailable = (DataCloudStore.metadata.enrichmentsTotal == 0) ? false : true;

        FeatureFlagService.GetAllFlags().then(function(result) {
            var flags = FeatureFlagService.Flags();
            
            store.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            store.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            store.showApiConsole = FeatureFlagService.FlagIsEnabled(flags.API_CONSOLE_PAGE);
            store.showMarketoSettings = FeatureFlagService.FlagIsEnabled(flags.USE_MARKETO_SETTINGS);
            store.showEloquaSettings = FeatureFlagService.FlagIsEnabled(flags.USE_ELOQUA_SETTINGS);
            store.showSalesforceSettings = FeatureFlagService.FlagIsEnabled(flags.USE_SALESFORCE_SETTINGS);
            store.showCampaignsPage = FeatureFlagService.FlagIsEnabled(flags.CAMPAIGNS_PAGE);
            store.showAnalysisPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            store.showPlayBook = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            store.showRatingsEngine = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            store.showSegmentationPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            store.showCdlEnabledPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            store.showLatticeInsightsPage = FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS);
            store.showContactUs = false;

            store.sources = {
                root: [{
                        if: store.showAnalysisPage,
                        active: store.checkMyDataActiveState,
                        sref: store.getMyDataState() + "({ segment: 'Create' })",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_MYDATA"),
                        icon: "ico-analysis ico-light-gray"
                    },{
                        if: store.showSegmentationPage,
                        active: store.checkSegmentationActiveState,
                        sref: "home.segments",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_SEGMENTATION"),
                        icon: "ico-segments ico-light-gray"
                    },{
                        if: store.showRatingsEngine,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.ratingsengine') && !store.isTransitingFrom(['home.ratingsengine', 'home.ratingsengine.list', 'home.ratingsengine.list.ratings']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.ratingsengine', 'home.ratingsengine.list', 'home.ratingsengine.list.ratings']);
                        },
                        sref: "home.ratingsengine",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_RATING_ENGINE")
                    },{
                        if: store.showPlayBook,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.playbook') && !store.isTransitingFrom(['home.playbook']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.playbook']);
                        },
                        sref: "home.playbook",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_PLAY_BOOK"),
                        icon: "ico-playbook"
                    },{
                        if: !store.showCdlEnabledPage,
                        disabled: !store.isDataAvailablstoree && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.models') || store.state.includes('home.models.history') && !store.isTransitingFrom(['home.models','home.models.history']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.models','home.models.history']);
                        },
                        sref: "home.models",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_MODEL_LIST")
                    },{
                        if: store.showCampaignsPage,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.campaigns') && !store.isTransitingFrom(['home.campaigns']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.campaigns']);
                        },
                        sref: "home.campaigns",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_CAMPAIGNS"),
                        icon: "ico-campaign ico-light-gray"
                    },{
                        if: !store.showCdlEnabledPage,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.datacloud.explorer') || store.state.includes('home.datacloud.lookup') && !store.isTransitingFrom(['home.datacloud.explorer','home.datacloud.explorer']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.datacloud.lookup','home.datacloud.explorer']);
                        },
                        sref: "home.datacloud.explorer({section:'edit',category:'',subcategory:''})",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_DATA_CLOUD"),
                        icon: "ico-enrichment ico-light-gray"
                    },{
                        if: store.showMarketoSettings && !store.showCdlEnabledPage,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.marketosettings') && !store.isTransitingFrom(['home.marketosettings']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.marketosettings']);
                        },
                        sref: "home.marketosettings.apikey",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_MARKETO"),
                        icon: "ico-marketo ico-light-gray"
                    },{
                        if: store.showEloquaSettings && !store.showCdlEnabledPage,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        active: function() {
                            return store.state.includes('home.eloquasettings') && !store.isTransitingFrom(['home.eloquasettings']);
                        },
                        transitioning: function() {
                            return store.isTransitingTo(['home.eloquasettings']);
                        },
                        sref: "home.eloquasettings.apikey",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_ELOQUA"),
                        icon: "ico-eloqua ico-light-gray"
                    },{
                        if: store.showSalesforceSettings,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        sref: "home.sfdcsettings",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_SFDC"),
                        icon: "ico-salesforce ico-light-gray"
                    },{
                        if: store.showApiConsole && !store.showCdlEnabledPage,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        sref: "home.apiconsole",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_API_CONSOLE"),
                        icon: "ico-api-console ico-light-gray"
                    },{
                        if: store.showContactUs,
                        disabled: !store.isDataAvailable && store.showCdlEnabledPage,
                        href: "https://docs.google.com/forms/d/e/1FAIpQLSdxVGLgkna6zA_m2z6TF4eVH5OtF_qHPtyq80Oiy53vu9Of3A/viewform",
                        target: "_contact_us",
                        label: ResourceUtility.getString("NAVIGATION_SIDEBAR_LP_CONTACT_US"),
                        icon: "ico-contact-us ico-light-gray"
                    }
                ]
            }

            Object.keys(store.sources).forEach(function(type) {
                store.setDefaults(type);
                store.items[type] = store.items[type].concat(store.sources[type]);
            });
        });
    }

    this.setDefaults = function(type) {
        var items = this.sources[type],
            template = {
                if: true,
                disabled: false,
                active: function() {},
                transitioning: function() {},
                sref: '',
                href: null,
                target: null,
                icon: "ico-model ico-light-gray"
            },
            properties = Object.keys(template);

        items.forEach(function(item) {
            properties.forEach(function(property) {
                if (!item.hasOwnProperty(property)) {
                    item[property] = template[property];
                }
            });
        });

        return items;
    };

    store.getMyDataState = function() {
        return store.isDataAvailable ? "home.segment.explorer.attributes" : "home.nodata";
    }

    store.checkMyDataActiveState = function() {
        return store.isStateName(store.MyDataStates) && (!store.stateParams.segment || store.stateParams.segment == 'Create')
    }

    store.checkSegmentationActiveState = function() {
        return store.isStateName(store.MyDataStates) && (store.stateParams.segment && store.stateParams.segment != 'Create') || store.state.current.name == 'home.segments';
    }

    store.checkToState = function(toState) {
        return StateHistory.lastTo().name == toState;
    }

    store.isStateName = function(state_names) {
        return (state_names || []).indexOf($state.current.name) !== -1; 
    }

    store.isTransitingFrom = function(state_names) {
        return false;
        return (state_names || []).indexOf(StateHistory.lastFrom().name) !== -1 && (state_names || []).indexOf(StateHistory.lastTo().name) !== -1; 
    }

    store.isTransitingTo = function(state_names) {
        return false;
        return (state_names || []).indexOf(StateHistory.lastTo().name) !== -1 && (state_names || []).indexOf($state.current.name) === -1; 
    }

    store.isTransitingToMyData = function(state_names) {
        return false;
        return (state_names || []).indexOf($state.current.name) === -1 && store.isTransitingTo(state_names) && (!StateHistory.lastToParams().segment || StateHistory.lastToParams().segment == 'Create');
    }

    store.isTransitingToSegmentation = function(state_names) {
        return false;
        return store.isTransitingTo(state_names) && (StateHistory.lastToParams().segment && StateHistory.lastToParams().segment != 'Create') || ('home.segments' !== $state.current.name && StateHistory.lastTo().name === 'home.segments');
    }

    this.init();
});