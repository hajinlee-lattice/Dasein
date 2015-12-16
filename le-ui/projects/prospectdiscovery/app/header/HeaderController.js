angular.module('pd.header', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.core.utilities.NavUtility'
])

.controller('MainHeaderCtrl', function (
        $scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, 
        LoginService, FeatureFlagService
    ) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.showUserManagement = false;

    console.log('ResourceUtility', $scope.ResourceUtility);
    var clientSession = BrowserStorageUtility.getClientSession();
    
    $scope.userDisplayName = clientSession.DisplayName;

    
    if (clientSession != null) {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showSystemSetup = FeatureFlagService.FlagIsEnabled(flags.SYSTEM_SETUP_PAGE);
            $scope.showModelCreationHistoryDropdown = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showActivateModel = FeatureFlagService.FlagIsEnabled(flags.ACTIVATE_MODEL_PAGE);
            $scope.showSetup = FeatureFlagService.FlagIsEnabled(flags.SETUP_PAGE);
        });
    }
    

    $(".dropdown > a").click(function(e){
        $(this).toggleClass("active");
        $(".dropdown > ul").toggle();
        e.stopPropagation();
    });

    $(document).click(function() {
        if ($(".dropdown > ul").is(':visible')) {
            $(".dropdown > ul", this).hide();
            $(".dropdown > a").removeClass('active');
        }
    });
    // Toggle Collapsible Areas
    $(".toggle > a").click(function(e){
        $(this).parent().toggleClass("open");
        e.preventDefault();
        if ($(".toggle > ul").is(':visible')) {
            $(".toggle > a > span:last-child").removeClass("fa-angle-double-down");
            $(".toggle > a > span:last-child").addClass("fa-angle-double-up");
        } else {
            $(".toggle > a > span:last-child").removeClass("fa-angle-double-up");
            $(".toggle > a > span:last-child").addClass("fa-angle-double-down");
        }
    });

    $scope.handleClick = function ($event, name) {
        $event ? $event.preventDefault() : null;

        switch(name) {
            case 'dropdown':
                // Clickable Dropdown


                break;
            case 'logout': 
                LoginService.Logout(); 
                return;
            default:
                break;
        }

        name ? $rootScope.$broadcast(NavUtility[name]) : null;
    };

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        console.log('BUH');
        $("body").toggleClass("open-nav");
    }

    function checkBrowserWidth(){
        if (window.matchMedia("(min-width: 768px)").matches) {
            $("body").addClass("open-nav");
        } else {
            $("body").removeClass("open-nav");
        }
    }
});