angular.module('mainApp.plays.controllers.PlayListController', [
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'common.utilities.browserstorage',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.plays.controllers.NoPlaysController'
])
.controller('PlayListController', function ($scope, $http, $rootScope, $compile, WidgetConfigUtility, 
    BrowserStorageUtility, WidgetFrameworkService, DanteWidgetService, NotionService) {
    
    var data = BrowserStorageUtility.getCurrentRootObject();
    var metadata = BrowserStorageUtility.getMetadata();
    var widgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
    
    var playListScreenConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig, 
        "playListScreenWidget"
    );
    
    if (playListScreenConfig == null) {
        return;
    }
    
    NotionService.findItemsByKey("DanteLead", data.SalesforceAccountID, "SalesforceAccountID").then(function(result) {
        if (result == null) {
            return;
        }
        
        data.Preleads = result;
        
        // If there are no Plays then just display a message
        if (data.Preleads == null || data.Preleads.length === 0) {
            $http.get('app/plays/views/NoPlaysView.html').success(function (html) {
                var scope = $rootScope.$new();
                scope.accountName = data.DisplayName;
                var mainContentView = $("#mainContentView");
                mainContentView.addClass("no-plays-background");
                $compile(mainContentView.html(html))(scope);
            });
            return;
        }
        
        var container = $('#playListScreen');
        WidgetFrameworkService.CreateWidget({
            element: container,
            widgetConfig: playListScreenConfig,
            metadata: metadata,
            data: data
        });
    });
});