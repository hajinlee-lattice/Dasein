angular.module('mainApp.leads.controllers.LeadDetailsController', [
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'common.utilities.browserstorage',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService'
])
.controller('LeadDetailsController', function ($scope, BrowserStorageUtility, WidgetConfigUtility, DanteWidgetService, WidgetFrameworkService, NotionService) {
    
    var data = BrowserStorageUtility.getCurrentRootObject();
    var metadata = BrowserStorageUtility.getMetadata();
    var widgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
  
    var leadTileWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "leadDetailsTileWidget"
    );
    
    if (leadTileWidgetConfig != null && data != null) {
        var tileContainer = $('.js-dante-lead-details-nav-tiles-container');
        WidgetFrameworkService.CreateWidget({
            element: tileContainer,
            widgetConfig: leadTileWidgetConfig,
            metadata: metadata,
            data: data
        });
    }
    
    // NotionService.findOne("FrontEndCombinedModelSummary", data.ModelID).then(function(result) {
    //     if (result == null || result.success === false) {
    //         return;
    //     }
        
    //     // This section adds the External Analytic Attribute tiles
    //     var externalPanelWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
    //         widgetConfig,
    //         "externalLeadDetailsPanelWidget"
    //     );
        
    //     if (externalPanelWidgetConfig != null && data != null) {
    //         var externalAttributeContainer = $('.js-lead-details-external-container');
    //         WidgetFrameworkService.CreateWidget({
    //             element: externalAttributeContainer,
    //             widgetConfig: externalPanelWidgetConfig,
    //             metadata: metadata,
    //             data: data,
    //             parentData: data
    //         });
    //     }
        
    //     // This section adds the Internal Analytic Attribute tiles
    //     var internalPanelWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
    //         widgetConfig,
    //         "internalLeadDetailsPanelWidget"
    //     );
        
    //     if (internalPanelWidgetConfig != null && data != null) {
    //         var internalAttributeContainer = $('.js-lead-details-internal-container');
    //         WidgetFrameworkService.CreateWidget({
    //             element: internalAttributeContainer,
    //             widgetConfig: internalPanelWidgetConfig,
    //             metadata: metadata,
    //             data: data,
    //             parentData: data
    //         });
    //     }
    // });
});