angular.module('mainApp.plays.services.PlayService', [
    'mainApp.appCommon.utilities.MetadataUtility',
    'common.utilities.browserstorage',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService'
])
.service('PlayService', function ($http, $q, MetadataUtility, BrowserStorageUtility, DanteWidgetService, NotionService) {
    
    this.FindCrmNotion = function (selectedCrmObject, successCallback, failCallback) {
        if (selectedCrmObject == null || selectedCrmObject.ID == null) {
            return null;
        }
        
        var rootObject = BrowserStorageUtility.getCurrentRootObject();
        var applicationWidgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
        if (applicationWidgetConfig == null && applicationWidgetConfig.Notion == null) {
            if (failCallback != null && typeof failCallback === "function") {
                failCallback();
                return;
            }
        }
        var allMetadata = BrowserStorageUtility.getMetadata();
        var rootObjectMetadata = MetadataUtility.GetNotionMetadata(applicationWidgetConfig.Notion, allMetadata);
        if (rootObjectMetadata == null) {
            if (failCallback != null && typeof failCallback === "function") {
                failCallback();
                return;
            }
        }
        
        var targetNotionAssociationMetadata = MetadataUtility.GetNotionAssociationMetadata(rootObjectMetadata.Name, selectedCrmObject.Notion, allMetadata);
        if (targetNotionAssociationMetadata == null) {
            if (failCallback != null && typeof failCallback === "function") {
                failCallback();
                return;
            }
        }

        var notionList = [];
        NotionService.findItemsByKey(targetNotionAssociationMetadata.TargetNotion, rootObject.SalesforceAccountID, "SalesforceAccountID").then(function(result) {
            if (result == null) {
                return;
            }
            
            var setNotion = function(key, value) {
                if (value == selectedCrmObject.ID) {
                    foundNotion = notion;
                    return false;
                }
            };
            
            notionList = result;
            var foundNotion = null;
            for (var i = 0; i < notionList.length; i++) {
                var notion = notionList[i];
                $.each(notion, setNotion);
            }
            
            if (foundNotion != null) {
                if (successCallback != null && typeof successCallback === "function") {
                    successCallback(foundNotion);
                    return;
                }
            } else {
                if (failCallback != null && typeof failCallback === "function") {
                    failCallback();
                    return;
                }
            }
        });
    };
});