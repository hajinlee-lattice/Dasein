angular.module('mainApp.appCommon.services.PlayTileService', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.appCommon.utilities.NumberUtility'
])
.service('PlayTileService', function (ResourceUtility, MetadataUtility, DateTimeFormatUtility, NumberUtility) {
    
    this.GetTileData = function (data, widgetConfig, metadata, successCallback) {
        if (data == null || widgetConfig == null || metadata == null) {
            return {};
        }
        
        var tileData = {
            ID: null,
            Name: "",
            Lift: null,
            LiftDisplayKey: null,
            ActiveDays: null,
            ActiveDaysDisplayKey: null,
            Revenue: null,
            RevenueDisplayKey: null,
            Objective: "",
            PlayType: "",
            IsActive: true,
            CrmId: null
        };

        // Find the appropriate notion metadata
        var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);
        
        // Find ID from the metadata
        var leadIdProperty = MetadataUtility.GetNotionProperty(widgetConfig.IdProperty, notionMetadata);
        if (leadIdProperty != null) {
            tileData.ID = data[leadIdProperty.Name] || null;
        }
        // Find Play Name from the metadata
        var nameProperty = MetadataUtility.GetNotionProperty(widgetConfig.NameProperty, notionMetadata);
        if (nameProperty != null && nameProperty.PropertyTypeString == MetadataUtility.PropertyType.STRING) {
            tileData.Name = data[nameProperty.Name] || "";
        }

        // Find Lift from the metadata
        var liftProperty = MetadataUtility.GetNotionProperty(widgetConfig.LiftProperty, notionMetadata);
        if (liftProperty != null && liftProperty.PropertyTypeString == MetadataUtility.PropertyType.DOUBLE) {
            tileData.Lift = data[liftProperty.Name] || null;
            if (tileData.Lift != null) {
                tileData.Lift = tileData.Lift.toPrecision(2);
                tileData.Lift += "x";
                tileData.LiftDisplayKey = ResourceUtility.getString(liftProperty.DisplayNameKey);
            }
        }

        // Find LaunchDate from the metadata
        var launchDateProperty = MetadataUtility.GetNotionProperty(widgetConfig.LaunchDateProperty, notionMetadata);
        if (launchDateProperty != null) {
            tileData.ActiveDaysDisplayKey = ResourceUtility.getString(launchDateProperty.DisplayNameKey);
            var launchDate = data[launchDateProperty.Name] || null;
            if (launchDate != null) {
                var dateObj;
                if (launchDateProperty.PropertyTypeString == MetadataUtility.PropertyType.DATE_TIME_OFFSET) {
                    dateObj = DateTimeFormatUtility.ConvertCSharpDateTimeOffsetToJSDate(launchDate);
                    tileData.ActiveDays = DateTimeFormatUtility.CalculateDaysBetweenDates(dateObj, new Date());
                } else if (launchDateProperty.PropertyTypeString == MetadataUtility.PropertyType.EPOCH_TIME) {
                    var epochTime = parseInt(launchDate) * 1000;
                    dateObj = new Date();
                    dateObj.setTime(epochTime);
                    tileData.ActiveDays = DateTimeFormatUtility.CalculateDaysBetweenDates(dateObj, new Date());
                }
            }
        }
        
        // Never show 0 for Days Active
        if (tileData.ActiveDays === 0) {
            tileData.ActiveDays = 1;
        }

        // Find Revenue from the metadata
        var revenueProperty = MetadataUtility.GetNotionProperty(widgetConfig.RevenueProperty, notionMetadata);
        if (revenueProperty != null && revenueProperty.PropertyTypeString == MetadataUtility.PropertyType.CURRENCY) {
            var revenueInt = data[revenueProperty.Name] || null;
            if (revenueInt != null) {
                tileData.Revenue = NumberUtility.AbbreviateLargeNumber(revenueInt);
                tileData.RevenueDisplayKey = ResourceUtility.getString(revenueProperty.DisplayNameKey);
            }
        }

        // Find the Objective from the metadata
        var objectiveProperty = MetadataUtility.GetNotionProperty(widgetConfig.ObjectiveProperty, notionMetadata);
        if (objectiveProperty != null && objectiveProperty.PropertyTypeString == MetadataUtility.PropertyType.STRING) {
            tileData.Objective = data[objectiveProperty.Name] || "";
            if (tileData.Objective.length > 140) {
                tileData.Objective = this.Objective.substring(0, 140) + "...";
            }
        }

        // Find the Play Type icon
        var playTypeProperty = MetadataUtility.GetNotionProperty(widgetConfig.PlayTypeProperty, notionMetadata);
        if (playTypeProperty != null) {
            tileData.PlayType = data[playTypeProperty.Name] || null;
        }
        
        // Check if the CRM Lead is active in the CRM system
        var salesforceIdProperty = MetadataUtility.GetNotionProperty(widgetConfig.CrmIdProperty, notionMetadata);
        if (salesforceIdProperty != null) {
            tileData.CrmId = data[salesforceIdProperty.Name];
            if (tileData.CrmId == null) {
                if (successCallback != null && typeof successCallback === "function") {
                    successCallback(tileData);
                }
            } else {
                var isActiveEvent = "IsLeadActiveEvent=" + tileData.CrmId;
                window.parent.postMessage(isActiveEvent, "*");
                
                
                window.addEventListener('message', function (evt) {
                    if (evt && evt.data.indexOf("IsLeadActiveReturnEvent="+tileData.CrmId) !== -1) {
                        var questionMarkIndex = evt.data.indexOf("?");
                        var isActiveQueryString = evt.data.substring(questionMarkIndex + 1, evt.data.length);
                        var separatorIndex = isActiveQueryString.indexOf("=");
                        var isActiveString = isActiveQueryString.substring(separatorIndex + 1, isActiveQueryString.length);
                        tileData.IsActive = isActiveString === "true" ? true : false;
                        
                        if (successCallback != null && typeof successCallback === "function") {
                            successCallback(tileData);
                        }
                    }
                });
            }
        } else {
            if (successCallback != null && typeof successCallback === "function") {
                successCallback(tileData);
            }
        } 
    };
});