angular.module('test.testData.WidgetConfigTestDataService', [
    'mainApp.appCommon.utilities.WidgetConfigUtility'])
.service('WidgetConfigTestDataService', function (WidgetConfigUtility) {

    this.GetSampleWidgetConfig = function () {
        return {
            ID: "LatticeForAccounts",
            Type: "ApplicationWidget",
            Version: "1",
            Widgets: [
                {
                    ID: "playDetailsWidget",
                    Type: "ScreenWidget",
                    Widgets: [
                        {
                            ID: "talkingPointRepeater",
                            Type: WidgetConfigUtility.REPEATER_WIDGET,
                            Notion: "DanteLead",
                            TargetNotion: "DanteTalkingPoint",
                            Widgets: [
                                {
                                    ID: "talkingPointPanelWidget",
                                    Type: WidgetConfigUtility.COLLAPSIBLE_PANEL_WIDGET,
                                    TitleProperty: "Account.PreLead.TalkingPoint.Title",
                                    Widgets: [
                                        {
                                            ID: "talkingPointRichTextWidget",
                                            Type: WidgetConfigUtility.RICH_TEXT_WIDGET,
                                            Row: 0,
                                            Column: 0
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        };
    }; // end GetSampleWidgetConfig
    
});