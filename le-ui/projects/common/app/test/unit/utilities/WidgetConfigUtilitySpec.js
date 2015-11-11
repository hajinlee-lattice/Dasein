'use strict';

describe('WidgetConfigUtility Tests', function () {
    var widgetUtil,
        rootConfig,
        richTextWidgetConfig = {
            ID: "talkingPointRichTextWidget",
            Type: "RichTextWidget",
            Row: 0,
            Column: 0
        },
        tp1PanelWidgetConfig = {
            ID: "talkingPointPanelWidget",
            Type: "CollapsiblePanelWidget",
            TitleProperty: "Account.PreLead.TalkingPoint.Title",
            Widgets: [richTextWidgetConfig]
        };
    
    beforeEach(function () {
        module('mainApp.appCommon.utilities.WidgetConfigUtility');
        module('test.testData.WidgetConfigTestDataService');
        
        inject(['WidgetConfigUtility', 'WidgetConfigTestDataService',
            function (WidgetConfigUtility, WidgetConfigTestDataService) {
                widgetUtil = WidgetConfigUtility;
                rootConfig = WidgetConfigTestDataService.GetSampleWidgetConfig();
            }
        ]);
    });

    describe('GetWidgetConfig given invalid parameters', function () {
        it('should return null if given a null application widget config', function () {
            var config = widgetUtil.GetWidgetConfig(null, "playDetailsWidget1");
            expect(config).toEqual(null);
        });

        it('should return null if given a null ID', function () {
            var config = widgetUtil.GetWidgetConfig(rootConfig, null);
            expect(config).toEqual(null);
        });
        
        it('should return null if given an ID of an non-existent widget', function () {
            var config = widgetUtil.GetWidgetConfig(rootConfig, "doesNotExist");
            expect(config).toEqual(null);
        });
    });
    
    describe('GetWidgetConfig given valid parameters', function () {
        it('should return the correct widget config when given the ID of a valid widget with no children', function () {
            var expConfig = richTextWidgetConfig;
            var config = widgetUtil.GetWidgetConfig(rootConfig, "talkingPointRichTextWidget");
            expect(config).toEqual(expConfig);
        });
        
        it('should return the correct widget config when given the ID of a valid widget with children', function () {
            var expConfig = tp1PanelWidgetConfig;
            var config = widgetUtil.GetWidgetConfig(rootConfig, "talkingPointPanelWidget");
            expect(config).toEqual(expConfig);
        });
        
        it('should return the correct widget config when given the ID of the root widget config', function () {
            var expConfig = getAccountRootWidgetConfig();
            var config = widgetUtil.GetWidgetConfig(rootConfig, "LatticeForAccounts");
            expect(config).toEqual(expConfig);
        });
    });
    
    function getAccountRootWidgetConfig() {
        var accountRootWidget = {
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
                            Type: widgetUtil.REPEATER_WIDGET,
                            Notion: "DanteLead",
                            TargetNotion: "DanteTalkingPoint",
                            Widgets: [
                                {
                                    ID: "talkingPointPanelWidget",
                                    Type: widgetUtil.COLLAPSIBLE_PANEL_WIDGET,
                                    TitleProperty: "Account.PreLead.TalkingPoint.Title",
                                    Widgets: [
                                        {
                                            ID: "talkingPointRichTextWidget",
                                            Type: widgetUtil.RICH_TEXT_WIDGET,
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
        return accountRootWidget;
    }
});
