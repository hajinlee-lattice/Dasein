angular.module('mainApp.plays.controllers.PlayDetailsController', [
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.utilities.WidgetEventConstantUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.URLUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'common.utilities.browserstorage',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.plays.controllers.NoPlaysController',
])
.controller('PlayDetailsController', function ($scope, $http, $rootScope, $compile, WidgetConfigUtility, WidgetEventConstantUtility, MetadataUtility,
    BrowserStorageUtility, WidgetFrameworkService, DanteWidgetService, NotionService, URLUtility, LpiPreviewStore) {

    var data = BrowserStorageUtility.getCurrentRootObject();
    var metadata = BrowserStorageUtility.getMetadata();
    var widgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
    var crmObject = BrowserStorageUtility.getSelectedCrmObject();
    var playList = null;
    var selectedId = $scope.playId;

    // Only show the nav section if there is an existing Play
    $scope.showPlayDetailsNav = false;

    // Setting default showTabs value
    $scope.showTabs = true;

    var repeaterWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "playDetailsTileRepeater"
    );

    if (repeaterWidgetConfig == null) {
        return;
    }

    // Find the Plays property definition on the TargetNotion
    var targetNotionProperty = MetadataUtility.GetNotionAssociationMetadata(
        repeaterWidgetConfig.Notion,
        repeaterWidgetConfig.TargetNotion,
        metadata);
    if (targetNotionProperty == null) {
        return;
    }

    // Need to find the ID of a Play in order to know which Play is selected.
    // This is configurable and needs to be fetched from the Widget Config and then the Metadata.
    var playDetailsTileWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "playDetailsTileWidget"
    );

    // Find the play notion metadata
    var playMetadata = MetadataUtility.GetNotionMetadata(playDetailsTileWidgetConfig.Notion, metadata);
    var idProperty = MetadataUtility.GetNotionProperty(playDetailsTileWidgetConfig.IdProperty, playMetadata);
    var crmIdProperty = MetadataUtility.GetNotionProperty(playDetailsTileWidgetConfig.CrmIdProperty, playMetadata);

    var createPlayDetailsView = function (playId) {
        var selectedPlay = null;

        for (var i = 0; i < playList.length; i++) {
            var play = playList[i];

            if (play[idProperty.Name] == playId ||
                play[crmIdProperty.Name] == playId ||
                (play.PlayID && play.PlayID == playId)) {
                    selectedPlay = playList[i];
                    break;
            }
        }

        if (selectedPlay == null) {
            selectedPlay = playList[0];
        }

        var headerWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
            widgetConfig,
            "playDetailsHeaderWidget"
        );

        if (headerWidgetConfig == null) {
            return;
        }

        var headerContainer = $('#playDetailsHeader');
        WidgetFrameworkService.CreateWidget({
            element: headerContainer,
            widgetConfig: headerWidgetConfig,
            metadata: metadata,
            data: selectedPlay
        });

        if (URLUtility.LpiPreview() === null) {
            fetchTalkingPoints(selectedPlay).then(function() {
                //return fetchModelSummary(selectedPlay.ModelID);
            }).then(function() {
                renderPlayDetails(selectedPlay);
            });
        } else {
            renderPlayDetails(selectedPlay);
        }
    };

    function fetchModelSummary (modelId) {
        return NotionService.findOne("FrontEndCombinedModelSummary", modelId);
    }

    function renderPlayDetails(selectedPlay) {
        var tabWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
            widgetConfig,
            "playDetailsTabWidget"
        );

        if (tabWidgetConfig == null) {
            return;
        }

        // Hide tabs if it is set in the custom settings
        var customSettings = BrowserStorageUtility.getCrmCustomSettings();
        $scope.showTabs = (customSettings && customSettings.HideTabs) ? false : true;

        // Check the CRM custom settings for Default Tab and if it is configured then they will take precedence
        if (customSettings != null && customSettings.DefaultTab != null) {
            var defaultTab = customSettings.DefaultTab.trim();
            switch (defaultTab.toUpperCase()) {
                case 'TALKINGPOINTS':
                    tabWidgetConfig.DefaultTab = 1;
                    break;
                case 'PURCHASEHISTORY':
                    tabWidgetConfig.DefaultTab = 2;
                    break;
                default:
                    tabWidgetConfig.DefaultTab = 0;
            }
        }

        // Override DefaultTab if custom setting for hiding the buying signal tab is turned on
        // and remove the tab
        if (customSettings != null && customSettings.HideBSTab === true) {
            for (var i = 0; i < tabWidgetConfig.Widgets.length; i++) {
                if (tabWidgetConfig.Widgets[i].ID === "playAttributesScreenWidget") {
                    tabWidgetConfig.Widgets.splice(i, 1);
                    //when a tab is removed, if default tab has index higher than removed index
                    //shift default index left by 1
                    if (tabWidgetConfig.DefaultTab > i) {
                        tabWidgetConfig.DefaultTab--;
                    }
                    break;
                }
            }
        }

        var contentContainer = $('#playDetailsContent');
        WidgetFrameworkService.CreateWidget({
            element: contentContainer,
            widgetConfig: tabWidgetConfig,
            metadata: metadata,
            data: selectedPlay,
            parentData: data
        });
    }

    function fetchTalkingPoints(selectedPlay) {
        return NotionService.findItemsByKey('DanteTalkingPoint', selectedPlay.PlayID, 'PlayID').then(function (result) {
            if (result !== null && result.length > 0) {
                selectedPlay.TalkingPoints = result.slice().sort(sortByTalkingPointOffset);
            }
        });
    }

    function sortByTalkingPointOffset(a, b) {
        if (a.Offset < b.Offset) {
            return -1;
        } else if (a.Offset > b.Offset) {
            return 1;
        } else {
            return 0;
        }
    }

    var recId = URLUtility.CrmRecommendation();
    if (crmObject.associationType === 'recommendation' && URLUtility.LpiPreview() === 'true') {
        var lead = LpiPreviewStore.getLead();
        playList = [lead];
        selectedPlay = playList[0];
        createPlayDetailsView(selectedPlay);
    } else if (crmObject.associationType === 'recommendation' && recId) {
        NotionService.findOneByKey('DanteLead', recId, "RecommendationID", 1).then(function (result) {
            playList = [result.resultObj];
            selectedPlay = playList[0].PlayID;
            createPlayDetailsView(selectedPlay);
        });
    } else {
        NotionService.findItemsByKey(repeaterWidgetConfig.TargetNotion, data[repeaterWidgetConfig.TargetNotionKey], repeaterWidgetConfig.TargetNotionKey).then(function(result) {
            if (result == null) {
                return;
            }
            playList = result;

            // If there are no Plays then just display a message
            if (playList == null || playList.length === 0) {
                $http.get('app/plays/views/NoPlaysView.html').success(function (html) {
                    var scope = $rootScope.$new();
                    scope.accountName = data.DisplayName;
                    var mainContentView = $("#mainContentView");
                    mainContentView.addClass("no-plays-background");
                    $compile(mainContentView.html(html))(scope);
                });
                return;
            } else {
                $scope.showPlayDetailsNav = true;
            }

            var playFound = false;
            var currentPlay;
            if (idProperty != null && idProperty.Name != null) {
                for (var i = 0; i < result.length; i++) {
                    currentPlay = result[i];
                    currentPlay.isSelected = (selectedId == currentPlay[idProperty.Name] || selectedId == currentPlay[crmIdProperty.Name]);
                    if (currentPlay.isSelected === true) {
                        playFound = true;
                        break;
                    }
                }
            }

            // Need to select first one here if it can't find associated lead
            if (playList.length > 0 && !playFound) {
                playList[0].isSelected = true;
            }
            data.Preleads = result;

            // ENG-7249 - Only show the Play that the SFDC lead represents if it is found for the current account
            if (playFound && $scope.isLatticeForLeadsContext) {
               data.Preleads = [currentPlay];
            }

            var container = $('#playDetailsNav');
            WidgetFrameworkService.CreateWidget({
                element: container,
                widgetConfig: repeaterWidgetConfig,
                metadata: metadata,
                data: data,
                parentData: data
            });

            createPlayDetailsView(selectedId);
        });
    }

    $scope.$on(WidgetEventConstantUtility.PLAY_DETAILS_TAB_CLICKED, function(event, playId) {
        createPlayDetailsView(playId);
    });
});
