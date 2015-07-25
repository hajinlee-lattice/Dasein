angular.module('mainApp.appCommon.services.WidgetFrameworkService', [
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.widgets.RepeaterWidget',
    'mainApp.appCommon.widgets.ScreenWidget',
    'mainApp.appCommon.widgets.ArcChartWidget',
    'mainApp.appCommon.widgets.CollapsiblePanelWidget',
    'mainApp.appCommon.widgets.ScreenHeaderWidget',
    'mainApp.appCommon.widgets.SimpleGridWidget',
    'mainApp.appCommon.widgets.SimpleTabWidget',
    'mainApp.appCommon.widgets.TabWidget',
    'mainApp.appCommon.widgets.TopPredictorWidget',
    'mainApp.appCommon.widgets.ThresholdExplorerWidget',
    'mainApp.appCommon.widgets.UserManagementWidget',
    'mainApp.appCommon.widgets.AdminInfoSummaryWidget',
    'mainApp.appCommon.widgets.AdminInfoAlertsWidget',
    'mainApp.appCommon.widgets.LeadsTabWidget',
    'mainApp.appCommon.widgets.ModelListCreationHistoryWidget'
])
.service('WidgetFrameworkService', function ($compile, $rootScope, WidgetConfigUtility, MetadataUtility) {

    this.CreateWidget = function (options) {
        if (options == null || options.element == null || options.widgetConfig == null) {
            return;
        }


        var root = $rootScope;
        var scope = root.$new();
        scope.data = options.data;
        scope.parentData = options.parentData;
        scope.metadata = options.metadata;
        scope.widgetConfig = options.widgetConfig;
        scope.element = options.element;
        scope.isActive = options.isActive;

        // alphabetical order please
        switch (options.widgetConfig.Type) {
            case WidgetConfigUtility.ANALYTIC_ATTRIBUTE_LIST_WIDGET:
                $compile(options.element.html('<div data-analytic-attribute-list-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.ANALYTIC_ATTRIBUTE_TILE_WIDGET:
                $compile(options.element.html('<div data-analytic-attribute-tile-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.ARC_CHART_WIDGET:
                $compile(options.element.html('<div data-arc-chart-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.COLLAPSIBLE_PANEL_WIDGET:
                $compile(options.element.html('<div data-collapsible-panel-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.GRID_WIDGET:
                //TODO:pierce options.$element.grid_widget(widgetOptions);
                break;
            case WidgetConfigUtility.LEAD_DETAILS_TILE_WIDGET:
                $compile(options.element.html('<div data-lead-details-tile-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.MODEL_DETAILS_WIDGET:
                $compile(options.element.html('<div data-model-details-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.MODEL_LIST_TILE_WIDGET:
                $compile(options.element.html('<div data-model-list-tile-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.MODEL_LIST_CREATION_HISTORY:
                $compile(options.element.html('<div data-model-list-creation-history-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.PLAY_DETAILS_TILE_WIDGET:
                $compile(options.element.html('<div data-play-details-tile-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.PLAY_LIST_TILE_WIDGET:
                $compile(options.element.html('<div data-play-list-tile-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.REPEATER_WIDGET:
                $compile(options.element.html('<div data-repeater-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.SCREEN_WIDGET:
                $compile(options.element.html('<div data-screen-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.SCREEN_HEADER_WIDGET:
                $compile(options.element.html('<div data-screen-header-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.SIMPLE_GRID_WIDGET:
                $compile(options.element.html('<div data-simple-grid-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.SIMPLE_TAB_WIDGET:
                $compile(options.element.html('<div data-simple-tab-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.TALKING_POINT_WIDGET:
                $compile(options.element.html('<div data-talking-point-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.TAB_WIDGET:
                $compile(options.element.html('<div data-tab-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.TEST_WIDGET:
                //TODO:pierce options.$element.test_widget(widgetOptions);
                break;
            case WidgetConfigUtility.THRESHOLD_EXPLORER_WIDGET:
                $compile(options.element.html('<div data-threshold-explorer-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.TOP_PREDICTOR_WIDGET:
                $compile(options.element.html('<div data-top-predictor-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.ADMIN_INFO_SUMMARY_WIDGET:
                $compile(options.element.html('<div data-admin-info-summary-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.ADMIN_INFO_ALERTS_WIDGET:
                $compile(options.element.html('<div data-admin-info-alerts-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.LEADS_TAB_WIDGET:
                $compile(options.element.html('<div data-leads-tab-widget></div>'))(scope);
                break;
            case WidgetConfigUtility.USER_MANAGEMENT_WIDGET:
                $compile(options.element.html('<div data-user-management-widget></div>'))(scope);
                break;
            default:
                return;
        }
    };

    this.CreateChildWidgets = function (options, dataForChildren) {
        var numLayoutColumns = options.widgetConfig.Columns || 1;
        this.CreateChildWidgetsInColumnLayout(options, dataForChildren, numLayoutColumns);
    };

    this.CreateChildWidgetsInColumnLayout = function (options, dataForChildren, numLayoutColumns) {
        var self = this;

        if (options == null || options.widgetConfig == null) {
            return;
        }

        var children = options.widgetConfig.Widgets;
        if (children == null) {
            return;
        }

        self.CreateWidgetsInColumnLayout(
            options.element,
            children.length,
            numLayoutColumns,
            function (childElement, numItems, childIndex) {
                self.CreateChildWidget(
                    options, childElement, children, dataForChildren, numItems, childIndex);
            }
        );
    };

    this.CreateWidgetsInColumnLayout = function (element, numItems, numCols, createWidgetCallback) {
        // Only create the extra column layout HTML for multi-column layouts
        var isMultiCol = ((numCols !== 1) && (typeof numCols === "number"));

        var colClass = 'layout-column';

        if (isMultiCol) {
            this.CreateLayoutColumnHtml(element, numCols, colClass);
        }

        var numItemsPerCol = Math.floor(numItems / numCols);

        // The number of left over items (when items do not divide evenly into columns),
        // we will spread these out evenly across columns
        var leftoverItems = numItems % numCols;

        var curColNum = 0;
        var curColItemCount = 0;

        for (var i = 0; i < numItems; i++) {
            var childElement = $('<div></div>');

            if (isMultiCol) {
                // Figure out if we should move to the next column
                if (curColItemCount >= numItemsPerCol) {
                    // current column is full but can fit a leftover
                    if (leftoverItems > 0 && (curColItemCount == numItemsPerCol)) {
                        leftoverItems--;
                    } else { // current column has filled up (including leftovers), move to next
                        curColNum++;
                        curColItemCount = 0;
                    }
                }
                curColItemCount++;
                this.AddItemToColumn(element, colClass, curColNum, childElement);
            } else {
                this.AddItemToElement(element, childElement);
            }

            if (typeof createWidgetCallback === "function") {
                createWidgetCallback(childElement, numItems, i);
            }
        }
    };

    this.CreateLayoutColumnHtml = function (element, numCols, colClass) {
        var colWidth = 100 / numCols;
        for (var col = 0; col < numCols; col++) {
            var cols = $('<div class="float-left ' + colClass + ' ' + colClass + '-' + col + '"></div>');
            $(element).append(cols);
            cols.css("width", colWidth + "%"); // must add percent sign otherwise will be pixels
        }
    };

    this.AddItemToColumn = function (element, colClass, curColNum, item) {
        var curCol = $(element).find('.' + colClass + '-' + curColNum);
        $(curCol).append(item);
    };

    this.AddItemToElement = function (element, item) {
        $(element).append(item);
    };

    this.CreateChildWidget = function (options, childElement, children, dataForChildren, numChildren, index) {
        var child = children[index];
        var childData = this.GetChildDataAtIndex(dataForChildren, numChildren, index);
        var childIsActive = options.isActive;

        this.CreateWidget({
            scope: options.scope,
            element: childElement,
            widgetConfig: child,
            metadata: options.metadata,
            data: childData,
            parentData: options.parentData,
            isActive: childIsActive
        });
    };

    this.IsChildActive = function (activeWidgetStates, childIndex, numChildren) {
        if (!Array.isArray(activeWidgetStates)) {
            return true;
        }

        var isAllActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_ALL) !== -1),
            isEvenActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_EVEN) !== -1),
            isFirstActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_FIRST) !== -1),
            isLastActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_LAST) !== -1),
            isNoneActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_NONE) !== -1),
            isOddActive = (activeWidgetStates.indexOf(WidgetConfigUtility.ACTIVE_WIDGET_ODD) !== -1),
            isChildActive = false;

        // Note: All takes precedence over None,
        // so ["All", "None"] and ["None", "All"] both evaluate to "All" 
        if (isAllActive) {
            isChildActive = true;
        } else if (isNoneActive) {
            isChildActive = false;
        } else if (isEvenActive || isFirstActive || isLastActive || isOddActive) {
            // values that can be combined
            if (isEvenActive) {
                isChildActive = isChildActive || ((childIndex + 1) % 2 === 0);
            }
            if (isFirstActive) {
                isChildActive = isChildActive || (childIndex === 0);
            }
            if (isLastActive) {
                isChildActive = isChildActive || (childIndex === numChildren - 1);
            }
            if (isOddActive) {
                isChildActive = isChildActive || ((childIndex + 1) % 2 !== 0);
            }
        } else { // no valid active widget states specified
            isChildActive = true;
        }

        return isChildActive;
    };

    this.GetChildDataAtIndex = function (dataForChildren, numChildren, index) {
        var childData = null;

        // if data for children is an array, use the correct index
        if (dataForChildren instanceof Array &&
            dataForChildren.length === numChildren) {
            childData = dataForChildren[index];
        } else { // otherwise data applies for all children
            childData = dataForChildren;
        }

        return childData;
    };

    // Get a list of properties to sort on (in the form that SortUtil expects)
    // given the Sorts specified in a WidgetConfig
    this.GetSortProperties = function (widgetConfig, metadata) {
        if (widgetConfig == null) {
            return [];
        }

        var widgetConfigSorts = widgetConfig.Sorts;
        if (!(widgetConfigSorts instanceof Array)) {
            return [];
        }

        if (metadata == null) {
            return [];
        }

        var targetNotionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.TargetNotion, metadata);

        var propsToSortOn = [];
        for (var i = 0; i < widgetConfigSorts.length; i++) {
            var widgetConfigSort = widgetConfigSorts[i];
            if (widgetConfigSort == null) {
                continue;
            }

            var propName = widgetConfigSort.Property;
            var propIsAscending = widgetConfigSort.IsAscending;

            var metadataProperty = MetadataUtility.GetNotionProperty(
                propName, targetNotionMetadata);
            var propTypeString = (metadataProperty != null) ? metadataProperty.PropertyTypeString : null;
            var propCompareFunction = MetadataUtility.GetCompareFunction(propTypeString);

            var prop = {
                Name: propName,
                IsAscending: propIsAscending,
                CompareFunction: propCompareFunction
            };
            propsToSortOn.push(prop);
        }

        return propsToSortOn;
    };
});