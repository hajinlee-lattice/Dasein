angular.module('templates-main', ['app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html', 'app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html', 'app/AppCommon/widgets/arcChartWidget/ArcChartWidgetTemplate.html', 'app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidgetTemplate.html', 'app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html', 'app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html', 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html', 'app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidgetTemplate.html', 'app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidgetTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryNavTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySegmentTableTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySpendTableTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryTimelineTemplate.html', 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryYearTableTemplate.html', 'app/AppCommon/widgets/repeaterWidget/RepeaterWidgetTemplate.html', 'app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidgetTemplate.html', 'app/AppCommon/widgets/screenWidget/ScreenWidgetTemplate.html', 'app/AppCommon/widgets/tabWidget/TabWidgetTemplate.html', 'app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html', 'app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html', 'app/core/views/InitialView.html', 'app/core/views/MainHeaderView.html', 'app/core/views/MainView.html', 'app/core/views/NoAssociationView.html', 'app/core/views/NoNotionView.html', 'app/core/views/ServiceErrorView.html', 'app/leads/views/LeadDetailsView.html', 'app/plays/views/NoPlaysView.html', 'app/plays/views/PlayDetailsView.html', 'app/plays/views/PlayListView.html', 'app/purchaseHistory/views/PurchaseHistoryView.html']);

angular.module("app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html",
    "<div data-ng-controller=\"AnalyticAttributeListWidgetController\"> <div data-ng-show=\"!hasAttributes\"> <div class=\"empty-analytic-list-container\"> <span class=\"empty-analytic-list-description\"> {{ResourceUtility.getString('DANTE_LEAD_NO_INSIGHTS_DESCRIPTION')}}&nbsp; <a class=\"tooltip fa fa-info-circle\" href=\"#\" title=\"{{learnMoreTooltip}}\" data-ng-click=\"learnMoreClicked($event)\"> </a> </span> </div> </div> <div class=\"js-analytic-attribute-list analytic-attribute-list\"> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html",
    "<div class=\"analytic-attribute-tile {{aboveAverageLift ? 'attr-lift-positive': 'attr-lift-negative'}}\" data-ng-controller=\"AnalyticAttributeTileWidgetController\"> <img class=\"analytic-attribute-tile-icon\" src=\"assets/images/{{icon}}.png\"> <div class=\"analytic-attribute-tile-position\"> <div class=\"float-right\" data-ng-show=\"data.Lift\"> <div class=\"lift-container\" data-ng-show=\"showLift\"> <div class=\"lift-super-label\">CONVERTS</div> <div class=\"lift\">{{data.Lift}}</div> <div class=\"lift-sub-label\">{{ResourceUtility.getString('LIFT_SUB_LABEL')}}</div> </div> <div data-ng-show=\"!showLift\"> <div class=\"lift-indicator-above\" data-ng-show=\"aboveAverageLift\"> <div class=\"lift-image fa fa-caret-up\"></div> <div class=\"lift-image-description\">{{ResourceUtility.getString('LIFT_ABOVE_AVERAGE')}}</div> </div> <div class=\"lift-indicator-below\" data-ng-show=\"!aboveAverageLift\"> <div class=\"lift-image fa fa-caret-down\"></div> <div class=\"lift-image-description\">{{ResourceUtility.getString('LIFT_BELOW_AVERAGE')}}</div> </div> </div> </div> <div class=\"analytic-attribute-heading\">{{data.DisplayName}}</div> <div class=\"analytic-attribute-description\" data-ng-show=\"data.Description\"> {{data.Description}} </div> <div class=\"analytic-attribute-title\">{{data.BucketName}}</div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/arcChartWidget/ArcChartWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/arcChartWidget/ArcChartWidgetTemplate.html",
    "<div data-ng-controller=\"ArcChartWidgetController\"> <div data-arc-chart data-chart-size=\"{{ChartSize}}\" data-chart-value=\"{{ChartValue}}\" data-chart-total=\"{{ChartTotal}}\" data-chart-color=\"{{ChartColor}}\" data-chart-label=\"{{ChartLabel}}\"></div> </div>");
}]);

angular.module("app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidgetTemplate.html",
    "<div data-ng-controller=\"CollapsiblePanelWidgetController\"> <div class=\"collapsible-panel-container\"> <div class=\"collapsible-panel-header\" data-ng-click=\"panelClicked()\"> <span class=\"collapsible-panel-title-container\">{{panelTitle}}</span> <span class=\"collapsible-panel-button float-left fa {{panelOpen ? 'fa-chevron-up' : 'fa-chevron-down'}}\"></span> </div> <div class=\"js-collapsible-panel-content collapsible-panel-content\"></div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html",
    "<div data-ng-controller=\"LeadDetailsTileWidgetController\" class=\"dante-play-details-nav-tile dante-play-details-nav-tile-selected dante-lead-details-tile\" ng-style=\"style()\"> <div class=\"dante-play-tile-header\"> {{header}} </div> <div class=\"js-dante-lead-tile-score dante-lead-details-tile-score\"> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html",
    "<div class=\"col-md-12\" data-ng-controller=\"ModelDetailsWidgetController\"> <section class=\"panel\"> <div class=\"panel-body\"> <div class=\"model-details-header\"> <div class=\"mdh-lt\"> <div class=\"panel-model-scorerating\"> <div class=\"model-score-circ\"> <span>{{score}}</span> </div> <div class=\"model-rating\"><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star\"></span> </div> </div> </div> <div class=\"mdh-mid\"> <h1 class=\"h1-h2\"><a href=\"#\" id=\"editable-model-name\" data-type=\"text\" data-placement=\"right\" data-title=\"Enter Model Name\" class=\"editable editable-click\" style=\"display:inline\">{{displayName}}</a></h1> <div class=\"mdh-datapoints\" id=\"base-datapoints\"> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_STATUS_LABEL')}}</span> <span class=\"title-value\"><span class=\"fa fa-check-circle active\"></span> Active</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_EXTERNAL_LABEL')}}</span> <span class=\"title-value\">{{externalAttributes}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_INTERNAL_LABEL')}}</span> <span class=\"title-value\">{{internalAttributes}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_CREATED_DATE_LABEL')}}</span> <span class=\"title-value\">{{createdDate}}</span> </div> </div> <div id=\"link-showmore\"><a href=\"\" class=\"btn\"><i class=\"fa fa-angle-down\"></i> {{ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON')}} <i class=\"fa fa-angle-down\"></i></a></div> <div class=\"mdh-datapoints sub\" id=\"more-datapoints\" style=\"display:block\"> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TOTAL_LEADS_LABEL')}}</span> <span class=\"title-value\">{{totalLeads}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TEST_SET_LABEL')}}</span> <span class=\"title-value\">{{testSet}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TRAINING_SET_LABEL')}}</span> <span class=\"title-value\">{{trainingSet}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_SUCCESS_EVENTS_LABEL')}}</span> <span class=\"title-value\">{{totalSuccessEvents}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_CONVERSION_RATE_LABEL')}}</span> <span class=\"title-value\">{{conversionRate}}%</span> </div> <div class=\"mdh-datapoints sub\" id=\"more-datapoints-2\"> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_LEAD_SOURCE_LABEL')}}</span> <span class=\"title-value\">{{leadSource}}</span> </div> <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_OPPORTUNITY_LABEL')}}</span> <span class=\"title-value\">{{opportunity}}</span> </div> </div> </div> </div> <div class=\"mdh-rt\"> <div class=\"btn-group\"> <a class=\"btn btn-primary\" data-toggle=\"modal\" href=\"#score-modal\">{{ResourceUtility.getString('BUTTON_ACTIVATE_LABEL')}}</a> </div> <div class=\"btn-group\"> <a class=\"btn btn-secondary\" data-toggle=\"modal\" href=\"#delete-modal\"><i class=\"fa fa-trash-o\"></i></a> </div> </div> </div> </div> </section> </div>");
}]);

angular.module("app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html",
    "<div class=\"col-md-4 model-list-tile-widget\" data-ng-controller=\"ModelListTileWidgetController\" data-ng-click=\"tileClick()\"> <section class=\"panel panel-model-fancy\" id=\"panel-model-1\"> <div class=\"fancy-header model-bg-1\"> <div class=\"model-header-txt\"><span class=\"fa fa-check-circle\"></span> {{displayName}}</div> <div class=\"panel-model-scorerating\"> <div class=\"model-score-circ\"> <span>{{score}}</span> </div> <div class=\"model-rating\"><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star\"></span> </div> </div> <ul class=\"tile-datapoints-primary\"> <li> <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_STATUS_LABEL')}}</span> <span class=\"tile-value\">{{status}}</span> </li> </ul> </div> <div class=\"tile-datapoints\"> <ul> <li> <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_EXTERNAL_LABEL')}}</span> <span class=\"tile-value\">{{externalAttributes}}</span> </li> <li> <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_INTERNAL_LABEL')}}</span> <span class=\"tile-value\">{{internalAttributes}}</span> </li> <li> <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_CREATED_DATE_LABEL')}}</span> <span class=\"tile-value\">{{createdDate}}</span> </li> </ul> </div> </section> </div>");
}]);

angular.module("app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidgetTemplate.html",
    "<div data-ng-controller=\"PlayDetailsTileWidgetController\" class=\"dante-play-details-nav-tile {{isSelected ? 'dante-play-details-nav-tile-selected' : ''}}\" data-ng-show=\"tileData.IsActive\" data-ng-click=\"playNameClicked()\"> <div class=\"dante-play-tile-header\"> <span> </span> <div class=\"icon pt-{{tileData.PlayType}} play-type-icon\"></div> {{tileData.Name}} </div> <div class=\"js-dante-play-details-nav-tile-score dante-play-details-nav-tile-score float-left\" data-ng-show=\"showScore\"> </div> <div class=\"dante-play-details-nav-tile-selected-arrow float-right\" data-ng-show=\"isSelected\"></div> <div class=\"float-right\"> <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.Lift\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.Lift}} </p> <p class=\"dante-play-tile-metric-label\"> {{tileData.LiftDisplayKey}} </p> </div> </div> <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.ActiveDays\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.ActiveDays}} </p> <p class=\"dante-play-tile-metric-label\"> {{tileData.ActiveDaysDisplayKey}} </p> </div> </div> <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.Revenue\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.Revenue}} </p> <p class=\"dante-play-tile-metric-label\"> {{tileData.RevenueDisplayKey}} </p> </div> </div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html",
    "<section data-ng-controller=\"PlayListTileWidgetController\" class=\"tile-flip-container dante-play-tile-container\" data-ng-click=\"playNameClicked()\"> <div class=\"tile-flipper\"> <div class=\"dante-play-tile\"> <div class=\"dante-play-tile-content\"> <div class=\"dante-play-tile-header\"> <span> </span> <div class=\"icon pt-{{tileData.PlayType}} play-type-icon\" data-ng-show=\"tileData.PlayType\"></div> {{tileData.Name}} </div> <div class=\"js-dante-play-tile-score dante-play-tile-score\" data-ng-class=\"noScoreClass\"> </div> <div> <div class=\"dante-play-tile-metric-container\" data-ng-show=\"showLift && tileData.Lift\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.Lift}} </p> <p class=\"dante-play-tile-metric-label uppercase\"> {{tileData.LiftDisplayKey}} </p> </div> </div> <div class=\"dante-play-tile-metric-container\" data-ng-show=\"tileData.ActiveDays\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.ActiveDays}} </p> <p class=\"dante-play-tile-metric-label uppercase\"> {{tileData.ActiveDaysDisplayKey}} </p> </div> </div> <div class=\"dante-play-tile-metric-container\" data-ng-show=\"tileData.Revenue\"> <div class=\"dante-play-tile-metric\"> <p class=\"dante-play-tile-metric-value\"> {{tileData.Revenue}} </p> <p class=\"dante-play-tile-metric-label uppercase\"> {{tileData.RevenueDisplayKey}} </p> </div> </div> </div> <p class=\"dante-play-tile-description\"> {{tileData.Objective}} </p> </div> </div> </div> </section>");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidgetTemplate.html",
    "<div data-ng-controller=\"PurchaseHistoryWidgetController\" class=\"purchase-history\"> <div class=\"initial-loading-spinner-container\" style=\"height:250px\"> <div style=\"color:#a0cadb;display:inline\" class=\"la-line-scale-party la-dark la-2x\"> <div></div> <div></div> <div></div> <div></div> <div></div> </div> <div style=\"color:#a0cadb;display:inline\" class=\"la-line-scale-party la-dark la-2x\"> <div></div> <div></div> <div></div> <div></div> <div></div> </div> </div> <div id=\"purchaseHistoryHeader\"></div> <div id=\"purchaseHistoryMain\"></div> </div> ");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryNavTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryNavTemplate.html",
    "<div ng-controller=\"PurchaseHistoryNavCtrl as vm\"> <div class=\"ph-filter\"> <span class=\"ph-filter-group\"> Date Range: <div class=\"datepicker\" dropdown-select=\"periodIdStartOptions\" dropdown-model=\"periodIdStartSelected\" dropdown-onchange=\"vm.selectStartPeriodId(selected)\"></div> to <div class=\"datepicker\" dropdown-select=\"periodIdEndOptions\" dropdown-model=\"periodIdEndSelected\" dropdown-onchange=\"vm.selectEndPeriodId(selected)\"></div> </span> <span class=\"ph-filter-group\"> Show: <div dropdown-select=\"periodOptions\" dropdown-model=\"periodSelected\" dropdown-onchange=\"vm.selectPeriod(selected)\"></div> </span> <span class=\"ph-filter-group\"> Show: <div dropdown-select=\"filterOptions\" dropdown-model=\"filterSelected\" dropdown-onchange=\"vm.selectFilter(selected)\"></div> </span> </div> <div class=\"ph-nav\"> <div ng-repeat=\"view in views\" class=\"ph-nav-item\" ng-class=\"{selected: view.selected, disabled: view.disabled}\" ng-click=\"vm.handleViewChange(view)\"> <div class=\"ph-nav-content\"> <div ng-bind-html=\"view.titleHtml\"></div> <div ng-bind-html=\"headerContents[view.contentKey].textHtml\"></div> </div> <div class=\"ph-header-icon {{::view.icon}}\"></div> </div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySegmentTableTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySegmentTableTemplate.html",
    "<div class=\"ph-segment-table\" ng-controller=\"PurchaseHistorySegmentTableCtrl as vm\"> <script type=\"text/ng-template\" id=\"segment_table_tree_renderer.html\"> <tbody>\n" +
    "			<tr class=\"ph-table-row\" ng-init=\"marginLeft={marginLeft:(tree.levelsId.length-1)*20+(tree.data.hasChildren?0:20)+'px'}\">\n" +
    "				<td title=\"{{::tree.data.displayName}}\">\n" +
    "					<span ng-style=\"marginLeft\">\n" +
    "						<span ng-if=\"::tree.data.hasChildren\" class=\"toggle\" ng-class=\"tree.expanded ? 'expanded' : 'collapsed'\" ng-click=\"vm.toggleNode(tree)\"></span>\n" +
    "						{{::tree.data.displayName}}\n" +
    "					</span>\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					{{tree.data.accountProductTotalSpend | formatDollar}}\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					{{tree.data.segmentProductTotalAverageSpend | formatDollar}}\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					<div class=\"ph-segment-bar\" ng-if=\"tree.data.segmentDiff !== null && tree.data.segmentDiff < 0\">\n" +
    "						<span>{{tree.data.segmentDiff | formatDollar}}</span>\n" +
    "						<div ng-style=\"tree.data.segmentBarStyle\"></div>\n" +
    "					</div>\n" +
    "				</td> \n" +
    "				<td>\n" +
    "					<div class=\"ph-segment-bar\" ng-if=\"tree.data.segmentDiff !== null && tree.data.segmentDiff > 0\">\n" +
    "						<div ng-style=\"tree.data.segmentBarStyle\"></div>\n" +
    "						<span>+ {{tree.data.segmentDiff | formatDollar}}</span>\n" +
    "					</div>\n" +
    "				</td> \n" +
    "			</tr>\n" +
    "			<tr ng-if=\"tree.expanded\">\n" +
    "				<td class=\"ph-table-tree-row\" colspan=\"5\">\n" +
    "					<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'segment_table_tree_renderer.html'\"></table>\n" +
    "				</td>\n" +
    "			</tr>\n" +
    "		</tbody> </script> <div class=\"ph-sub-header\"> <div class=\"ph-search-group\"> <input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\"> <label for=\"search-input\"></label> </div> </div> <div class=\"ph-table-container\"> <div class=\"ph-table-header\"> <table> <tbody> <tr> <td ng-click=\"sortBy('displayName')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products </div> </td> <td ng-click=\"sortBy('data.accountProductTotalSpend')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='data.accountProductTotalSpend', 'ph-sort-reverse':sortReverse}\">Spend</div> </td> <td ng-click=\"sortBy('data.segmentProductTotalAverageSpend')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='data.segmentProductTotalAverageSpend', 'ph-sort-reverse':sortReverse}\">Benchmark</div> </td> <td> <span class=\"float-left\">Less Than Average</span><span class=\"wrap-arrow\">Benchmark</span><span class=\"float-right\">More than Average</span> </td> </tr> </tbody> </table> </div> <div class=\"ph-tree-table-container\"> <table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'segment_table_tree_renderer.html'\"></table> </div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySpendTableTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySpendTableTemplate.html",
    "<div class=\"ph-spend-table\" ng-controller=\"PurchaseHistorySpendTableCtrl as vm\"> <script type=\"text/ng-template\" id=\"spend_table_tree_renderer.html\"> <tbody>\n" +
    "			<tr class=\"ph-table-row\" ng-init=\"marginLeft={marginLeft:(tree.levelsId.length-1)*20+(tree.data.hasChildren?0:20)+'px'}\">\n" +
    "				<td title=\"{{::tree.data.displayName}}\">\n" +
    "					<span ng-style=\"marginLeft\">\n" +
    "						<span ng-if=\"::tree.data.hasChildren\" class=\"toggle\" ng-class=\"tree.expanded ? 'expanded' : 'collapsed'\" ng-click=\"vm.toggleNode(tree)\"></span>\n" +
    "						{{::tree.data.displayName}}\n" +
    "					</span>\n" +
    "				</td>\n" +
    "				<td data-ng-repeat=\"periodData in tree.data.periods | startFrom: periodStartIndex | limitTo: colsize\">\n" +
    "					<div ng-if=\"::periodData.accountTotalSpend !== null || periodData.accountPrevYearSpend !== null\"\n" +
    "						ng-mouseenter=\"vm.showTooltip($event, periodData)\"\n" +
    "						ng-mouseleave=\"vm.hideTooltip()\"\n" +
    "						class=\"ph-cell-arrow {{::periodData.yoyStat}}\">{{::periodData.accountTotalSpend | formatDollar}}\n" +
    "					</div>\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					<div class=\"ph-cell-arrow {{tree.data.yoyStat}}\">{{tree.data.accountProductTotalSpend | formatDollar}}</div>\n" +
    "				</td>\n" +
    "			</tr>\n" +
    "			<tr ng-if=\"tree.expanded\">\n" +
    "				<td class=\"ph-table-tree-row\" colspan=\"{{colsize+2}}\">\n" +
    "					<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'spend_table_tree_renderer.html'\"></table>\n" +
    "				</td>\n" +
    "			</tr>\n" +
    "		</tbody> </script> <div class=\"ph-sub-header\"> <div class=\"ph-search-group\"> <input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\"> <label for=\"search-input\"></label> </div> <div class=\"float-right ph-legend\"> <span class=\"ph-legend-group\"> <div class=\"square blue\"></div> <div>Account</div> </span> <span class=\"ph-legend-group\"> <div class=\"circle orange\"></div> <div>YoY</div> </span> </div> </div> <div class=\"ph-table-container\"> <div class=\"ph-chart-table\"> <table> <tbody> <tr> <td></td> <td> <div data-purchase-history-timeline period-start-index=\"periodStartIndex\" colsize=\"colsize\"></div> </td> <td></td> </tr> </tbody> </table> </div> <div class=\"ph-table-header\"> <table> <tbody> <tr> <td ng-click=\"sortBy('displayName')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products </div> </td> <td data-ng-repeat=\"periodId in formattedPeriodRange | startFrom: periodStartIndex | limitTo: colsize\">{{::periodId}}</td> <td ng-click=\"sortBy('data.accountProductTotalSpend')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='data.accountProductTotalSpend', 'ph-sort-reverse':sortReverse}\">Total</div> </td> </tr> </tbody> </table> </div> <div class=\"ph-tree-table-container\"> <table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'spend_table_tree_renderer.html'\"></table> <ul class=\"ph-tooltip\" id=\"ph-tooltip\"></ul> </div> </div> </div> ");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryTimelineTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryTimelineTemplate.html",
    "<div class=\"ph-chart-container\"> <button type=\"button\" class=\"prev-button\" ng-click=\"PurchaseHistoryTimelineVm.handlePeriodRangeClick(-1)\" ng-disabled=\"periodStartIndex === 0\"> <span>Previous</span> </button> <div id=\"ph-chart\" class=\"ph-chart\"></div> <button type=\"button\" class=\"next-button\" ng-click=\"PurchaseHistoryTimelineVm.handlePeriodRangeClick(1)\" ng-disabled=\"(periodStartIndex + 1) > max\"> <span>Next</span> </button> </div> ");
}]);

angular.module("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryYearTableTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryYearTableTemplate.html",
    "<div class=\"ph-year-table\" ng-controller=\"PurchaseHistoryYearTableCtrl as vm\"> <script type=\"text/ng-template\" id=\"year_table_tree_renderer.html\"> <tbody>\n" +
    "			<tr class=\"ph-table-row\" ng-init=\"marginLeft={marginLeft:(tree.levelsId.length-1)*20+(tree.data.hasChildren?0:20)+'px'}\">\n" +
    "				<td title=\"{{::tree.data.displayName}}\">\n" +
    "					<span ng-style=\"marginLeft\">\n" +
    "						<span ng-if=\"::tree.data.hasChildren\" class=\"toggle\" ng-class=\"tree.expanded ? 'expanded' : 'collapsed'\" ng-click=\"vm.toggleNode(tree)\"></span>\n" +
    "						{{::tree.data.displayName}}\n" +
    "					</span>\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					<div class=\"ph-year-bar\">\n" +
    "						<div class=\"ph-year-cur\" ng-style=\"tree.data.currentYearBarStyle\"></div>\n" +
    "						<span>{{tree.data.accountProductTotalSpend | formatDollar}}</span>\n" +
    "					</div>\n" +
    "					<div class=\"ph-year-bar\">\n" +
    "						<div class=\"ph-year-prev\" ng-style=\"tree.data.prevYearBarStyle\"></div>\n" +
    "						<span>{{tree.data.accountProductTotalPrevYearSpend | formatDollar}}</span>\n" +
    "					</div>\n" +
    "				</td>\n" +
    "				<td>\n" +
    "					<span class=\"ph-cell-arrow {{tree.data.yoyStat}}\">\n" +
    "						{{tree.data.yoyDiff | formatDollar}}\n" +
    "					</span>\n" +
    "				</td>\n" +
    "			</tr>\n" +
    "			<tr ng-if=\"tree.expanded\">\n" +
    "				<td class=\"ph-table-tree-row\" colspan=\"3\">\n" +
    "					<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'year_table_tree_renderer.html'\"></table>\n" +
    "				</td>\n" +
    "			</tr>\n" +
    "		</tbody> </script> <div class=\"ph-sub-header\"> <div class=\"ph-search-group\"> <input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\"> <label for=\"search-input\"></label> </div> <div class=\"float-right ph-legend\"> <span class=\"ph-legend-group\"> <div class=\"square blue\"></div> <div>This Year</div> </span> <span class=\"ph-legend-group\"> <div class=\"square gray\"></div> <div>Last Year</div> </span> </div> </div> <div class=\"ph-table-container\"> <div class=\"ph-table-header\"> <table> <tbody> <tr> <td ng-click=\"sortBy('displayName')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products </div> </td> <td>Year over Year Spend</td> <td ng-click=\"sortBy('data.yoyDiff')\"> <div class=\"ph-sortable\" ng-class=\"{'ph-sort':sortKey==='data.yoyDiff', 'ph-sort-reverse':sortReverse}\">Change</div> </td> </tr> </tbody> </table> </div> <div class=\"ph-tree-table-container\"> <table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'year_table_tree_renderer.html'\"></table> </div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/repeaterWidget/RepeaterWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/repeaterWidget/RepeaterWidgetTemplate.html",
    "<div data-ng-controller=\"RepeaterWidgetController\"> </div>");
}]);

angular.module("app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidgetTemplate.html",
    "<div data-ng-controller=\"ScreenHeaderWidgetController\"> <h1 class=\"screen-header-widget-title\"> {{title}} </h1> <h2 class=\"screen-header-widget-subtitle\"> {{subtitle}} </h2> </div>");
}]);

angular.module("app/AppCommon/widgets/screenWidget/ScreenWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/screenWidget/ScreenWidgetTemplate.html",
    "<div data-ng-controller=\"ScreenWidgetController\"> </div>");
}]);

angular.module("app/AppCommon/widgets/tabWidget/TabWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/tabWidget/TabWidgetTemplate.html",
    "<div data-ng-controller=\"TabWidgetController\" class=\"tab-container\"> <ul class=\"nav nav-tabs\"> <li class=\"{{tab.IsActive ? 'active' : ''}}\" data-ng-repeat=\"tab in tabs\"> <a href=\"#{{tab.ID}}\" data-ng-click=\"tabClicked($event, tab)\">{{tab.Title}}</a> </li> </ul> <div class=\"tab-content\"> <div class=\"tab-pane {{tab.IsActive ? 'active' : ''}}\" id=\"{{tab.ID}}\" data-ng-repeat=\"tab in tabs\"> <div data-ng-show=\"!tab.HasData\"> <div class=\"empty-analytic-list-container\"> <img class=\"empty-analytic-list-image\" src=\"assets/images/gray_line_chart_icon.png\"> <span class=\"empty-analytic-list-description\"> {{tab.NoDataString}} </span> </div> </div> </div> </div> </div>");
}]);

angular.module("app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html",
    "<div data-ng-controller=\"TalkingPointWidgetController\" class=\"talking-point-container\"> <div data-ng-bind-html=\"talkingPointText\"></div> </div>");
}]);

angular.module("app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html",
    "<div data-ng-controller=\"ThresholdExplorerWidgetController\" class=\"col-md-12\"> <section class=\"panel\"> <div class=\"panel-body\"> <div data-threshold-explorer-chart data-chart-data=\"{{chartData}}\"></div> </div> </section> </div>");
}]);

angular.module("app/core/views/InitialView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/InitialView.html",
    "<div class=\"initial-loading-spinner-container\"> <img class=\"centered\" width=\"80\" height=\"80\" src=\"assets/CommonAssets/images/loading_spinner.gif\"> </div>");
}]);

angular.module("app/core/views/MainHeaderView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/MainHeaderView.html",
    "<div> <div class=\"float-left\" data-ng-show=\"showSellingIdeas\"> <ul class=\"dropdown-navigation\"> <li id=\"dantePlaysTabLink\"> <a href=\"#\" class=\"dante-main-header-link\" data-ng-click=\"handlePlayListClick($event)\"> <i class=\"fa fa-angle-double-left\"></i> <span>{{ResourceUtility.getString('MAIN_HEADER_PLAY_LABEL')}}</span> </a> </li> </ul> </div> <div class=\"float-left\"> <div class=\"dante-header-account-name\"> {{headerLabel}} </div> </div> <div class=\"float-right dante-main-header-logo-container\"> <img class=\"dante-main-header-logo\" src=\"assets/images/lattice_logo_white.png\" width=\"76\" height=\"20\"> </div> <div class=\"float-right salesprism-link-container\" data-ng-show=\"showSalesprismButton\"> <a class=\"btn-salesprism-link\" data-ng-click=\"handleViewInSalesprismClick()\"> {{ResourceUtility.getString('MAIN_HEADER_SALESPRISM_LABEL')}} </a> </div> </div>");
}]);

angular.module("app/core/views/MainView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/MainView.html",
    "<div data-ng-controller=\"MainViewController\"> <div data-ng-controller=\"MainHeaderController\" data-ng-include data-src=\"'app/core/views/MainHeaderView.html'\" data-ng-if=\"showMainHeader\" class=\"row dante-main-header\"> </div> <div id=\"mainContentView\" class=\"content\"> </div> </div>");
}]);

angular.module("app/core/views/NoAssociationView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/NoAssociationView.html",
    "<div data-ng-controller=\"NoAssociationController\" class=\"no-association-parent-container\"> <div class=\"no-association-container\"> <div class=\"no-notion-body\"> <br> <div class=\"no-association-message\"> {{message1}} </div> <br> <div class=\"no-association-message\"> {{supportEmailMessage}}&nbsp;<a href=\"mailto:{{supportEmail}}\">{{supportEmail}}</a>. </div> </div> </div> </div>");
}]);

angular.module("app/core/views/NoNotionView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/NoNotionView.html",
    "<div data-ng-controller=\"NoNotionController\" class=\"no-notion-container\"> <div class=\"no-notion-title\"> {{title}} </div> <div class=\"no-notion-body\"> <img width=\"117\" height=\"145\" class=\"no-notion-image float-left\" src=\"assets/images/lattice_mark.png\"> {{message1}} <br><br> {{message2}} </div> </div>");
}]);

angular.module("app/core/views/ServiceErrorView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/core/views/ServiceErrorView.html",
    "<div data-ng-controller=\"ServiceErrorController\" class=\"service-error-container\"> <div class=\"alert alert-error\"> {{serviceErrorTitle}} </div> <p class=\"service-error-message-container\"> {{serviceErrorMessage}} </p> </div>");
}]);

angular.module("app/leads/views/LeadDetailsView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/leads/views/LeadDetailsView.html",
    "<div data-ng-controller=\"LeadDetailsController\"> <div class=\"js-dante-lead-details-container dante-play-details-container\"> <div class=\"js-dante-lead-details-nav-container dante-play-details-nav-container scroll-y\"> <div class=\"js-dante-lead-details-nav-tiles-container dante-play-details-nav-tiles-container\"></div> </div> <div class=\"dante-play-details-content-container\"> <div class=\"js-lead-details-external-container lead-details-insights-container\"> </div> <div class=\"js-lead-details-internal-container lead-details-insights-container\"> </div> </div> </div> </div> ");
}]);

angular.module("app/plays/views/NoPlaysView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/plays/views/NoPlaysView.html",
    "<div data-ng-controller=\"NoPlaysController\"> <div class=\"no-plays-message\"> <p data-ng-show=\"showCustomMessage\">{{customMessage}}</p> <p data-ng-show=\"!showCustomMessage\">{{noPlaysMessage}} <b>{{accountName}}</b>.</p> </div> </div>");
}]);

angular.module("app/plays/views/PlayDetailsView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/plays/views/PlayDetailsView.html",
    "<div data-ng-controller=\"PlayDetailsController\"> <div class=\"dante-play-details-container\"> <div class=\"dante-play-details-content-container\"> <div id=\"playDetailsContent\" class=\"dante-play-details-tab-container\"> </div> </div> </div></div>");
}]);

angular.module("app/plays/views/PlayListView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/plays/views/PlayListView.html",
    "<div data-ng-controller=\"PlayListController\"> <div id=\"playListScreen\"> </div> </div> ");
}]);

angular.module("app/purchaseHistory/views/PurchaseHistoryView.html", []).run(["$templateCache", function ($templateCache) {
  $templateCache.put("app/purchaseHistory/views/PurchaseHistoryView.html",
    "<div data-ng-controller=\"PurchaseHistoryController\"> <div id=\"purchaseHistoryScreen\"></div> </div>");
}]);
