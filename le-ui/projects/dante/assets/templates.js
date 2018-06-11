angular.module('templates-main', []).run(['$templateCache', function ($templateCache) {
  $templateCache.put("../app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html",
    "<div data-ng-controller=\"AnalyticAttributeListWidgetController\">\n" +
    "    <div data-ng-show=\"!hasAttributes\">\n" +
    "        <div class=\"empty-analytic-list-container\">\n" +
    "            <span class=\"empty-analytic-list-description\">\n" +
    "                {{ResourceUtility.getString('DANTE_LEAD_NO_INSIGHTS_DESCRIPTION')}}&nbsp;\n" +
    "                <a class=\"tooltip fa fa-info-circle\" href=\"#\" title=\"{{learnMoreTooltip}}\" data-ng-click=\"learnMoreClicked($event)\">\n" +
    "                    <!--{{ResourceUtility.getString('ANALYTIC_ATTRIBUTE_LEARN_MORE_LABEL')}}-->\n" +
    "                </a>\n" +
    "            </span>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "    <!--\n" +
    "    <div class=\"analytic-attribute-list-header\" data-ng-show=\"hasAttributes\">\n" +
    "        <div class=\"analytic-attribute-header-label float-left\">\n" +
    "            {{ResourceUtility.getString('ANALYTIC_ATTRIBUTE_HEADER_LABEL')}}\n" +
    "        </div>\n" +
    "        <a class=\"analytic-attribute-header-label \n" +
    "                  analytic-attribute-lift-header-label \n" +
    "                  float-left\n" +
    "                  {{liftDescending ? 'lift-descending' : 'lift-ascending'}}\"\n" +
    "                  data-ng-click=\"liftSortClicked($event)\">\n" +
    "            <div></div>{{liftLabel}}\n" +
    "        </a>\n" +
    "    </div>\n" +
    "    -->\n" +
    "    <div class=\"js-analytic-attribute-list analytic-attribute-list\">\n" +
    "        \n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html",
    "<div class=\"analytic-attribute-tile {{aboveAverageLift ? 'attr-lift-positive': 'attr-lift-negative'}}\" data-ng-controller=\"AnalyticAttributeTileWidgetController\">\n" +
    "    <img class=\"analytic-attribute-tile-icon\" src=\"assets/images/{{icon}}.png\" />\n" +
    "    <div class=\"analytic-attribute-tile-position\">\n" +
    "        <div class=\"float-right\" data-ng-show=\"data.Lift\">\n" +
    "            <div class=\"lift-container\"  data-ng-show=\"showLift\">\n" +
    "                <div class=\"lift-super-label\">CONVERTS</div>\n" +
    "                <div class=\"lift\">{{data.Lift}}</div>\n" +
    "                <div class=\"lift-sub-label\">{{ResourceUtility.getString('LIFT_SUB_LABEL')}}</div>\n" +
    "            </div>\n" +
    "            <div data-ng-show=\"!showLift\">\n" +
    "                <div class=\"lift-indicator-above\" data-ng-show=\"aboveAverageLift\">\n" +
    "                    <div class=\"lift-image fa fa-caret-up\"></div>\n" +
    "                    <div class=\"lift-image-description\">{{ResourceUtility.getString('LIFT_ABOVE_AVERAGE')}}</div>\n" +
    "                </div>\n" +
    "                <div class=\"lift-indicator-below\" data-ng-show=\"!aboveAverageLift\">\n" +
    "                    <div class=\"lift-image fa fa-caret-down\"></div>\n" +
    "                    <div class=\"lift-image-description\">{{ResourceUtility.getString('LIFT_BELOW_AVERAGE')}}</div>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "      \n" +
    "        <div class=\"analytic-attribute-heading\">{{data.DisplayName}}</div>\n" +
    "        \n" +
    "        <div class=\"analytic-attribute-description\" data-ng-show=\"data.Description\">\n" +
    "            {{data.Description}}\n" +
    "        </div>\n" +
    "\n" +
    "        <div class=\"analytic-attribute-title\">{{data.BucketName}}</div>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/arcChartWidget/ArcChartWidgetTemplate.html",
    "<div data-ng-controller=\"ArcChartWidgetController\">\n" +
    "    <div data-arc-chart \n" +
    "         data-chart-size=\"{{ChartSize}}\" \n" +
    "         data-chart-value=\"{{ChartValue}}\" \n" +
    "         data-chart-total=\"{{ChartTotal}}\" \n" +
    "         data-chart-color=\"{{ChartColor}}\" \n" +
    "         data-chart-label=\"{{ChartLabel}}\"></div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidgetTemplate.html",
    "<div data-ng-controller=\"CollapsiblePanelWidgetController\">\n" +
    "    <div class='collapsible-panel-container'>\n" +
    "    <div class='collapsible-panel-header' data-ng-click=\"panelClicked()\">\n" +
    "        <span class='collapsible-panel-title-container'>{{panelTitle}}</span>\n" +
    "        <span class=\"collapsible-panel-button float-left fa {{panelOpen ? 'fa-chevron-up' : 'fa-chevron-down'}}\"></span>\n" +
    "    </div>\n" +
    "    <div class='js-collapsible-panel-content collapsible-panel-content'></div>\n" +
    "</div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html",
    "<div data-ng-controller=\"LeadDetailsTileWidgetController\" class=\"dante-play-details-nav-tile dante-play-details-nav-tile-selected dante-lead-details-tile\" ng-style=\"style()\">\n" +
    "    <div class=\"dante-play-tile-header\">\n" +
    "        {{header}}\n" +
    "    </div>\n" +
    "\n" +
    "    <div class=\"js-dante-lead-tile-score dante-lead-details-tile-score\">\n" +
    "    \n" +
    "    </div>\n" +
    "\n" +
    "    <!--\n" +
    "        <div class='dante-lead-details-tile-selected-arrow' ng-style=\"style2()\"></div>\n" +
    "    -->\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html",
    "<div class=\"col-md-12\" data-ng-controller=\"ModelDetailsWidgetController\">\n" +
    "    <section class=\"panel\">\n" +
    "        <div class=\"panel-body\">\n" +
    "            <div class=\"model-details-header\">\n" +
    "                <div class=\"mdh-lt\">\n" +
    "                    <div class=\"panel-model-scorerating\">\n" +
    "                        <div class=\"model-score-circ\"> <span>{{score}}</span> </div>\n" +
    "                        <div class=\"model-rating\"><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star\"></span> </div>\n" +
    "                    </div>\n" +
    "                </div>\n" +
    "                <div class=\"mdh-mid\">\n" +
    "                    <h1 class=\"h1-h2\"><a href=\"#\" id=\"editable-model-name\" data-type=\"text\" data-placement=\"right\" data-title=\"Enter Model Name\" class=\"editable editable-click\" style=\"display: inline;\">{{displayName}}</a></h1>\n" +
    "                    <div class=\"mdh-datapoints\" id=\"base-datapoints\">\n" +
    "                        <div class=\"value-pair\"> <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_STATUS_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\"><span class=\"fa fa-check-circle active\"></span> Active</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_EXTERNAL_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{externalAttributes}}</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_INTERNAL_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{internalAttributes}}</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_TILE_CREATED_DATE_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{createdDate}}</span> \n" +
    "                        </div>\n" +
    "                    </div>\n" +
    "                    <div id=\"link-showmore\"><a href=\"\" class=\"btn\"><i class=\"fa fa-angle-down\"></i> {{ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON')}} <i class=\"fa fa-angle-down\"></i></a></div>\n" +
    "                    <div class=\"mdh-datapoints sub\" id=\"more-datapoints\" style=\"display: block;\">\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TOTAL_LEADS_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{totalLeads}}</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TEST_SET_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{testSet}}</span>\n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_TRAINING_SET_LABEL')}}</span>\n" +
    "                            <span class=\"title-value\">{{trainingSet}}</span>\n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_SUCCESS_EVENTS_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{totalSuccessEvents}}</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"value-pair\"> \n" +
    "                            <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_CONVERSION_RATE_LABEL')}}</span> \n" +
    "                            <span class=\"title-value\">{{conversionRate}}%</span> \n" +
    "                        </div>\n" +
    "                        <div class=\"mdh-datapoints sub\" id=\"more-datapoints-2\">\n" +
    "                            <div class=\"value-pair\"> \n" +
    "                                <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_LEAD_SOURCE_LABEL')}}</span> \n" +
    "                                <span class=\"title-value\">{{leadSource}}</span> \n" +
    "                            </div>\n" +
    "                            <div class=\"value-pair\"> \n" +
    "                                <span class=\"title-label\">{{ResourceUtility.getString('MODEL_DETAILS_OPPORTUNITY_LABEL')}}</span> \n" +
    "                                <span class=\"title-value\">{{opportunity}}</span> \n" +
    "                            </div>\n" +
    "                        </div>\n" +
    "                    </div>\n" +
    "                </div>\n" +
    "                <div class=\"mdh-rt\">\n" +
    "                    <div class=\"btn-group\"> <a class=\"btn btn-primary\" data-toggle=\"modal\" href=\"#score-modal\">{{ResourceUtility.getString('BUTTON_ACTIVATE_LABEL')}}</a> </div>\n" +
    "                    <div class=\"btn-group\"> <a class=\"btn btn-secondary\" data-toggle=\"modal\" href=\"#delete-modal\"><i class=\"fa fa-trash-o\"></i></a> </div>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </section>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html",
    "<div class=\"col-md-4 model-list-tile-widget\" data-ng-controller=\"ModelListTileWidgetController\"  data-ng-click=\"tileClick()\">\n" +
    "    <section class=\"panel panel-model-fancy\" id=\"panel-model-1\">\n" +
    "        <div class=\"fancy-header model-bg-1\">\n" +
    "            <div class=\"model-header-txt\"><span class=\"fa fa-check-circle\"></span> {{displayName}}</div>\n" +
    "                <div class=\"panel-model-scorerating\">\n" +
    "                    <div class=\"model-score-circ\"> <span>{{score}}</span> </div>\n" +
    "                    <div class=\"model-rating\"><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star active\"></span><span class=\"fa fa-star\"></span> </div>\n" +
    "                </div>\n" +
    "                <ul class=\"tile-datapoints-primary\">\n" +
    "                    <li> \n" +
    "                        <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_STATUS_LABEL')}}</span> \n" +
    "                        <span class=\"tile-value\">{{status}}</span> \n" +
    "                    </li>\n" +
    "                </ul>\n" +
    "        </div>\n" +
    "        <div class=\"tile-datapoints\">\n" +
    "            <ul>\n" +
    "                <li>\n" +
    "                    <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_EXTERNAL_LABEL')}}</span> \n" +
    "                    <span class=\"tile-value\">{{externalAttributes}}</span> \n" +
    "                </li>\n" +
    "                <li>\n" +
    "                    <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_INTERNAL_LABEL')}}</span> \n" +
    "                    <span class=\"tile-value\">{{internalAttributes}}</span> \n" +
    "                </li>\n" +
    "                <li>\n" +
    "                    <span class=\"tile-label\">{{ResourceUtility.getString('MODEL_TILE_CREATED_DATE_LABEL')}}</span> \n" +
    "                    <span class=\"tile-value\">{{createdDate}}</span> \n" +
    "                </li>\n" +
    "            </ul>\n" +
    "        </div>\n" +
    "    </section>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidgetTemplate.html",
    "<div data-ng-controller=\"PlayDetailsTileWidgetController\" class=\"dante-play-details-nav-tile {{isSelected ? 'dante-play-details-nav-tile-selected' : ''}}\" data-ng-show=\"tileData.IsActive\" data-ng-click=\"playNameClicked()\">\n" +
    "    <div class=\"dante-play-tile-header\">\n" +
    "        <span> </span> <!-- Empty span so ellipsis will be correct color in IE (see DEF-6421) -->\n" +
    "        <div class=\"icon pt-{{tileData.PlayType}} play-type-icon\"></div>\n" +
    "        {{tileData.Name}}\n" +
    "    </div>\n" +
    "    <div class=\"js-dante-play-details-nav-tile-score dante-play-details-nav-tile-score float-left\" data-ng-show=\"showScore\">\n" +
    "        \n" +
    "    </div>\n" +
    "    <div class='dante-play-details-nav-tile-selected-arrow float-right' data-ng-show=\"isSelected\"></div>\n" +
    "    <div class='float-right'>\n" +
    "        <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.Lift\">\n" +
    "            <div class=\"dante-play-tile-metric\">\n" +
    "                <p class=\"dante-play-tile-metric-value\">\n" +
    "                    {{tileData.Lift}}\n" +
    "                </p>\n" +
    "                <p class=\"dante-play-tile-metric-label\">\n" +
    "                    {{tileData.LiftDisplayKey}}\n" +
    "                </p>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "                        \n" +
    "        <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.ActiveDays\">\n" +
    "            <div class=\"dante-play-tile-metric\">\n" +
    "                <p class=\"dante-play-tile-metric-value\">\n" +
    "                    {{tileData.ActiveDays}}\n" +
    "                </p>\n" +
    "                <p class=\"dante-play-tile-metric-label\">\n" +
    "                    {{tileData.ActiveDaysDisplayKey}}\n" +
    "                </p>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "                        \n" +
    "        <div class=\"dante-play-details-tile-metric-container\" data-ng-show=\"tileData.Revenue\">\n" +
    "            <div class=\"dante-play-tile-metric\">\n" +
    "                <p class=\"dante-play-tile-metric-value\">\n" +
    "                    {{tileData.Revenue}}\n" +
    "                </p>\n" +
    "                <p class=\"dante-play-tile-metric-label\">\n" +
    "                    {{tileData.RevenueDisplayKey}}\n" +
    "                </p>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html",
    "<section data-ng-controller=\"PlayListTileWidgetController\" class=\"tile-flip-container dante-play-tile-container\" data-ng-click=\"playNameClicked()\">\n" +
    "<div class=\"tile-flipper\">\n" +
    "<div class=\"dante-play-tile\">\n" +
    "    <div class=\"dante-play-tile-content\">\n" +
    "                \n" +
    "        <div class=\"dante-play-tile-header\">\n" +
    "            <span> </span> <!-- Empty span so ellipsis will be correct color in IE (see DEF-6421) -->\n" +
    "            <div class=\"icon pt-{{tileData.PlayType}} play-type-icon\" data-ng-show=\"tileData.PlayType\"></div>\n" +
    "            {{tileData.Name}}\n" +
    "        </div>\n" +
    "        <div class=\"js-dante-play-tile-score dante-play-tile-score\" data-ng-class=\"noScoreClass\"> </div>\n" +
    "        \n" +
    "        <div>\n" +
    "            <div class=\"dante-play-tile-metric-container\" data-ng-show=\"showLift && tileData.Lift\">\n" +
    "                <div class=\"dante-play-tile-metric\">\n" +
    "                    <p class=\"dante-play-tile-metric-value\">\n" +
    "                        {{tileData.Lift}}\n" +
    "                    </p>\n" +
    "                    <p class=\"dante-play-tile-metric-label uppercase\">\n" +
    "                        {{tileData.LiftDisplayKey}}\n" +
    "                    </p>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "                \n" +
    "            <div class=\"dante-play-tile-metric-container\" data-ng-show=\"tileData.ActiveDays\">\n" +
    "                <div class=\"dante-play-tile-metric\">\n" +
    "                    <p class=\"dante-play-tile-metric-value\">\n" +
    "                        {{tileData.ActiveDays}}\n" +
    "                    </p>\n" +
    "                    <p class=\"dante-play-tile-metric-label uppercase\">\n" +
    "                        {{tileData.ActiveDaysDisplayKey}}\n" +
    "                    </p>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "                \n" +
    "            <div class=\"dante-play-tile-metric-container\" data-ng-show=\"tileData.Revenue\">\n" +
    "                <div class=\"dante-play-tile-metric\">\n" +
    "                    <p class=\"dante-play-tile-metric-value\">\n" +
    "                        {{tileData.Revenue}}\n" +
    "                    </p>\n" +
    "                    <p class=\"dante-play-tile-metric-label uppercase\">\n" +
    "                        {{tileData.RevenueDisplayKey}}\n" +
    "                    </p>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "        <p class=\"dante-play-tile-description\">\n" +
    "            {{tileData.Objective}}\n" +
    "        </p>\n" +
    "    </div>   \n" +
    "</div>\n" +
    "</div>\n" +
    "</section>");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidgetTemplate.html",
    "<div data-ng-controller=\"PurchaseHistoryWidgetController\" class=\"purchase-history\">\n" +
    "	<div class=\"initial-loading-spinner-container\" style=\"height: 250px;\">\n" +
    "		<div style=\"color: #a0cadb; display: inline;\" class=\"la-line-scale-party la-dark la-2x\">\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "		</div>\n" +
    "		<div style=\"color: #a0cadb; display: inline;\" class=\"la-line-scale-party la-dark la-2x\">\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "			<div></div>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "	<div id=\"purchaseHistoryHeader\"></div>\n" +
    "	<div id=\"purchaseHistoryMain\"></div>\n" +
    "</div>\n" +
    "");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryNavTemplate.html",
    "<div ng-controller=\"PurchaseHistoryNavCtrl as vm\">\n" +
    "	<div class=\"ph-filter\">\n" +
    "		<span class=\"ph-filter-group\">\n" +
    "			Date Range:\n" +
    "			<div class=\"datepicker\"\n" +
    "				dropdown-select=\"periodIdStartOptions\"\n" +
    "				dropdown-model=\"periodIdStartSelected\"\n" +
    "				dropdown-onchange=\"vm.selectStartPeriodId(selected)\"></div>\n" +
    "			to\n" +
    "			<div class=\"datepicker\"\n" +
    "				dropdown-select=\"periodIdEndOptions\"\n" +
    "				dropdown-model=\"periodIdEndSelected\"\n" +
    "				dropdown-onchange=\"vm.selectEndPeriodId(selected)\"></div>\n" +
    "		</span>\n" +
    "		<span class=\"ph-filter-group\">\n" +
    "			Show:\n" +
    "			<div dropdown-select=\"periodOptions\"\n" +
    "				dropdown-model=\"periodSelected\"\n" +
    "				dropdown-onchange=\"vm.selectPeriod(selected)\"></div>\n" +
    "		</span>\n" +
    "		<span class=\"ph-filter-group\">\n" +
    "			Show:\n" +
    "			<div dropdown-select=\"filterOptions\"\n" +
    "				dropdown-model=\"filterSelected\"\n" +
    "				dropdown-onchange=\"vm.selectFilter(selected)\"></div>\n" +
    "		</span>\n" +
    "	</div>\n" +
    "	<div class=\"ph-nav\">\n" +
    "		<div ng-repeat=\"view in views\" class=\"ph-nav-item\" ng-class=\"{selected: view.selected, disabled: view.disabled}\" ng-click=\"vm.handleViewChange(view)\">\n" +
    "			<div class=\"ph-nav-content\">\n" +
    "				<div ng-bind-html=\"view.titleHtml\"></div>\n" +
    "				<div ng-bind-html=\"headerContents[view.contentKey].textHtml\"></div>\n" +
    "			</div>\n" +
    "			<div class=\"ph-header-icon {{::view.icon}}\"></div>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySegmentTableTemplate.html",
    "<div class=\"ph-segment-table\" ng-controller=\"PurchaseHistorySegmentTableCtrl as vm\">\n" +
    "	<script type=\"text/ng-template\" id=\"segment_table_tree_renderer.html\">\n" +
    "		<tbody>\n" +
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
    "		</tbody>\n" +
    "	</script>\n" +
    "	<div class=\"ph-sub-header\">\n" +
    "		<div class=\"ph-search-group\">\n" +
    "			<input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\" />\n" +
    "			<label for=\"search-input\"></label>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "	<div class=\"ph-table-container\">\n" +
    "		<div class=\"ph-table-header\">\n" +
    "			<table>\n" +
    "				<tbody>\n" +
    "					<tr>\n" +
    "						<td ng-click=\"sortBy('displayName')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products\n" +
    "							</div>\n" +
    "						</td>\n" +
    "						<td ng-click=\"sortBy('data.accountProductTotalSpend')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='data.accountProductTotalSpend', 'ph-sort-reverse':sortReverse}\">Spend</div>\n" +
    "						</td>\n" +
    "						<td ng-click=\"sortBy('data.segmentProductTotalAverageSpend')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='data.segmentProductTotalAverageSpend', 'ph-sort-reverse':sortReverse}\">Benchmark</div>\n" +
    "						</td>\n" +
    "						<td>\n" +
    "							<span class=\"float-left\">Less Than Average</span><span class=\"wrap-arrow\">Benchmark</span><span class=\"float-right\">More than Average</span>\n" +
    "						</td>\n" +
    "					</tr>\n" +
    "				</tbody>\n" +
    "			</table>\n" +
    "		</div>\n" +
    "		<div class=\"ph-tree-table-container\">\n" +
    "			<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'segment_table_tree_renderer.html'\"></table>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySpendTableTemplate.html",
    "<div class=\"ph-spend-table\" ng-controller=\"PurchaseHistorySpendTableCtrl as vm\">\n" +
    "	<script type=\"text/ng-template\" id=\"spend_table_tree_renderer.html\">\n" +
    "		<tbody>\n" +
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
    "		</tbody>\n" +
    "	</script>\n" +
    "	<div class=\"ph-sub-header\">\n" +
    "		<div class=\"ph-search-group\">\n" +
    "			<input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\" />\n" +
    "			<label for=\"search-input\"></label>\n" +
    "		</div>\n" +
    "		<div class=\"float-right ph-legend\">\n" +
    "			<span class=\"ph-legend-group\">\n" +
    "				<div class=\"square blue\"></div>\n" +
    "				<div>Account</div>\n" +
    "			</span>\n" +
    "			<span class=\"ph-legend-group\">\n" +
    "				<div class=\"circle orange\"></div>\n" +
    "				<div>YoY</div>\n" +
    "			</span>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "	<div class=\"ph-table-container\">\n" +
    "		<div class=\"ph-chart-table\">\n" +
    "			<table>\n" +
    "				<tbody>\n" +
    "					<tr>\n" +
    "						<td></td>\n" +
    "						<td>\n" +
    "							<div data-purchase-history-timeline\n" +
    "								period-start-index=\"periodStartIndex\"\n" +
    "								colsize=\"colsize\"></div>\n" +
    "						</td>\n" +
    "						<td></td>\n" +
    "					</tr>\n" +
    "				</tbody>\n" +
    "			</table>\n" +
    "		</div>\n" +
    "		<div class=\"ph-table-header\">\n" +
    "			<table>\n" +
    "				<tbody>\n" +
    "					<tr>\n" +
    "						<td ng-click=\"sortBy('displayName')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products\n" +
    "							</div>\n" +
    "						</td>\n" +
    "						<td data-ng-repeat=\"periodId in formattedPeriodRange | startFrom: periodStartIndex | limitTo: colsize\">{{::periodId}}</td>\n" +
    "						<td ng-click=\"sortBy('data.accountProductTotalSpend')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='data.accountProductTotalSpend', 'ph-sort-reverse':sortReverse}\">Total</div>\n" +
    "						</td>\n" +
    "					</tr>\n" +
    "				</tbody>\n" +
    "			</table>\n" +
    "		</div>\n" +
    "		<div class=\"ph-tree-table-container\">\n" +
    "			<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'spend_table_tree_renderer.html'\"></table>\n" +
    "			<ul class=\"ph-tooltip\" id=\"ph-tooltip\"></ul>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "</div>\n" +
    "");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryTimelineTemplate.html",
    "<div class=\"ph-chart-container\">\n" +
    "	<button type=\"button\" class=\"prev-button\"\n" +
    "		ng-click=\"PurchaseHistoryTimelineVm.handlePeriodRangeClick(-1)\"\n" +
    "		ng-disabled=\"periodStartIndex === 0\">\n" +
    "		<span>Previous</span>\n" +
    "	</button>\n" +
    "	<div id=\"ph-chart\" class=\"ph-chart\"></div>\n" +
    "	<button type=\"button\" class=\"next-button\"\n" +
    "		ng-click=\"PurchaseHistoryTimelineVm.handlePeriodRangeClick(1)\"\n" +
    "		ng-disabled=\"(periodStartIndex + 1) > max\">\n" +
    "		<span>Next</span>\n" +
    "	</button>\n" +
    "</div>\n" +
    "");
  $templateCache.put("../app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryYearTableTemplate.html",
    "<div class=\"ph-year-table\" ng-controller=\"PurchaseHistoryYearTableCtrl as vm\">\n" +
    "	<script type=\"text/ng-template\" id=\"year_table_tree_renderer.html\">\n" +
    "		<tbody>\n" +
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
    "		</tbody>\n" +
    "	</script>\n" +
    "	<div class=\"ph-sub-header\">\n" +
    "		<div class=\"ph-search-group\">\n" +
    "			<input name=\"search-input\" type=\"text\" placeholder=\"Search Products\" ng-model=\"searchProduct\" />\n" +
    "			<label for=\"search-input\"></label>\n" +
    "		</div>\n" +
    "		<div class=\"float-right ph-legend\">\n" +
    "			<span class=\"ph-legend-group\">\n" +
    "				<div class=\"square blue\"></div>\n" +
    "				<div>This Year</div>\n" +
    "			</span>\n" +
    "			<span class=\"ph-legend-group\">\n" +
    "				<div class=\"square gray\"></div>\n" +
    "				<div>Last Year</div>\n" +
    "			</span>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "	<div class=\"ph-table-container\">\n" +
    "		<div class=\"ph-table-header\">\n" +
    "			<table>\n" +
    "				<tbody>\n" +
    "					<tr>\n" +
    "						<td ng-click=\"sortBy('displayName')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='displayName', 'ph-sort-reverse':sortReverse}\">Products\n" +
    "							</div>\n" +
    "						</td>\n" +
    "						<td>Year over Year Spend</td>\n" +
    "						<td ng-click=\"sortBy('data.yoyDiff')\">\n" +
    "							<div class=\"ph-sortable\"\n" +
    "								ng-class=\"{'ph-sort':sortKey==='data.yoyDiff', 'ph-sort-reverse':sortReverse}\">Change</div>\n" +
    "						</td>\n" +
    "					</tr>\n" +
    "				</tbody>\n" +
    "			</table>\n" +
    "		</div>\n" +
    "		<div class=\"ph-tree-table-container\">\n" +
    "			<table ng-repeat=\"tree in tree.nodes | filter:vm.spendFilter | filter:vm.searchProduct | sortWithNulls:sortKey:sortReverse\" ng-include=\"'year_table_tree_renderer.html'\"></table>\n" +
    "		</div>\n" +
    "	</div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/repeaterWidget/RepeaterWidgetTemplate.html",
    "<div data-ng-controller=\"RepeaterWidgetController\">\n" +
    "    \n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidgetTemplate.html",
    "<div data-ng-controller=\"ScreenHeaderWidgetController\">\n" +
    "    <h1 class=\"screen-header-widget-title\"> {{title}} </h1>\n" +
    "    <h2 class=\"screen-header-widget-subtitle\"> {{subtitle}} </h2>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/screenWidget/ScreenWidgetTemplate.html",
    "<div data-ng-controller=\"ScreenWidgetController\">\n" +
    "    \n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/tabWidget/TabWidgetTemplate.html",
    "<div data-ng-controller=\"TabWidgetController\" class=\"tab-container\">\n" +
    "    <ul class=\"nav nav-tabs\">\n" +
    "        <li class=\"{{tab.IsActive ? 'active' : ''}}\" data-ng-repeat=\"tab in tabs\">\n" +
    "            <a href=\"#{{tab.ID}}\" data-ng-click=\"tabClicked($event, tab)\">{{tab.Title}}</a>\n" +
    "        </li>\n" +
    "    </ul>\n" +
    "    <div class=\"tab-content\">\n" +
    "        <div class=\"tab-pane {{tab.IsActive ? 'active' : ''}}\" id=\"{{tab.ID}}\" data-ng-repeat=\"tab in tabs\">\n" +
    "            <div data-ng-show=\"!tab.HasData\">\n" +
    "                <div class=\"empty-analytic-list-container\">\n" +
    "                    <img class=\"empty-analytic-list-image\" src=\"assets/images/gray_line_chart_icon.png\"/>\n" +
    "                    <span class=\"empty-analytic-list-description\">\n" +
    "                        {{tab.NoDataString}}\n" +
    "                    </span>\n" +
    "                </div>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html",
    "<div data-ng-controller=\"TalkingPointWidgetController\" class=\"talking-point-container\">\n" +
    "    <div data-ng-bind-html=\"talkingPointText\"></div>\n" +
    "</div>");
  $templateCache.put("../app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidgetTemplate.html",
    "<div data-ng-controller=\"ThresholdExplorerWidgetController\" class=\"col-md-12\">\n" +
    "    <section class=\"panel\">\n" +
    "        <div class=\"panel-body\">\n" +
    "            <div data-threshold-explorer-chart \n" +
    "                 data-chart-data=\"{{chartData}}\"></div>\n" +
    "        </div>\n" +
    "    </section>\n" +
    "</div>");
  $templateCache.put("../app/core/views/InitialView.html",
    "<div class=\"initial-loading-spinner-container\">\n" +
    "    <img class=\"centered\" width=\"80\" height=\"80\" src=\"assets/CommonAssets/images/loading_spinner.gif\"/>\n" +
    "</div>");
  $templateCache.put("../app/core/views/MainHeaderView.html",
    "<div>\n" +
    "    <div class=\"float-left\" data-ng-show=\"showSellingIdeas\">\n" +
    "        <ul class=\"dropdown-navigation\">\n" +
    "            <li id=\"dantePlaysTabLink\">\n" +
    "                <a href=\"#\" class=\"dante-main-header-link\" data-ng-click=\"handlePlayListClick($event)\">\n" +
    "                    <i class=\"fa fa-angle-double-left\"></i>\n" +
    "                    \n" +
    "                    <span>{{ResourceUtility.getString('MAIN_HEADER_PLAY_LABEL')}}</span>\n" +
    "                    \n" +
    "                </a>\n" +
    "            </li>\n" +
    "        </ul>\n" +
    "    </div>\n" +
    "    <div class=\"float-left\">\n" +
    "        <div class=\"dante-header-account-name\">\n" +
    "            {{headerLabel}}\n" +
    "        </div>\n" +
    "    </div>\n" +
    "    <div class=\"float-right dante-main-header-logo-container\">\n" +
    "        <img class=\"dante-main-header-logo\" src=\"assets/images/lattice_logo_white.png\" width=\"76\" height=\"20\">\n" +
    "    </div> \n" +
    "    <div class=\"float-right salesprism-link-container\" data-ng-show=\"showSalesprismButton\">\n" +
    "        <a class=\"btn-salesprism-link\" data-ng-click=\"handleViewInSalesprismClick()\">\n" +
    "            {{ResourceUtility.getString('MAIN_HEADER_SALESPRISM_LABEL')}}\n" +
    "        </a>\n" +
    "        \n" +
    "    </div> \n" +
    "</div>");
  $templateCache.put("../app/core/views/MainView.html",
    "<div data-ng-controller=\"MainViewController\">\n" +
    "     \n" +
    "    <div data-ng-controller=\"MainHeaderController\" \n" +
    "         data-ng-include data-src=\"'app/core/views/MainHeaderView.html'\"\n" +
    "         data-ng-if=\"showMainHeader\"\n" +
    "         class=\"row dante-main-header\"> </div>\n" +
    "    \n" +
    "    <div id=\"mainContentView\" class=\"content\">\n" +
    "        \n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/core/views/NoAssociationView.html",
    "<div data-ng-controller=\"NoAssociationController\" class=\"no-association-parent-container\">\n" +
    "    <div class=\"no-association-container\">\n" +
    "        <div class=\"no-notion-body\">\n" +
    "            <br/>\n" +
    "            <div class=\"no-association-message\">\n" +
    "                {{message1}}\n" +
    "            </div>\n" +
    "            <br/>\n" +
    "            <div class=\"no-association-message\">\n" +
    "                {{supportEmailMessage}}&nbsp;<a href=\"mailto:{{supportEmail}}\">{{supportEmail}}</a>.\n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/core/views/NoNotionView.html",
    "<div data-ng-controller=\"NoNotionController\" class=\"no-notion-container\">\n" +
    "    <div class=\"no-notion-title\">\n" +
    "        {{title}}\n" +
    "    </div>\n" +
    "    <div class=\"no-notion-body\">\n" +
    "        <img width=\"117\" height=\"145\" class=\"no-notion-image float-left\" src=\"assets/images/lattice_mark.png\"/>\n" +
    "        {{message1}}\n" +
    "        <br/><br/>\n" +
    "        {{message2}}\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/core/views/ServiceErrorView.html",
    "<div data-ng-controller=\"ServiceErrorController\" class=\"service-error-container\">\n" +
    "    <div class=\"alert alert-error\">\n" +
    "        {{serviceErrorTitle}}\n" +
    "    </div>\n" +
    "    <p class=\"service-error-message-container\">\n" +
    "        {{serviceErrorMessage}}\n" +
    "    </p>\n" +
    "</div>");
  $templateCache.put("../app/leads/views/LeadDetailsView.html",
    "<div data-ng-controller=\"LeadDetailsController\">\n" +
    "    <div class=\"js-dante-lead-details-container dante-play-details-container\">\n" +
    "        <div class='js-dante-lead-details-nav-container dante-play-details-nav-container scroll-y'>\n" +
    "            <div class='js-dante-lead-details-nav-tiles-container dante-play-details-nav-tiles-container'></div>\n" +
    "        </div>\n" +
    "        <div class=\"dante-play-details-content-container\">\n" +
    "            <div class=\"js-lead-details-external-container lead-details-insights-container\">\n" +
    "                \n" +
    "            </div>\n" +
    "            <div class=\"js-lead-details-internal-container lead-details-insights-container\">\n" +
    "                \n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "</div>\n" +
    "");
  $templateCache.put("../app/plays/views/NoPlaysView.html",
    "<div data-ng-controller=\"NoPlaysController\">\n" +
    "    <div class=\"no-plays-message\">\n" +
    "        <p data-ng-show=\"showCustomMessage\">{{customMessage}}</p>\n" +
    "        <p data-ng-show=\"!showCustomMessage\">{{noPlaysMessage}} <b>{{accountName}}</b>.</p>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/plays/views/PlayDetailsView.html",
    "<div data-ng-controller=\"PlayDetailsController\">\n" +
    "    <div class=\"dante-play-details-container\">\n" +
    "        \n" +
    "            <!-- \n" +
    "            <div class=\"dante-play-details-nav-container float-left scroll-y\" data-ng-show=\"showPlayDetailsNav\">\n" +
    "                <div id=\"playDetailsNav\" class=\"dante-play-details-nav-tiles-container\"></div>\n" +
    "            </div>\n" +
    "            -->\n" +
    "        \n" +
    "        <div class=\"dante-play-details-content-container\">\n" +
    "            <!-- \n" +
    "                <div id=\"playDetailsHeader\"> </div>\n" +
    "            -->\n" +
    "            <div id=\"playDetailsContent\" class=\"dante-play-details-tab-container\"> </div>\n" +
    "    </div>\n" +
    "</div>");
  $templateCache.put("../app/plays/views/PlayListView.html",
    "<div data-ng-controller=\"PlayListController\">\n" +
    "    <div id=\"playListScreen\">\n" +
    "    \n" +
    "    </div>\n" +
    "</div>\n" +
    "\n" +
    "");
  $templateCache.put("../app/purchaseHistory/views/PurchaseHistoryView.html",
    "<div data-ng-controller=\"PurchaseHistoryController\">\n" +
    "	<div id=\"purchaseHistoryScreen\"></div>\n" +
    "</div>");
  $templateCache.put("../index.html",
    "<!DOCTYPE html>\n" +
    "<html lang=\"en\" id=\"ng-app\" data-ng-app=\"mainApp\">\n" +
    "<head runat=\"server\">\n" +
    "    <base href=\"/dante/\">\n" +
    "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>\n" +
    "    <meta http-equiv=\"X-UA-Compatible\" content=\"IE=9\"><!-- This is needed to prevent IE from rendering the site in Compatibility Mode -->\n" +
    "\n" +
    "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0\" />\n" +
    "\n" +
    "    <link rel=\"shortcut icon\" href=\"assets/CommonAssets/images/lattice.ico\" type=\"image/x-icon\">\n" +
    "    <title>Lattice Engines</title>\n" +
    "\n" +
    "    <!-- build:sass(DanteWebSite) assets/styles/production_@@versionString.css -->\n" +
    "    <link href=\"assets/styles/production.css\" rel=\"stylesheet\" type=\"text/css\" />\n" +
    "    <!-- endbuild -->\n" +
    "</head>\n" +
    "<body data-ng-controller=\"MainController\" class=\"core-background\">\n" +
    "    <div id=\"mainView\">\n" +
    "        <div class=\"initial-loading-spinner-container\">\n" +
    "            <!--\n" +
    "                <img class=\"centered\" width=\"80\" height=\"80\" src=\"assets/CommonAssets/images/loading_spinner.gif\"/>\n" +
    "            -->\n" +
    "            <div style=\"color: #a0cadb; display: inline;\" class=\"la-line-scale-party la-dark la-2x\">\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "            </div>\n" +
    "            <div style=\"color: #a0cadb; display: inline;\" class=\"la-line-scale-party la-dark la-2x\">\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "                <div></div>\n" +
    "            </div>\n" +
    "        </div>\n" +
    "    </div>\n" +
    "    <div id=\"notificationCenter\"></div>\n" +
    "    <div id=\"dialogContainer\"> </div>\n" +
    "    <!--\n" +
    "    This div will be added to the DOM if the browser is IE\n" +
    "    Use DOMUtil.isIE() to detect internet explorer.\n" +
    "    -->\n" +
    "    <!--[if IE]> <div id=\"isIE\"></div> <![endif]-->\n" +
    "\n" +
    "    <!-- build:js(DanteWebSite) app/production_@@versionString.js -->\n" +
    "    <!-- vendor -->\n" +
    "    <script src=\"app/AppCommon/vendor/jquery-2.1.1.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular/angular.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular/angular-resource.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular/angular-route.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular/angular-sanitize.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular/angular-animate.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/jstorage.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/jquery-ui-1.9.2.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/d3.v3.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/moment.js\"></script>\n" +
    "    <script src=\"app/AppCommon/vendor/angular-dropdowns.js\"></script>\n" +
    "\n" +
    "\n" +
    "    <!-- common -->\n" +
    "    <script src=\"assets/templates.js\"></script>\n" +
    "    <script src=\"app/AppCommon/directives/ngEnterDirective.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/ExceptionOverrideUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/URLUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/ResourceUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/FaultUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/ConfigConstantUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/AuthenticationUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/SortUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/WidgetConfigUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/MetadataUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/AnimationUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/DateTimeFormatUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/NumberUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/AnalyticAttributeUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/StringUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/utilities/PurchaseHistoryUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/modals/SimpleModal.js\"></script>\n" +
    "    <script src=\"app/AppCommon/services/WidgetFrameworkService.js\"></script>\n" +
    "    <script src=\"app/AppCommon/services/PlayTileService.js\"></script>\n" +
    "    <script src=\"app/AppCommon/services/PurchaseHistoryService.js\"></script>\n" +
    "    <script src=\"app/AppCommon/services/PurchaseHistoryTooltipService.js\"></script>\n" +
    "    <script src=\"app/AppCommon/factory/ProductTreeFactory.js\"></script>\n" +
    "    <script src=\"app/AppCommon/factory/ProductTotalFactory.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/WidgetEventConstantUtility.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/screenWidget/ScreenWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/repeaterWidget/RepeaterWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/controllers/PurchaseHistoryNav.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/controllers/PurchaseHistoryTimeline.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/controllers/PurchaseHistorySpendTable.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/controllers/PurchaseHistorySegmentTable.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/controllers/PurchaseHistoryYearTable.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/purchaseHistoryWidget/stores/PurchaseHistoryStore.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/arcChartWidget/ArcChartWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/playListTileWidget/PlayListTileWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/tabWidget/TabWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js\"></script>\n" +
    "    <script src=\"app/AppCommon/widgets/talkingPointWidget/TalkingPointWidget.js\"></script>\n" +
    "    <script src=\"app/AppCommon/directives/charts/ArcChartDirective.js\"></script>\n" +
    "\n" +
    "    <!-- app specific -->\n" +
    "    <script src=\"app/app.js\"></script>\n" +
    "    <script src=\"app/core/utilities/BrowserStorageUtility.js\"></script>\n" +
    "    <script src=\"app/core/utilities/ServiceErrorUtility.js\"></script>\n" +
    "\n" +
    "    <script src=\"app/core/services/ResourceStringsService.js\"></script>\n" +
    "    <script src=\"app/core/services/SessionService.js\"></script>\n" +
    "    <script src=\"app/core/services/ConfigurationService.js\"></script>\n" +
    "    <script src=\"app/core/services/DanteWidgetService.js\"></script>\n" +
    "    <script src=\"app/core/services/NotionService.js\"></script>\n" +
    "    <script src=\"app/core/services/LpiPreviewService.js\"></script>\n" +
    "\n" +
    "    <script src=\"app/core/controllers/MainViewController.js\"></script>\n" +
    "    <script src=\"app/core/controllers/MainHeaderController.js\"></script>\n" +
    "    <script src=\"app/core/controllers/ServiceErrorController.js\"></script>\n" +
    "    <script src=\"app/core/controllers/NoNotionController.js\"></script>\n" +
    "    <script src=\"app/core/controllers/NoAssociationController.js\"></script>\n" +
    "    <script src=\"app/plays/controllers/PlayListController.js\"></script>\n" +
    "    <script src=\"app/plays/controllers/PlayDetailsController.js\"></script>\n" +
    "    <script src=\"app/plays/controllers/NoPlaysController.js\"></script>\n" +
    "    <script src=\"app/leads/controllers/LeadDetailsController.js\"></script>\n" +
    "    <script src=\"app/purchaseHistory/controllers/PurchaseHistoryController.js\"></script>\n" +
    "    <!-- endbuild -->\n" +
    "</body>\n" +
    "</html>");
  $templateCache.put("../node_modules/colors/example.html",
    "<!DOCTYPE HTML>\n" +
    "<html lang=\"en-us\">\n" +
    "  <head>\n" +
    "    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\">\n" +
    "    <title>Colors Example</title>\n" +
    "    <script src=\"colors.js\"></script>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "    <script>\n" +
    "\n" +
    "    var test = colors.red(\"hopefully colorless output\");\n" +
    "\n" +
    "    document.write('Rainbows are fun!'.rainbow + '<br/>');\n" +
    "    document.write('So '.italic + 'are'.underline + ' styles! '.bold + 'inverse'.inverse); // styles not widely supported\n" +
    "    document.write('Chains are also cool.'.bold.italic.underline.red); // styles not widely supported\n" +
    "    //document.write('zalgo time!'.zalgo);\n" +
    "    document.write(test.stripColors);\n" +
    "    document.write(\"a\".grey + \" b\".black);\n" +
    "\n" +
    "    document.write(\"Zebras are so fun!\".zebra);\n" +
    "\n" +
    "    document.write(colors.rainbow('Rainbows are fun!'));\n" +
    "    document.write(\"This is \" + \"not\".strikethrough + \" fun.\");\n" +
    "\n" +
    "    document.write(colors.italic('So ') + colors.underline('are') + colors.bold(' styles! ') + colors.inverse('inverse')); // styles not widely supported\n" +
    "    document.write(colors.bold(colors.italic(colors.underline(colors.red('Chains are also cool.'))))); // styles not widely supported\n" +
    "    //document.write(colors.zalgo('zalgo time!'));\n" +
    "    document.write(colors.stripColors(test));\n" +
    "    document.write(colors.grey(\"a\") + colors.black(\" b\"));\n" +
    "\n" +
    "    colors.addSequencer(\"america\", function(letter, i, exploded) {\n" +
    "      if(letter === \" \") return letter;\n" +
    "      switch(i%3) {\n" +
    "        case 0: return letter.red;\n" +
    "        case 1: return letter.white;\n" +
    "        case 2: return letter.blue;\n" +
    "      }\n" +
    "    });\n" +
    "\n" +
    "    colors.addSequencer(\"random\", (function() {\n" +
    "      var available = ['bold', 'underline', 'italic', 'inverse', 'grey', 'yellow', 'red', 'green', 'blue', 'white', 'cyan', 'magenta'];\n" +
    "\n" +
    "      return function(letter, i, exploded) {\n" +
    "        return letter === \" \" ? letter : letter[available[Math.round(Math.random() * (available.length - 1))]];\n" +
    "      };\n" +
    "    })());\n" +
    "\n" +
    "    document.write(\"AMERICA! F--K YEAH!\".america);\n" +
    "    document.write(\"So apparently I've been to Mars, with all the little green men. But you know, I don't recall.\".random);\n" +
    "\n" +
    "    //\n" +
    "    // Custom themes\n" +
    "    //\n" +
    "\n" +
    "    colors.setTheme({\n" +
    "      silly: 'rainbow',\n" +
    "      input: 'grey',\n" +
    "      verbose: 'cyan',\n" +
    "      prompt: 'grey',\n" +
    "      info: 'green',\n" +
    "      data: 'grey',\n" +
    "      help: 'cyan',\n" +
    "      warn: 'yellow',\n" +
    "      debug: 'blue',\n" +
    "      error: 'red'\n" +
    "    });\n" +
    "\n" +
    "    // outputs red text\n" +
    "    document.write(\"this is an error\".error);\n" +
    "\n" +
    "    // outputs yellow text\n" +
    "    document.write(\"this is a warning\".warn);\n" +
    "\n" +
    "    </script>\n" +
    "  </body>\n" +
    "</html>");
  $templateCache.put("../node_modules/faye-websocket/examples/sse.html",
    "<!doctype html>\n" +
    "<html>\n" +
    "  <head>\n" +
    "    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\">\n" +
    "    <title>EventSource test</title>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "\n" +
    "    <h1>EventSource test</h1>\n" +
    "    <ul></ul>\n" +
    "\n" +
    "    <script type=\"text/javascript\">\n" +
    "      var logger = document.getElementsByTagName('ul')[0],\n" +
    "          socket = new EventSource('/');\n" +
    "\n" +
    "      var log = function(text) {\n" +
    "        logger.innerHTML += '<li>' + text + '</li>';\n" +
    "      };\n" +
    "\n" +
    "      socket.onopen = function() {\n" +
    "        log('OPEN');\n" +
    "      };\n" +
    "\n" +
    "      socket.onmessage = function(event) {\n" +
    "        log('MESSAGE: ' + event.data);\n" +
    "      };\n" +
    "\n" +
    "      socket.addEventListener('update', function(event) {\n" +
    "        log('UPDATE(' + event.lastEventId + '): ' + event.data);\n" +
    "      });\n" +
    "\n" +
    "      socket.onerror = function(event) {\n" +
    "        log('ERROR: ' + event.message);\n" +
    "      };\n" +
    "    </script>\n" +
    "\n" +
    "  </body>\n" +
    "</html>\n" +
    "\n" +
    "");
  $templateCache.put("../node_modules/faye-websocket/examples/ws.html",
    "<!doctype html>\n" +
    "<html>\n" +
    "  <head>\n" +
    "    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\">\n" +
    "    <title>WebSocket test</title>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "\n" +
    "    <h1>WebSocket test</h1>\n" +
    "    <ul></ul>\n" +
    "\n" +
    "    <script type=\"text/javascript\">\n" +
    "      var logger = document.getElementsByTagName('ul')[0],\n" +
    "          Socket = window.MozWebSocket || window.WebSocket,\n" +
    "          protos = ['foo', 'bar', 'xmpp'],\n" +
    "          socket = new Socket('ws://' + location.hostname + ':' + location.port + '/', protos),\n" +
    "          index  = 0;\n" +
    "\n" +
    "      var log = function(text) {\n" +
    "        logger.innerHTML += '<li>' + text + '</li>';\n" +
    "      };\n" +
    "\n" +
    "      socket.addEventListener('open', function() {\n" +
    "        log('OPEN: ' + socket.protocol);\n" +
    "        socket.send('Hello, world');\n" +
    "      });\n" +
    "\n" +
    "      socket.onerror = function(event) {\n" +
    "        log('ERROR: ' + event.message);\n" +
    "      };\n" +
    "\n" +
    "      socket.onmessage = function(event) {\n" +
    "        log('MESSAGE: ' + event.data);\n" +
    "        setTimeout(function() { socket.send(++index + ' ' + event.data) }, 2000);\n" +
    "      };\n" +
    "\n" +
    "      socket.onclose = function(event) {\n" +
    "        log('CLOSE: ' + event.code + ', ' + event.reason);\n" +
    "      };\n" +
    "    </script>\n" +
    "\n" +
    "  </body>\n" +
    "</html>\n" +
    "\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/broken_newlines.tpl.html",
    "abc\n" +
    "def");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/custom_attribute_collapse.tpl.html",
    "<div\n" +
    "        my-style=\"\n" +
    "            background-color: red;\n" +
    "            font-size: large;\"\n" +
    "></div>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/empty_attribute.tpl.html",
    "<div ui-view></div>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/five.tpl.html",
    "<div class=\"quotes should be escaped\">\n" +
    "  <span>\n" +
    "    <span>\n" +
    "      <span>\n" +
    "        Lorem ipsum\n" +
    "      </span>\n" +
    "    </span>\n" +
    "  </span>\n" +
    "</div>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/four.tpl.html",
    "This data is \"in quotes\"\n" +
    "And this data is 'in single quotes'\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/one.tpl.html",
    "1 2 3");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/pattern.tpl.html",
    "<form>\n" +
    "    <span class=\"registration-error\" ng-show=\"regForm.password.$error.pattern\">- Fail to match..</span>\n" +
    "    <input type=\"password\" ng-model=\"registerForm.password\" name=\"password\" ng-pattern=\"/^.*(?=.{8,})(?=.*[a-z])(?=.*[A-Z])(?=.*[\\d\\W]).*$/\" required/>\n" +
    "</form>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/process_function.tpl.html",
    "<h1> (ONE) </h1>\n" +
    "<h2> (TWO) </h2>\n" +
    "<h3> (THREE) </h3>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/process_template.tpl.html",
    "<h1> <%= html2js.process_template.testMessages.title %> </h1>\n" +
    "<h2> <%= html2js.process_template.testMessages.subtitle %> </h2>\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/three.tpl.html",
    "Multiple\n" +
    "Lines\n" +
    "");
  $templateCache.put("../node_modules/grunt-html2js/test/fixtures/two.tpl.html",
    "Testing");
  $templateCache.put("../node_modules/grunt-legacy-log-utils/node_modules/underscore.string/test/test.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <meta charset=\"utf-8\">\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jslitmus.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"underscore.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "  <br />\n" +
    "  <h1 class=\"qunit-header\">Underscore.string Speed Suite</h1>\n" +
    "  <!-- <h2 class=\"qunit-userAgent\">\n" +
    "    A representative sample of the functions are benchmarked here, to provide\n" +
    "    a sense of how fast they might run in different browsers.\n" +
    "    Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "    For example, the 'intersect' test measures the number of times you can\n" +
    "    find the intersection of two thousand-element arrays in one second.\n" +
    "  </h2> -->\n" +
    "  <br />\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/grunt-legacy-log-utils/node_modules/underscore.string/test/test_standalone.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings_standalone.js\"></script>\n" +
    "\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/grunt-legacy-log-utils/node_modules/underscore.string/test/test_underscore/index.html",
    "<!DOCTYPE html>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"vendor/qunit.css\" type=\"text/css\" media=\"screen\">\n" +
    "  <script src=\"vendor/jquery.js\"></script>\n" +
    "  <script src=\"vendor/qunit.js\"></script>\n" +
    "  <script src=\"vendor/jslitmus.js\"></script>\n" +
    "  <script src=\"../underscore.js\"></script>\n" +
    "  <script src=\"../../lib/underscore.string.js\"></script>\n" +
    "\n" +
    "  <script src=\"collections.js\"></script>\n" +
    "  <script src=\"arrays.js\"></script>\n" +
    "  <script src=\"functions.js\"></script>\n" +
    "  <script src=\"objects.js\"></script>\n" +
    "  <script src=\"utility.js\"></script>\n" +
    "  <script src=\"chaining.js\"></script>\n" +
    "  <script src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <div id=\"qunit\"></div>\n" +
    "  <div id=\"qunit-fixture\">\n" +
    "    <div id=\"map-test\">\n" +
    "      <div id=\"id1\"></div>\n" +
    "      <div id=\"id2\"></div>\n" +
    "    </div>\n" +
    "  </div>\n" +
    "  <br>\n" +
    "  <h1 class=\"qunit-header\">Underscore Speed Suite</h1>\n" +
    "  <p>\n" +
    "    A representative sample of the functions are benchmarked here, to provide\n" +
    "    a sense of how fast they might run in different browsers.\n" +
    "    Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "    For example, the 'intersection' test measures the number of times you can\n" +
    "    find the intersection of two thousand-element arrays in one second.\n" +
    "  </p>\n" +
    "  <br>\n" +
    "  <script type=\"text/html\" id=\"template\">\n" +
    "    <%\n" +
    "    // a comment\n" +
    "    if (data) { data += 12345; }; %>\n" +
    "    <li><%= data %></li>\n" +
    "  </script>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/grunt-legacy-log/node_modules/underscore.string/test/test.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <meta charset=\"utf-8\">\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jslitmus.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"underscore.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "  <br />\n" +
    "  <h1 class=\"qunit-header\">Underscore.string Speed Suite</h1>\n" +
    "  <!-- <h2 class=\"qunit-userAgent\">\n" +
    "    A representative sample of the functions are benchmarked here, to provide\n" +
    "    a sense of how fast they might run in different browsers.\n" +
    "    Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "    For example, the 'intersect' test measures the number of times you can\n" +
    "    find the intersection of two thousand-element arrays in one second.\n" +
    "  </h2> -->\n" +
    "  <br />\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/grunt-legacy-log/node_modules/underscore.string/test/test_standalone.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings_standalone.js\"></script>\n" +
    "\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/grunt-legacy-log/node_modules/underscore.string/test/test_underscore/index.html",
    "<!DOCTYPE html>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"vendor/qunit.css\" type=\"text/css\" media=\"screen\">\n" +
    "  <script src=\"vendor/jquery.js\"></script>\n" +
    "  <script src=\"vendor/qunit.js\"></script>\n" +
    "  <script src=\"vendor/jslitmus.js\"></script>\n" +
    "  <script src=\"../underscore.js\"></script>\n" +
    "  <script src=\"../../lib/underscore.string.js\"></script>\n" +
    "\n" +
    "  <script src=\"collections.js\"></script>\n" +
    "  <script src=\"arrays.js\"></script>\n" +
    "  <script src=\"functions.js\"></script>\n" +
    "  <script src=\"objects.js\"></script>\n" +
    "  <script src=\"utility.js\"></script>\n" +
    "  <script src=\"chaining.js\"></script>\n" +
    "  <script src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <div id=\"qunit\"></div>\n" +
    "  <div id=\"qunit-fixture\">\n" +
    "    <div id=\"map-test\">\n" +
    "      <div id=\"id1\"></div>\n" +
    "      <div id=\"id2\"></div>\n" +
    "    </div>\n" +
    "  </div>\n" +
    "  <br>\n" +
    "  <h1 class=\"qunit-header\">Underscore Speed Suite</h1>\n" +
    "  <p>\n" +
    "    A representative sample of the functions are benchmarked here, to provide\n" +
    "    a sense of how fast they might run in different browsers.\n" +
    "    Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "    For example, the 'intersection' test measures the number of times you can\n" +
    "    find the intersection of two thousand-element arrays in one second.\n" +
    "  </p>\n" +
    "  <br>\n" +
    "  <script type=\"text/html\" id=\"template\">\n" +
    "    <%\n" +
    "    // a comment\n" +
    "    if (data) { data += 12345; }; %>\n" +
    "    <li><%= data %></li>\n" +
    "  </script>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/html-minifier/node_modules/uglify-js/tools/props.html",
    "<html>\n" +
    "  <head>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "    <script>(function(){\n" +
    "      var props = {};\n" +
    "\n" +
    "      function addObject(obj) {\n" +
    "        if (obj == null) return;\n" +
    "        try {\n" +
    "          Object.getOwnPropertyNames(obj).forEach(add);\n" +
    "        } catch(ex) {}\n" +
    "        if (obj.prototype) {\n" +
    "          Object.getOwnPropertyNames(obj.prototype).forEach(add);\n" +
    "        }\n" +
    "        if (typeof obj == \"function\") {\n" +
    "          try {\n" +
    "            Object.getOwnPropertyNames(new obj).forEach(add);\n" +
    "          } catch(ex) {}\n" +
    "        }\n" +
    "      }\n" +
    "\n" +
    "      function add(name) {\n" +
    "        props[name] = true;\n" +
    "      }\n" +
    "\n" +
    "      Object.getOwnPropertyNames(window).forEach(function(thing){\n" +
    "        addObject(window[thing]);\n" +
    "      });\n" +
    "\n" +
    "      try {\n" +
    "        addObject(new Event(\"click\"));\n" +
    "        addObject(new Event(\"contextmenu\"));\n" +
    "        addObject(new Event(\"mouseup\"));\n" +
    "        addObject(new Event(\"mousedown\"));\n" +
    "        addObject(new Event(\"keydown\"));\n" +
    "        addObject(new Event(\"keypress\"));\n" +
    "        addObject(new Event(\"keyup\"));\n" +
    "      } catch(ex) {}\n" +
    "\n" +
    "      var ta = document.createElement(\"textarea\");\n" +
    "      ta.style.width = \"100%\";\n" +
    "      ta.style.height = \"20em\";\n" +
    "      ta.style.boxSizing = \"border-box\";\n" +
    "      <!-- ta.value = Object.keys(props).sort(cmp).map(function(name){ -->\n" +
    "      <!--   return JSON.stringify(name); -->\n" +
    "      <!-- }).join(\",\\n\"); -->\n" +
    "      ta.value = JSON.stringify({\n" +
    "        vars: [],\n" +
    "        props: Object.keys(props).sort(cmp)\n" +
    "      }, null, 2);\n" +
    "      document.body.appendChild(ta);\n" +
    "\n" +
    "      function cmp(a, b) {\n" +
    "        a = a.toLowerCase();\n" +
    "        b = b.toLowerCase();\n" +
    "        return a < b ? -1 : a > b ? 1 : 0;\n" +
    "      }\n" +
    "    })();</script>\n" +
    "  </body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/jade/block-code.html",
    "");
  $templateCache.put("../node_modules/qs/test/browser/index.html",
    "<html>\n" +
    "  <head>\n" +
    "    <title>Mocha</title>\n" +
    "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">\n" +
    "    <link rel=\"stylesheet\" href=\"mocha.css\" />\n" +
    "    <script src=\"jquery.js\" type=\"text/javascript\"></script>\n" +
    "    <script src=\"expect.js\"></script>\n" +
    "    <script src=\"mocha.js\"></script>\n" +
    "    <script>mocha.setup('bdd')</script>\n" +
    "    <script src=\"qs.js\"></script>\n" +
    "    <script src=\"../parse.js\"></script>\n" +
    "    <script src=\"../stringify.js\"></script>\n" +
    "    <script>onload = mocha.run;</script>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "    <div id=\"mocha\"></div>\n" +
    "  </body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/uglify-js/tools/props.html",
    "<html>\n" +
    "  <head>\n" +
    "  </head>\n" +
    "  <body>\n" +
    "    <script>(function(){\n" +
    "      var props = {};\n" +
    "\n" +
    "      function addObject(obj) {\n" +
    "        if (obj == null) return;\n" +
    "        try {\n" +
    "          Object.getOwnPropertyNames(obj).forEach(add);\n" +
    "        } catch(ex) {}\n" +
    "        if (obj.prototype) {\n" +
    "          Object.getOwnPropertyNames(obj.prototype).forEach(add);\n" +
    "        }\n" +
    "        if (typeof obj == \"function\") {\n" +
    "          try {\n" +
    "            Object.getOwnPropertyNames(new obj).forEach(add);\n" +
    "          } catch(ex) {}\n" +
    "        }\n" +
    "      }\n" +
    "\n" +
    "      function add(name) {\n" +
    "        props[name] = true;\n" +
    "      }\n" +
    "\n" +
    "      Object.getOwnPropertyNames(window).forEach(function(thing){\n" +
    "        addObject(window[thing]);\n" +
    "      });\n" +
    "\n" +
    "      try {\n" +
    "        addObject(new Event(\"click\"));\n" +
    "        addObject(new Event(\"contextmenu\"));\n" +
    "        addObject(new Event(\"mouseup\"));\n" +
    "        addObject(new Event(\"mousedown\"));\n" +
    "        addObject(new Event(\"keydown\"));\n" +
    "        addObject(new Event(\"keypress\"));\n" +
    "        addObject(new Event(\"keyup\"));\n" +
    "      } catch(ex) {}\n" +
    "\n" +
    "      var ta = document.createElement(\"textarea\");\n" +
    "      ta.style.width = \"100%\";\n" +
    "      ta.style.height = \"20em\";\n" +
    "      ta.style.boxSizing = \"border-box\";\n" +
    "      <!-- ta.value = Object.keys(props).sort(cmp).map(function(name){ -->\n" +
    "      <!--   return JSON.stringify(name); -->\n" +
    "      <!-- }).join(\",\\n\"); -->\n" +
    "      ta.value = JSON.stringify({\n" +
    "        vars: [],\n" +
    "        props: Object.keys(props).sort(cmp)\n" +
    "      }, null, 2);\n" +
    "      document.body.appendChild(ta);\n" +
    "\n" +
    "      function cmp(a, b) {\n" +
    "        a = a.toLowerCase();\n" +
    "        b = b.toLowerCase();\n" +
    "        return a < b ? -1 : a > b ? 1 : 0;\n" +
    "      }\n" +
    "    })();</script>\n" +
    "  </body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/underscore.string/test/test.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <meta charset=\"utf-8\">\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jslitmus.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"underscore.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "  <br />\n" +
    "  <h1 class=\"qunit-header\">Underscore.string Speed Suite</h1>\n" +
    "  <!-- <h2 class=\"qunit-userAgent\">\n" +
    "    A representative sample of the functions are benchmarked here, to provide\n" +
    "    a sense of how fast they might run in different browsers.\n" +
    "    Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "    For example, the 'intersect' test measures the number of times you can\n" +
    "    find the intersection of two thousand-element arrays in one second.\n" +
    "  </h2> -->\n" +
    "  <br />\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/underscore.string/test/test_standalone.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore.strings Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"test_underscore/vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"test_underscore/vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"strings_standalone.js\"></script>\n" +
    "\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 id=\"qunit-header\">Underscore.string Test Suite</h1>\n" +
    "  <h2 id=\"qunit-banner\"></h2>\n" +
    "  <h2 id=\"qunit-userAgent\"></h2>\n" +
    "  <ol id=\"qunit-tests\"></ol>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/underscore.string/test/test_underscore/temp_tests.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore Temporary Tests</title>\n" +
    "  <link rel=\"stylesheet\" href=\"vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"vendor/jslitmus.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../underscore.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"temp.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <h1 class=\"qunit-header\">Underscore Temporary Tests</h1>\n" +
    "  <h2 class=\"qunit-userAgent\">\n" +
    "    A page for temporary speed tests, used for developing faster implementations\n" +
    "    of existing Underscore methods.\n" +
    "  </h2>\n" +
    "  <br />\n" +
    "</body>\n" +
    "</html>\n" +
    "");
  $templateCache.put("../node_modules/underscore.string/test/test_underscore/test.html",
    "<!DOCTYPE HTML>\n" +
    "<html>\n" +
    "<head>\n" +
    "  <title>Underscore Test Suite</title>\n" +
    "  <link rel=\"stylesheet\" href=\"vendor/qunit.css\" type=\"text/css\" media=\"screen\" />\n" +
    "  <script type=\"text/javascript\" src=\"vendor/jquery.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"vendor/qunit.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"vendor/jslitmus.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../underscore.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"../../lib/underscore.string.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"collections.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"arrays.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"functions.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"objects.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"utility.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"chaining.js\"></script>\n" +
    "  <script type=\"text/javascript\" src=\"speed.js\"></script>\n" +
    "</head>\n" +
    "<body>\n" +
    "  <div class=\"underscore-test\">\n" +
    "    <h1 id=\"qunit-header\">Underscore Test Suite</h1>\n" +
    "    <h2 id=\"qunit-banner\"></h2>\n" +
    "    <h2 id=\"qunit-userAgent\"></h2>\n" +
    "    <ol id=\"qunit-tests\"></ol>\n" +
    "    <br />\n" +
    "    <h1 class=\"qunit-header\">Underscore Speed Suite</h1>\n" +
    "    <p>\n" +
    "      A representative sample of the functions are benchmarked here, to provide\n" +
    "      a sense of how fast they might run in different browsers.\n" +
    "      Each iteration runs on an array of 1000 elements.<br /><br />\n" +
    "      For example, the 'intersect' test measures the number of times you can\n" +
    "      find the intersection of two thousand-element arrays in one second.\n" +
    "    </p>\n" +
    "    <br />\n" +
    "\n" +
    "    <script type=\"text/html\" id=\"template\">\n" +
    "      <%\n" +
    "      if (data) { data += 12345; }; %>\n" +
    "      <li><%= data %></li>\n" +
    "    </script>\n" +
    "  </div>\n" +
    "</body>\n" +
    "</html>\n" +
    "");
}]);
