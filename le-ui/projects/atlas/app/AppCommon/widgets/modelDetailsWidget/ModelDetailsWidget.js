angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.models.controllers.ModelDetailController',
    'lp.ratingsengine'
])
    .controller('ModelDetailsWidgetController', function (
        $stateParams, $scope, $rootScope, ResourceUtility, DateTimeFormatUtility,
        NavUtility, StringUtility, ModelStore, ModelService, RatingsEngineStore
    ) {
        $scope.ResourceUtility = ResourceUtility;
        var data = ModelStore.data;

        if (data == undefined) {

            var ratingEngine = $scope.RatingEngine,
                dashboard = ModelStore.getDashboardData();

            $scope.IsRuleBased = ratingEngine.type === 'RULE_BASED' ? true : false;
            $scope.IsCustomEvent = ratingEngine.type === 'CUSTOM_EVENT' ? true : false;
            $scope.IsRatingEngine = true;
            $scope.viewingIteration = false;
            $scope.type = ratingEngine.type;
            $scope.displayName = ratingEngine.displayName;
            $scope.createdBy = ratingEngine.createdBy;
            $scope.created = ratingEngine.created;
            $scope.lastRefreshedDate = ratingEngine.lastRefreshedDate;
            $scope.segmentName = ratingEngine.segment ? ratingEngine.segment.display_name : 'No segment selected';
            $scope.scorableAccounts = undefined;

            if ($scope.IsRuleBased || $scope.IsCustomEvent) {
                if ($scope.IsRuleBased) {
                    $scope.typeContext = 'rule';
                } else {
                    $scope.typeContext = 'AI';
                }
                $scope.modelingStrategy = ratingEngine.type;
                $scope.scorableAccounts = ratingEngine.segment ? ratingEngine.segment.accounts : 0;
            } else {
                var type = ratingEngine.type.toLowerCase();
                $scope.typeContext = 'AI';
                $scope.modelingStrategy = ratingEngine.latest_iteration.AI.advancedModelingConfig[type].modelingStrategy;

                if (ratingEngine.segment) {
                    RatingsEngineStore.getScorableAccounts(ratingEngine, ratingEngine.id, ratingEngine.latest_iteration.AI.id).then(function (result) {
                        $scope.scorableAccounts = result;
                    });
                } else {
                    $scope.scorableAccounts = 0;
                }
            }

            console.log($scope.scorableAccounts);

            $scope.segmentAccounts = ratingEngine.segment.accounts.toLocaleString('en');
            $scope.segmentAccountsString = "(" + $scope.segmentAccounts + " total accounts)";

            if ($scope.typeContext == 'AI') {

                $scope.totalIterations = dashboard.iterations.length;

                if (ratingEngine.published_iteration || ratingEngine.scoring_iteration) {
                    $scope.model = ratingEngine.published_iteration ? ratingEngine.published_iteration.AI : ratingEngine.scoring_iteration.AI;
                } else {
                    $scope.model = ratingEngine.latest_iteration.AI;
                }

                $scope.viewingIteration = $stateParams.viewingIteration ? true : false;


                $scope.expectedValueModel = $scope.model.predictionType === 'EXPECTED_VALUE' ? true : false;
                if ($scope.expectedValueModel) {
                    // $scope.averageRevenue = false;
                    $scope.averageRevenue = data && data.ModelDetails.AverageRevenue ? data.ModelDetails.AverageRevenue : false;
                }

                var engineId = $stateParams.rating_id,
                    ratingModelId = $scope.model.id;

                RatingsEngineStore.getRatingModel(engineId, ratingModelId).then(function (iteration) {
                    $scope.iteration = iteration.AI;

                    if ($scope.viewingIteration) {
                        $scope.modelHealthScore = data.ModelDetails.RocScore;
                        $scope.createdBy = iteration.AI.createdBy;
                        $scope.created = iteration.AI.created;
                    }
                });
            }


            $scope.activeIteration = ratingEngine.scoring_iteration ? ratingEngine.scoring_iteration[$scope.typeContext].iteration : ratingEngine.latest_iteration[$scope.typeContext].iteration;

            // Check if the sidebar nav should be enabled/disabled in /model/ pages
            if (ratingEngine.published_iteration != null && ratingEngine.published_iteration != undefined) {
                $scope.modelIsReady = true;

            } else if (ratingEngine.scoring_iteration != null && ratingEngine.scoring_iteration != undefined) {
                $scope.modelIsReady = ratingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != null && ratingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != undefined ? true : false;

            } else if (ratingEngine.latest_iteration != null && ratingEngine.latest_iteration != undefined) {
                // If only one iteration has been created, check it's status.
                if (dashboard.iterations.length == 1) {
                    $scope.modelIsReady = dashboard.iterations.length == 1 && (
                        ratingEngine.latest_iteration[$scope.typeContext].modelSummaryId != null && 
                        ratingEngine.latest_iteration[$scope.typeContext].modelSummaryId != undefined && 
                        (
                            ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Failed' && 
                            ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Pending' && 
                            ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Running'
                        )
                    ) ? true : false;
                } else if (dashboard.iterations.length > 1) {
                    // If multiple iterations have been created, at least one is completed, but have not been scored or published
                    var iterations = dashboard.iterations,
                        hasCompletedIteration = iterations.filter(iteration => (iteration.modelingJobStatus === "Completed"));
                    $scope.modelIsReady = hasCompletedIteration ? true : false;
                }
            }

            $scope.activeStatus = ratingEngine.status;

            $scope.$on('statusChange', function (event, args) {
                $scope.activeStatus = args.activeStatus;
                $scope.activeIteration = args.activeIteration;
            });

        } else {

            var widgetConfig = ModelStore.widgetConfig;
            var metadata = ModelStore.metadata;
            var dashboard = ModelStore.getDashboardData();
            var modelDetails = data.ModelDetails;

            $scope.displayName = modelDetails[widgetConfig.NameProperty];
            $scope.IsPmml = data.IsPmml;
            $scope.IsRatingEngine = (modelDetails.Name.substring(0, 2) === 'ai');

            if ($scope.IsRatingEngine) {
                var engineId = $stateParams.rating_id,
                    ratingEngine = RatingsEngineStore.getRatingEngine(),
                    type = ratingEngine.type
                        ? ratingEngine.type.toLowerCase()
                        : 'ai';

                $scope.IsRuleBased = ratingEngine.type === 'RULE_BASED' ? true : false;
                $scope.IsCustomEvent = ratingEngine.type === 'CUSTOM_EVENT' ? true : false;

                $scope.$on('statusChange', function (event, args) {
                    $scope.activeStatus = args.activeStatus;
                    $scope.activeIteration = args.activeIteration;
                });

                $scope.modelHealthScore = data.ModelDetails.RocScore;
                $scope.totalIterations = dashboard.iterations.length;
                $scope.externalAttributeCount = data.ExternalAttributes.total;
                $scope.internalAttributeCount = data.InternalAttributes.total;
                $scope.displayName = ratingEngine.displayName;
                $scope.createdBy = ratingEngine.createdBy;
                $scope.created = ratingEngine.created;

                if ($scope.IsRuleBased || $scope.IsCustomEvent) {
                    if ($scope.IsRuleBased) {
                        $scope.typeContext = 'rule';
                    } else {
                        $scope.typeContext = 'AI';
                    }
                    $scope.modelingStrategy = ratingEngine.type;
                    $scope.scorableAccounts = ratingEngine.segment ? ratingEngine.segment.accounts : 0;
                } else {
                    var type = ratingEngine.type.toLowerCase();
                    $scope.typeContext = 'AI';
                    $scope.modelingStrategy = ratingEngine.latest_iteration.AI.advancedModelingConfig[type].modelingStrategy;

                    if (ratingEngine.segment) {
                        RatingsEngineStore.getScorableAccounts(ratingEngine, ratingEngine.id, ratingEngine.latest_iteration.AI.id).then(function (result) {
                            $scope.scorableAccounts = result;
                        });
                    } else {
                        $scope.scorableAccounts = 0;
                    }
                }

                console.log($scope.scorableAccounts);

                $scope.segmentAccounts = ratingEngine.segment.accounts.toLocaleString('en');
                $scope.segmentAccountsString = "(" + $scope.segmentAccounts + " total accounts)";

                if ($scope.typeContext == 'AI') {

                    var engineId = $stateParams.rating_id,
                        ratingModelId = data.ModelDetails.Name;

                    if (ratingEngine.published_iteration || ratingEngine.scoring_iteration) {
                        $scope.model = ratingEngine.published_iteration ? ratingEngine.published_iteration.AI : ratingEngine.scoring_iteration.AI;
                    } else {
                        $scope.model = ratingEngine.latest_iteration.AI;
                    }

                    $scope.viewingIteration = $stateParams.viewingIteration ? true : false;

                    $scope.expectedValueModel = $scope.model.predictionType === 'EXPECTED_VALUE' ? true : false;
                    if ($scope.expectedValueModel) {
                        $scope.averageRevenue = data.ModelDetails.AverageRevenue ? data.ModelDetails.AverageRevenue : false;
                    }

                    RatingsEngineStore.getRatingModel(engineId, ratingModelId).then(function (iteration) {
                        $scope.iteration = iteration.AI;

                        if ($scope.viewingIteration) {
                            $scope.modelHealthScore = data.ModelDetails.RocScore;
                            $scope.createdBy = iteration.AI.createdBy;
                            $scope.created = iteration.AI.created;
                        }
                    });
                }

                $scope.activeIteration = ratingEngine.scoring_iteration ? ratingEngine.scoring_iteration[$scope.typeContext].iteration : ratingEngine.latest_iteration[$scope.typeContext].iteration;

                // Check if the sidebar nav should be enabled/disabled in /model/ pages
                if (ratingEngine.published_iteration != null && ratingEngine.published_iteration != undefined) {
                    $scope.modelIsReady = true;

                } else if (ratingEngine.scoring_iteration != null && ratingEngine.scoring_iteration != undefined) {
                    $scope.modelIsReady = ratingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != null && ratingEngine.scoring_iteration[$scope.typeContext].modelSummaryId != undefined ? true : false;

                } else if (ratingEngine.latest_iteration != null && ratingEngine.latest_iteration != undefined) {
                    // If only one iteration has been created, check it's status.
                    if (dashboard.iterations.length == 1) {
                        $scope.modelIsReady = dashboard.iterations.length == 1 && (
                            ratingEngine.latest_iteration[$scope.typeContext].modelSummaryId != null && 
                            ratingEngine.latest_iteration[$scope.typeContext].modelSummaryId != undefined && 
                            (
                                ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Failed' && 
                                ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Pending' && 
                                ratingEngine.latest_iteration[$scope.typeContext].modelingJobStatus != 'Running'
                            )
                        ) ? true : false;
                    } else if (dashboard.iterations.length > 1) {
                        // If multiple iterations have been created, at least one is completed, but have not been scored or published
                        var iterations = dashboard.iterations,
                            hasCompletedIteration = iterations.filter(iteration => (iteration.modelingJobStatus === "Completed"));
                        $scope.modelIsReady = hasCompletedIteration ? true : false;
                    }
                }

                $scope.lastRefreshedDate = ratingEngine.lastRefreshedDate;
                $scope.activeStatus = ratingEngine.status;

                $scope.segmentName = ratingEngine.segment ? ratingEngine.segment.display_name : 'No segment selected';

            }

            var isActive = modelDetails[widgetConfig.StatusProperty] == 'Active';

            if (isActive) {
                $scope.status = ResourceUtility.getString("MODEL_DETAILS_ACTIVE_LABEL");
            } else {
                $scope.status = ResourceUtility.getString("MODEL_DETAILS_INACTIVE_LABEL");
            }

            $scope.modelType = modelDetails[widgetConfig.TypeProperty];


            // LPI "Model" modelType options
            // ----------------------
            // SalesforceLead
            // SalesforceAccount

            // CDL "Rating Engine" modelingStrategy options & modelingType options
            // ----------------------
            // CROSS_SELL_FIRST_PURCHASE SalesforceAccount
            // CROSS_SELL_REPEAT_PURCHASE SalesforceAccount


            if ($scope.modelType == 'SalesforceAccount') {
                $scope.modelTypeLabel = ResourceUtility.getString("MODEL_DETAILS_ACCOUNTS_TITLE");
            } else {
                $scope.modelTypeLabel = ResourceUtility.getString("MODEL_DETAILS_LEADS_TITLE");
            }


            $scope.score = modelDetails[widgetConfig.ScoreProperty];
            if ($scope.score != null && $scope.score < 1) {
                $scope.score = Math.round($scope.score * 100);
            }

            //data.TopSample = ModelService.FormatLeadSampleData(data.TopSample);
            if (data.ExternalAttributes) {
                $scope.externalAttributes = data.ExternalAttributes.total;
                $scope.externalAttributes = StringUtility.AddCommas($scope.externalAttributes);
                $scope.totalExternalPredictors = data.ExternalAttributes.totalAttributeValues;

                $scope.internalAttributes = data.InternalAttributes.total;
                $scope.internalAttributes = StringUtility.AddCommas($scope.internalAttributes);
                $scope.totalInternalPredictors = data.InternalAttributes.totalAttributeValues;
            }
            $scope.createdDate = modelDetails[widgetConfig.CreatedDateProperty];
            $scope.createdDate = $scope.createdDate * 1000;
            $scope.createdDate = DateTimeFormatUtility.FormatShortDate($scope.createdDate);
            $scope.totalLeads = modelDetails[widgetConfig.TotalLeadsProperty];
            $scope.totalLeads = StringUtility.AddCommas($scope.totalLeads);

            $scope.testSet = modelDetails[widgetConfig.TestSetProperty];
            $scope.testSet = StringUtility.AddCommas($scope.testSet);

            $scope.trainingSet = modelDetails[widgetConfig.TrainingSetProperty];
            $scope.trainingSet = StringUtility.AddCommas($scope.trainingSet);

            $scope.totalSuccessEvents = modelDetails[widgetConfig.TotalSuccessEventsProperty];
            $scope.totalSuccessEvents = StringUtility.AddCommas($scope.totalSuccessEvents);

            $scope.conversionRate = modelDetails[widgetConfig.TotalSuccessEventsProperty] / (modelDetails[widgetConfig.TestSetProperty] + modelDetails[widgetConfig.TrainingSetProperty]);
            if ($scope.conversionRate != null && $scope.conversionRate < 1) {
                $scope.conversionRate = $scope.conversionRate * 100;
                $scope.conversionRate = $scope.conversionRate.toFixed(2);
            } else if ($scope.conversionRate != null && $scope.conversionRate === 1) {
                $scope.conversionRate = $scope.conversionRate * 100;
                $scope.conversionRate = $scope.conversionRate.toFixed(0);
            }
            $scope.leadSource = modelDetails[widgetConfig.LeadSourceProperty];
            $scope.opportunity = modelDetails[widgetConfig.OpportunityProperty];

            $scope.dataExpanded = false;
            $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON');
            $("#moreDataPoints").hide();

            $scope.backButtonClick = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }

                $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT);
            };

            $scope.showMoreClicked = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }
                $scope.dataExpanded = !$scope.dataExpanded;
                if ($scope.dataExpanded) {
                    $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_LESS_BUTTON');
                } else {
                    $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON');
                }

                $("#moreDataPoints").slideToggle('400');
            };
        }

    })
    .directive('modelDetailsWidget', function ($compile) {
        var directiveDefinitionObject = {
            templateUrl: 'app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html'
        };

        return directiveDefinitionObject;
    });