angular
	.module("lp.ratingsengine.wizard.training", [
		"mainApp.appCommon.directives.chips",
		"mainApp.appCommon.directives.formOnChange",
		"common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range"
	])
	.component("ratingsEngineAITraining", {
		templateUrl:
			"app/ratingsengine/content/training/training.component.html",
		bindings: {
			ratingEngine: "<",
			segments: "<",
			products: "<",
			datacollectionstatus: "<",
			iteration: "<",
			attributes: "<"
		},
		controller: function(
			$q,
			$scope,
			$state,
			$stateParams,
			$timeout,
			RatingsEngineStore,
			RatingsEngineService,
			DataCloudStore,
			SegmentService,
			Banner
		) {
			var vm = this;

			angular.extend(vm, {
				viewingIteration: $stateParams.viewingIteration,
                disableRemodelButton: true,
				spendCriteria: "GREATER_OR_EQUAL",
				spendValue: 1500,
				quantityCriteria: "GREATER_OR_EQUAL",
				quantityValue: 2,
				periodsCriteria: "WITHIN",
				periodsValue: 2,
				configFilters: angular.copy(
					RatingsEngineStore.getConfigFilters()
				),
				trainingSegment: null,
				trainingProducts: null,
				scoringCountReturned: false,
				recordsCountReturned: false,
				purchasesCountReturned: false,
				pageTitle:
					$stateParams.section === "wizard.ratingsengine_segment"
						? "How do you want to train the model?"
						: "Do you want to change the way the model is trained?",
				checkboxModel: {},
				repeatPurchaseRemodel: false
			});

			vm.getNumericalConfig = function() {
				var config = { debounce: 800 };
				// var ret = JSON.stringify(config);
				return config;
			};

			vm.$onInit = function() {
				vm.ratingModel = vm.iteration
					? vm.iteration.AI
					: vm.ratingEngine.latest_iteration.AI;
				vm.engineType = vm.ratingEngine.type.toLowerCase();
				vm.periodType = vm.datacollectionstatus.ApsRollingPeriod
					? vm.datacollectionstatus.ApsRollingPeriod.toLowerCase()
					: "quarter";
				vm.spendingConfig = {
					from: {
						name: "from-spend",
						value: vm.spendValue,
						position: 0,
						type: "Spend",
						min: "0",
						max: "2147483647"
					},
					to: {
						name: "to-spend",
						value: vm.spendValue,
						position: 1,
						type: "Spend",
						min: "0",
						max: "2147483647"
					}
				};
				vm.quantityConfig = {
					from: {
						name: "from-quantity",
						value: vm.quantityValue,
						position: 0,
						type: "Quantity",
						min: "0",
						max: "2147483647",
						disabled: true,
						visible: true
					},
					to: {
						name: "to-quantity",
						value: vm.quantityValue,
						position: 1,
						type: "Quantity",
						min: "0",
						max: "2147483647",
						disbaled: true,
						visible: false
					}
				};

				vm.periodConfig = {
					from: {
						name: "from-period",
						value: vm.periodsValue,
						position: 0,
						type: "Period",
						min: "0",
						max: "2147483647"
					},
					to: {
						name: "to-period",
						value: vm.periodsValue,
						position: 1,
						type: "Period",
						min: "0",
						max: "2147483647"
					}
				};
				vm.numericalConfig = { debounce: 800 };
				if ($stateParams.section != "wizard.ratingsengine_segment") {
					if (vm.engineType == "cross_sell") {
						vm.filters = angular.copy(
							vm.ratingModel.advancedModelingConfig.cross_sell
								.filters
						);

						vm.repeatPurchase =
							vm.ratingEngine.advancedRatingConfig.cross_sell
								.modelingStrategy ===
							"CROSS_SELL_REPEAT_PURCHASE"
								? true
								: false;
						if (vm.repeatPurchase) {
							vm.purchasedBeforePeriod = vm.filters
								.PURCHASED_BEFORE_PERIOD
								? vm.filters.PURCHASED_BEFORE_PERIOD.value
								: 6;
							vm.repeatPurchaseRemodel = true;
						}

						vm.trainingSegment = vm.ratingModel.trainingSegment
							? vm.ratingModel.trainingSegment
							: null;

						vm.trainingProducts = vm.ratingModel
							.advancedModelingConfig.cross_sell.trainingProducts
							? vm.ratingModel.advancedModelingConfig.cross_sell
									.trainingProducts
							: null;

						// Setup form for Cross Sell Models
						vm.checkboxModel = {
							spend: vm.filters.SPEND_IN_PERIOD ? true : false,
							quantity: vm.filters.QUANTITY_IN_PERIOD
								? true
								: false,
							periods: vm.filters.TRAINING_SET_PERIOD
								? true
								: false
						};

						vm.spendCriteria = vm.filters.SPEND_IN_PERIOD
							? vm.filters.SPEND_IN_PERIOD.criteria
							: "GREATER_OR_EQUAL";
						vm.spendValue = vm.filters.SPEND_IN_PERIOD
							? vm.filters.SPEND_IN_PERIOD.value
							: 1500;

						vm.quantityCriteria = vm.filters.QUANTITY_IN_PERIOD
							? vm.filters.QUANTITY_IN_PERIOD.criteria
							: "GREATER_OR_EQUAL";
						vm.quantityValue = vm.filters.QUANTITY_IN_PERIOD
							? vm.filters.QUANTITY_IN_PERIOD.value
							: 2;

						vm.periodsCriteria = vm.filters.TRAINING_SET_PERIOD
							? vm.filters.TRAINING_SET_PERIOD.criteria
							: "WITHIN";
						vm.periodsValue = vm.filters.TRAINING_SET_PERIOD
							? vm.filters.TRAINING_SET_PERIOD.value
							: 2;

						vm.validateCrossSellForm();
					} else {
						// Setup form for Custom Event Models
						vm.filters =
							vm.iteration.AI.advancedModelingConfig.custom_event;

						RatingsEngineStore.setDisplayFileName(
							vm.filters.sourceFileName
						);

						vm.checkboxModel = {
							datacloud:
								vm.filters.dataStores.indexOf("DataCloud") > -1
									? true
									: false,
							cdl:
								vm.filters.dataStores.indexOf("CDL") > -1
									? true
									: false,
							customFile:
								vm.filters.dataStores.indexOf(
									"CustomFileAttributes"
								) > -1
									? true
									: false,
							deduplicationType:
								vm.filters.deduplicationType ==
								"ONELEADPERDOMAIN"
									? true
									: false,
							excludePublicDomains:
								vm.filters.excludePublicDomains == true
									? false
									: true,
							transformationGroup:
								vm.filters.transformationGroup == "none"
									? false
									: true
						};

						vm.configFilters = angular.copy(vm.filters);
						vm.configFilters.dataStores = ["CDL", "DataCloud"];

						vm.checkForDisable();
						vm.validateCustomEventForm();
					}
				}

				vm.engineId = vm.ratingEngine.id;
				vm.modelId = vm.ratingModel.id;

				vm.modelingStrategy =
					vm.ratingModel.advancedModelingConfig[
						vm.engineType
					].modelingStrategy;

				if (vm.engineType == "cross_sell") {
					vm.getScoringCount(
						vm.engineId,
						vm.modelId,
						vm.ratingEngine
					);

                    if (!$stateParams.viewingIteration) {
                        RatingsEngineStore.setValidation("training", false);
                        RatingsEngineStore.setValidation("refine", false);

                        vm.getPurchasesCount(
                            vm.engineId,
                            vm.modelId,
                            vm.ratingEngine
                        );
                        vm.getRecordsCount(
                            vm.engineId,
                            vm.modelId,
                            vm.ratingEngine
                        );
                    }
				}
			};

			// Functions for Cross Sell Models
			// ============================================================================================
			vm.getRecordsCount = function(engineId, modelId, ratingEngine) {
				vm.recordsCountReturned = false;
				RatingsEngineStore.getTrainingCounts(
					engineId,
					modelId,
					ratingEngine,
					"TRAINING"
				).then(function(count) {
					vm.recordsCount = count;
					vm.recordsCountReturned = true;
				});
			};
			vm.getPurchasesCount = function(engineId, modelId, ratingEngine) {

                RatingsEngineStore.setValidation("training", false);
                vm.disableRemodelButton = true;
                vm.remodelingProgress = false;

				vm.purchasesCountReturned = false;
				RatingsEngineStore.getTrainingCounts(
					engineId,
					modelId,
					ratingEngine,
					"EVENT"
				).then(function(count) {
                    if (count >= 50) {
                        RatingsEngineStore.setValidation("training", true);
                        vm.disableRemodelButton = false;
                        vm.remodelingProgress = false;
                    }
					vm.purchasesCount = count;
					vm.purchasesCountReturned = true;
				});
			};
			vm.getScoringCount = function(engineId, modelId, ratingEngine) {
				vm.scoringCountReturned = false;
				RatingsEngineStore.getTrainingCounts(
					engineId,
					modelId,
					ratingEngine,
					"TARGET"
				).then(function(count) {
					vm.scoringCount = count;
					vm.scoringCountReturned = true;
				});
			};

			vm.segmentCallback = function(selectedSegment) {
				vm.trainingSegment = selectedSegment[0];
				RatingsEngineStore.setTrainingSegment(vm.trainingSegment);
				vm.ratingModel.trainingSegment = vm.trainingSegment;
				vm.updateSegmentSelected(vm.trainingSegment);
				vm.autcompleteChange();
			};
			vm.updateSegmentSelected = function(trainingSegment) {
				if (vm.ratingEngine.latest_iteration.AI) {
					vm.ratingEngine.latest_iteration.AI.trainingSegment = trainingSegment
						? trainingSegment
						: null;
				}
			};

			vm.productsCallback = function(selectedProducts) {
				vm.trainingProducts = [];
				angular.forEach(selectedProducts, function(product) {
					vm.trainingProducts.push(product.ProductId);
				});

				vm.trainingProducts =
					selectedProducts.length == 0 ? null : vm.trainingProducts;
				vm.updateProductsSelected(vm.trainingProducts);
				RatingsEngineStore.setTrainingProducts(vm.trainingProducts);
				vm.ratingModel.advancedModelingConfig.cross_sell.trainingProducts =
					vm.trainingProducts;

				vm.autcompleteChange();
			};
			vm.updateProductsSelected = function(listProducts) {
				if (vm.ratingEngine.latest_iteration.AI) {
					vm.ratingEngine.latest_iteration.AI.advancedModelingConfig.cross_sell.trainingProducts = listProducts;
				}
			};

			vm.autcompleteChange = function() {
				vm.getRecordsCount(vm.engineId, vm.modelId, vm.ratingEngine);
				vm.getPurchasesCount(vm.engineId, vm.modelId, vm.ratingEngine);
			};

			vm.getSpendConfig = function() {
				return {
					from: {
						name: "from-spend",
						value: vm.spendValue,
						position: 0,
						type: "Spend",
						min: "0",
						max: "2147483647"
					},
					to: {
						name: "to-spend",
						value: vm.spendValue,
						position: 1,
						type: "Spend",
						min: "0",
						max: "2147483647"
					}
				};
			};

			vm.getQuantityConfig = function() {
				return {
					from: {
						name: "from-quantity",
						value: vm.quantityValue,
						position: 0,
						type: "Quantity",
						min: "0",
						max: "2147483647",
						disabled: true,
						visible: true
					},
					to: {
						name: "to-quantity",
						value: vm.quantityValue,
						position: 1,
						type: "Quantity",
						min: "0",
						max: "2147483647",
						disbaled: true,
						visible: false
					}
				};
			};

			vm.getPeriodConfig = function() {
				return {
					from: {
						name: "from-period",
						value: vm.periodsValue,
						position: 0,
						type: "Period",
						min: "0",
						max: "2147483647"
					},
					to: {
						name: "to-period",
						value: vm.periodsValue,
						position: 1,
						type: "Period",
						min: "0",
						max: "2147483647"
					}
				};
			};

			vm.callbackSpend = function(type, position, value) {
				if (value) {
					vm.spendValue = value;
				}
				vm.formOnChange();
			};

			vm.callbackQuantity = function(type, position, value) {
				if (value) {
					vm.quantityValue = value;
				}
				vm.formOnChange();
			};

			vm.callbackPeriod = function(type, position, value) {
				if (value) {
					vm.periodsValue = value;
				}
				vm.formOnChange();
			};

			vm.validateCrossSellForm = function() {
				var valid = true;
				if ($scope.trainingForm) {
					valid = $scope.trainingForm.$valid;
				}

				if (valid == true) {
					// RatingsEngineStore.setValidation("training", true);
					// RatingsEngineStore.setValidation("refine", true);
					vm.recordsCountReturned = false;
					vm.purchasesCountReturned = false;

					if (vm.repeatPurchaseRemodel) {
						vm.configFilters.PURCHASED_BEFORE_PERIOD = {
							configName: "PURCHASED_BEFORE_PERIOD",
							criteria: "PRIOR_ONLY",
							value: vm.purchasedBeforePeriod
						};
						vm.ratingModel.advancedModelingConfig.cross_sell.filters =
							vm.configFilters;
					}

					if (vm.checkboxModel.spend) {
						vm.configFilters.SPEND_IN_PERIOD = {
							configName: "SPEND_IN_PERIOD",
							criteria: vm.spendCriteria,
							value: vm.spendValue
						};
					} else {
						delete vm.configFilters.SPEND_IN_PERIOD;
					}

					if (vm.checkboxModel.quantity) {
						vm.configFilters.QUANTITY_IN_PERIOD = {
							configName: "QUANTITY_IN_PERIOD",
							criteria: vm.quantityCriteria,
							value: vm.quantityValue
						};
					} else {
						delete vm.configFilters.QUANTITY_IN_PERIOD;
					}

					if (vm.checkboxModel.periods) {
						vm.configFilters.TRAINING_SET_PERIOD = {
							configName: "TRAINING_SET_PERIOD",
							criteria: vm.periodsCriteria,
							value: vm.periodsValue
						};
					} else {
						delete vm.configFilters.TRAINING_SET_PERIOD;
					}

					vm.ratingEngine.latest_iteration.AI.advancedModelingConfig.cross_sell.filters =
						vm.configFilters;

					// console.log(vm.ratingEngine);
					// console.log(vm.engineId, vm.modelId, vm.ratingModel);

					$timeout(function() {
						RatingsEngineStore.setConfigFilters(vm.configFilters);
						vm.getRecordsCount(
							vm.engineId,
							vm.modelId,
							vm.ratingEngine
						);
						vm.getPurchasesCount(
							vm.engineId,
							vm.modelId,
							vm.ratingEngine
						);
					}, 1500);
				} else {
					RatingsEngineStore.setValidation("training", false);
					RatingsEngineStore.setValidation("refine", false);
				}
			};
			// End of the functions for Cross Sell Models
			// ============================================================================================

			// Functions for Custom Event Models
			// ============================================================================================
			vm.checkForDisable = function() {
				RatingsEngineStore.setValidation("training", false);

				if (
					vm.checkboxModel.datacloud == false &&
					vm.checkboxModel.cdl == false
				) {
					RatingsEngineStore.setValidation("training", false);
				}
			};

			vm.validateCustomEventForm = function() {
				var valid = true;
				if ($scope.trainingForm) {
					valid = $scope.trainingForm.$valid;
				}

				if (valid == true) {
					// RatingsEngineStore.setConfigFilters(vm.configFilters);

					if (vm.checkboxModel.deduplicationType) {
						vm.configFilters.deduplicationType = "ONELEADPERDOMAIN";
					} else {
						vm.configFilters.deduplicationType =
							"MULTIPLELEADSPERDOMAIN";
					}

					vm.configFilters.excludePublicDomains = vm.checkboxModel
						.excludePublicDomains
						? false
						: true;

					if (!vm.checkboxModel.transformationGroup) {
						vm.configFilters.transformationGroup = "none";
					} else {
						vm.configFilters.transformationGroup = "all";
					}

					$timeout(function() {
						RatingsEngineStore.setConfigFilters(vm.configFilters);
						RatingsEngineStore.setValidation("training", true);
						vm.ratingModel.advancedModelingConfig.custom_event =
							vm.configFilters;
					}, 150);
				} else {
					RatingsEngineStore.setValidation("training", false);
					RatingsEngineStore.setValidation("refine", false);
				}
			};
			// End of the functions for Custom Event Models
			// ============================================================================================

			vm.formOnChange = function() {
                vm.disableRemodelButton = true;
                RatingsEngineStore.setValidation("training", false);

				$timeout(function() {
					if (vm.engineType === "cross_sell") {
						vm.validateCrossSellForm();
					} else {
						vm.validateCustomEventForm();
					}
				}, 1500);
			};

			vm.backToModel = function() {
				var modelId = vm.iteration.modelSummaryId,
					rating_id = $stateParams.rating_id;

				$state.go("home.ratingsengine.dashboard", {
					rating_id: rating_id,
					modelId: modelId
				});
			};

			vm.remodelIteration = function() {
				var engineId = vm.ratingEngine.id,
					modelId = vm.ratingModel.id,
					ratingModel = {
						AI: vm.ratingModel
					};

                vm.disableRemodelButton = true;
				vm.remodelingProgress = true;

				RatingsEngineStore.setRemodelIteration(ratingModel);
				RatingsEngineStore.setRatingEngine(vm.ratingEngine);
				RatingsEngineStore.saveIteration("training").then(function(
					result
				) {
					if (!result.result) {
						$state.go("home.ratingsengine.dashboard", {
							rating_id: engineId,
							modelId: "",
							viewingIteration: false,
							remodelSuccessBanner: true
						});
					}
					vm.remodelingProgress = result.showProgress;
				});
			};
		}
	});
