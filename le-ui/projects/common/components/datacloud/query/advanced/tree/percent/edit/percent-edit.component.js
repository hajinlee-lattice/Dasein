angular
	.module("common.datacloud.query.builder.tree.edit.percent.edit", [])
	.component("percentEditAttribute", {
		templateUrl:
			"/components/datacloud/query/advanced/tree/percent/edit/percent-edit.component.html",

		bindings: {
			type: "<",
			bucketrestriction: "=",
			form: "=",
			vm: "="
		},

		controller: function(QueryTreeService, PercentStore, $timeout) {
			var self = this;

			function changePeriodView() {
				switch (self.periodModel) {
					case "AS_MUCH_AS": {
						self.showFromPeriod = false;
						self.showToPeriod = true;
						break;
					}
					case "AT_LEAST": {
						self.showFromPeriod = true;
						self.showToPeriod = false;
						break;
					}
					case "BETWEEN": {
						self.showFromPeriod = true;
						self.showToPeriod = true;
						break;
					}
					default: {
						self.showFromPeriod = false;
						self.showToPeriod = false;
					}
				}
			}

			function initValues() {
				var fromTmp = PercentStore.getVal(
					self.periodModel,
					self.bucketrestriction.bkt,
					0
				);
				var toTmp = PercentStore.getVal(
					self.periodModel,
					self.bucketrestriction.bkt,
					1
				);
				self.numericalConfig.from.value =
					fromTmp != null ? Number(fromTmp) : undefined;
				self.numericalConfig.to.value =
					toTmp != null ? Number(toTmp) : undefined;
			}

			function resetValues() {
				self.showFromPeriod = false;
				self.showToPeriod = false;
				PercentStore.restValues(self.bucketrestriction.bkt);
				initValues();
			}

			this.$onInit = function() {
				this.periodChanging = false;
				this.showFromPeriod = false;
				this.showToPeriod = false;
				this.directionList = PercentStore.getDirectionList();
				this.periodList = PercentStore.getCmpList();
				this.numericalConfig = PercentStore.getNumericalPeriodConfig();
				this.periodModel = this.bucketrestriction.bkt.Chg.Cmp;
				this.directionModel = PercentStore.getDirection(
					this.bucketrestriction.bkt
				);
				changePeriodView();
				initValues();
			};

			this.getPeriodNumericalConfString = function() {
				if (!this.numericalConfig) {
					this.numericalConfig = PercentStore.getNumericalPeriodConfig();
				}
				return this.numericalConfig;
			};

			this.callbackChangedValue = function(type, position, value) {
				PercentStore.changeValue(
					this.periodModel,
					this.bucketrestriction.bkt.Chg.Vals,
					position,
					value
				);
			};

			this.changeDirection = function() {
				PercentStore.setDirection(
					this.bucketrestriction.bkt,
					this.directionModel
				);
			};

			this.changePeriod = function() {
				this.periodChanging = true;
				changePeriodView();
				PercentStore.changeCmp(
					this.bucketrestriction.bkt,
					this.periodModel
				);
				resetValues();
				$timeout(function() {
					changePeriodView();
					self.periodChanging = false;
				}, 0);
			};

			this.isPeriodRangeValid = function() {
				return PercentStore.isPeriodRangeValid(this.form);
			};

			this.getErrorMsg = function() {
				return PercentStore.getErrorMsg(
					this.showFromPeriod,
					this.showToPeriod
				);
			};
		}
	});
