import "./date-attribute-edit.scss";
angular
	.module("common.datacloud.query.builder.tree.edit.date.attribute", [
		"common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range",
		"common.datacloud.query.builder.tree.edit.transaction.edit.date.range",
		"angularMoment",
		"common.datacloud.query.builder.tree.transaction.service"
	])
	.component("dateAttributeEdit", {
		templateUrl:
			"/components/datacloud/query/advanced/tree/date-attribute/date-attribute-edit.html",

		bindings: {
			type: "<",
			bucketrestriction: "=",
			form: "=",
			vm: "="
		},
		controller: function(QueryTreeService, QueryTreeDateAttributeStore) {
			this.$onInit = function() {
				this.timeCmp = QueryTreeService.getCmp(
					this.bucketrestriction,
					this.type,
					"Date"
				);
				this.timeframePeriod = QueryTreeService.getPeriodValue(
					this.bucketrestriction,
					this.type,
					"Date"
				);
				this.showTimeFrame = false;
				this.showPeriodSelect = false;
				this.showPeriodNumber = false;
				this.showFromPeriod = false;
				this.showToPeriod = false;
				this.showFromTime = false;
				this.showToTime = false;
				this.periodsList = QueryTreeDateAttributeStore.periodList();

				this.periodTimeConfig = {
					from: {
						name: "from-time",
						initial: QueryTreeDateAttributeStore.getVal(
							"Date",
							this.timeCmp,
							this.bucketrestriction.bkt,
							0
						),
						position: 0,
						type: "Date",
						visible: this.showFromTime,
						pattern: "\\d+",
						step: 1
					},
					to: {
						name: "to-time",
						initial: QueryTreeDateAttributeStore.getVal(
							"Date",
							this.timeCmp,
							this.bucketrestriction.bkt,
							1
						),
						position: 1,
						type: "Date",
						visible: this.showToTime,
						pattern: "\\d+",
						step: 1
					}
				};
				this.periodNumberConfig = {
					from: {
						name: "from-period",
						value: QueryTreeDateAttributeStore.getVal(
							"Numerical",
							this.timeCmp,
							this.bucketrestriction.bkt,
							0
						),
						position: 0,
						type: "Numerical",
						min: "0",
						max: "",
						pattern: "\\d*",
						step: 1
					},
					to: {
						name: "to-period",
						value: QueryTreeDateAttributeStore.getVal(
							"Numerical",
							this.timeCmp,
							this.bucketrestriction.bkt,
							1
						),
						position: 1,
						type: "Numerical",
						min: "0",
						max: "",
						pattern: "\\d*",
						step: 1
					}
				};

				this.cmpsList = [
					{ name: "EVER", displayName: "Ever" },
					{ name: "LATEST_DAY", displayName: "Latest Day" },
					{ name: "IN_CURRENT_PERIOD", displayName: "Current" },
					{ name: "WITHIN", displayName: "Previous" },
					{ name: "LAST", displayName: "Last" },
					{ name: "BETWEEN", displayName: "Between Last" },
					{ name: "BETWEEN_DATE", displayName: "Between" },
					{ name: "BEFORE", displayName: "Before" },
					{ name: "AFTER", displayName: "After" },
					{ name: "IS_EMPTY", displayName: "Is Empty" }
				];
				this.changeCmp(this.timeCmp, true);
			};

			this.changeCmp = function(value, init) {
				// console.log("CHANGED ", value);
				let initialPeriod = this.periodsList[0].name;
				this.showTimeFrame = false;
				this.showPeriodSelect = false;
				this.showPeriodNumber = false;
				this.showFromPeriod = false;
				this.showToPeriod = false;
				this.showFromTime = false;
				this.showToTime = false;
				setTimeout(() => {
					// let tmp = this.bucketrestriction.bkt.Fltr.Vals;
					let values = this.bucketrestriction.bkt.Fltr.Vals;
					if (!init) {
						this.periodNumberConfig.from.value = undefined;
						this.periodNumberConfig.to.value = undefined;
						this.periodTimeConfig.from.initial = undefined;
						this.periodTimeConfig.to.initial = undefined;
						values = [];
					}
					// let copy = values;
					switch (value) {
						case "EVER":
						case "IS_EMPTY":
						case "LATEST_DAY":
							QueryTreeDateAttributeStore.restValues(
								this.bucketrestriction.bkt
							);
							this.showTimeFrame = false;
							this.showPeriodSelect = false;
							this.showPeriodNumber = false;
							this.showFromPeriod = false;
							this.showToPeriod = false;
							this.showFromTime = false;
							this.showToTime = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								initialPeriod,
								values
							);
							this.resetTimeFramePeriod();
							break;
						case "IN_CURRENT_PERIOD":
							this.showTimeFrame = false;
							this.showPeriodSelect = true;
							this.showPeriodNumber = false;
							this.showFromPeriod = false;
							this.showToPeriod = false;
							this.showFromTime = false;
							this.showToTime = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								initialPeriod,
								values
							);
							this.resetTimeFramePeriod();
							break;
						case "WITHIN":
							this.showTimeFrame = false;
							this.showPeriodSelect = true;
							this.showPeriodNumber = true;
							this.showFromPeriod = true;
							this.showToPeriod = false;
							this.showFromTime = false;
							this.showToTime = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								initialPeriod,
								values
							);
							this.resetTimeFramePeriod();
							break;
						case "LAST":
							this.showTimeFrame = false;
							this.showPeriodSelect = false;
							this.showPeriodNumber = true;
							this.showFromPeriod = true;
							this.showToPeriod = false;
							this.showFromTime = false;
							this.showToTime = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								"Day",
								values
							);
							this.resetTimeFramePeriod();
							break;
						case "BETWEEN":
							this.showTimeFrame = false;
							this.showPeriodSelect = true;
							this.showPeriodNumber = true;
							this.showFromPeriod = true;
							this.showToPeriod = true;
							this.showFromTime = false;
							this.showToTime = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								initialPeriod,
								values
							);
							this.resetTimeFramePeriod();
							break;
						case "BETWEEN_DATE":
							this.showTimeFrame = true;
							this.showPeriodSelect = false;
							this.showPeriodNumber = false;
							this.showFromPeriod = false;
							this.showToPeriod = false;
							this.showFromTime = true;
							this.showToTime = true;
							this.periodTimeConfig.from.visible = true;
							this.periodTimeConfig.to.visible = true;

							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								"Date",
								values
							);
							// this.periodTimeConfig.from.initial = copy[0];
							// this.periodTimeConfig.to.initial = copy[1];
							this.resetTimeFramePeriod();
							break;

						case "BEFORE":
							this.showTimeFrame = true;
							this.showPeriodSelect = false;
							this.showPeriodNumber = false;
							this.showFromPeriod = false;
							this.showToPeriod = false;
							this.showFromTime = true;
							this.showToTime = false;
							this.periodTimeConfig.from.visible = true;
							this.periodTimeConfig.to.visible = false;
							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								"Date",
								values
							);
							this.periodTimeConfig.from.initial = values[0]
								? values[0]
								: undefined;
							// this.periodTimeConfig.from.initial = copy[0];
							this.resetTimeFramePeriod();
							break;

						case "AFTER":
							this.showTimeFrame = true;
							this.showPeriodSelect = false;
							this.showPeriodNumber = false;
							this.showFromPeriod = false;
							this.showToPeriod = false;
							this.showFromTime = true;
							this.showToTime = false;
							this.periodTimeConfig.from.visible = true;
							this.periodTimeConfig.to.visible = false;

							QueryTreeDateAttributeStore.changeCmp(
								this.bucketrestriction.bkt,
								value,
								"Date",
								values
							);
							this.periodTimeConfig.from.initial = values[0]
								? values[0]
								: undefined;
							// this.periodTimeConfig.from.initial = copy[0];
							this.resetTimeFramePeriod();
							break;
					}
				}, 200);
			};

			this.resetTimeFramePeriod = () => {
				this.timeframePeriod = QueryTreeService.getPeriodValue(
					this.bucketrestriction,
					this.type,
					"Date"
				);
				console.log(this.timeframePeriod);
			};

			this.changePeriod = function() {
				QueryTreeService.changeTimeframePeriod(
					this.bucketrestriction,
					this.type,
					{
						Period: this.timeframePeriod,
						Vals: this.bucketrestriction.bkt.Fltr.Vals
					}
				);
			};
			this.callbackChangedValue = function(type, position, value) {
				QueryTreeDateAttributeStore.changeValue(
					this.timeCmp,
					this.bucketrestriction.bkt.Fltr.Vals,
					position,
					value
				);
				this.periodTimeConfig.from.initial = QueryTreeDateAttributeStore.getVal(
					"Date",
					this.timeCmp,
					this.bucketrestriction.bkt,
					0
				);
				this.periodTimeConfig.to.initial = QueryTreeDateAttributeStore.getVal(
					"Date",
					this.timeCmp,
					this.bucketrestriction.bkt,
					1
				);
			};
			// this.init();
		}
	});
