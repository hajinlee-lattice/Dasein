angular
	.module("common.datacloud.query.builder.tree.edit.percent", [])
	.service("PercentStore", [
		function() {
			var PercentStore = this;
			PercentStore.directionMap = {
				DEC: {
					name: "DEC",
					displayName: "Decrease",
					readable: "Decreased"
				},
				INC: {
					name: "INC",
					displayName: "Increase",
					readable: "Increased"
				}
			};
			PercentStore.cmpMap = {
				AS_MUCH_AS: {
					name: "AS_MUCH_AS",
					displayName: "By as Much as"
				},
				AT_LEAST: {
					name: "AT_LEAST",
					displayName: "By at Least"
				},
				BETWEEN: {
					name: "BETWEEN",
					displayName: "Between"
				}
			};

			function getConfigField(position) {
				// var tmp = PercentStore.getNumericalPeriodConfig();

				var values = PercentStore.getNumericalPeriodConfig(); //JSON.parse(JSON.stringify(tmp));
				var config = values[Object.keys(values)[position]];
				return config;
			}

			this.getNumericalFieldName = function(position) {
				var config = getConfigField(position);
				return config.name;
			};
			this.getDirectionList = function() {
				return [
					{
						name: "DEC",
						displayName: "Decrease"
					},
					{
						name: "INC",
						displayName: "Increase"
					}
				];
			};

			this.getCmpList = function() {
				return [
					{
						name: "AS_MUCH_AS",
						displayName: "By as Much as"
					},
					{
						name: "AT_LEAST",
						displayName: "By at Least"
					},
					{
						name: "BETWEEN",
						displayName: "Between"
					}
				];
			};

			this.getNumericalPeriodConfig = function() {
				return {
					from: {
						name: "from-period-percent",
						value: undefined,
						position: 0,
						type: "Avg",
						min: "0",
						max: ""
					},
					to: {
						name: "to-period-percent",
						value: undefined,
						position: 1,
						type: "Avg",
						min: "0",
						max: ""
					}
				};
			};
			this.restValues = function(bkt) {
				bkt.Chg.Vals = [];
			};

			this.changeValue = function(cmp, valsArray, position, value) {
				switch (cmp) {
					case "BETWEEN": {
						valsArray[position] = value;
						break;
					}
					default: {
						valsArray[0] = value;
					}
				}
			};
			this.getDirection = function(bkt) {
				var direction = bkt.Chg.Direction;
				if (direction) {
					return direction;
				} else {
					bkt.Chg.Direction = "DEC";
					return "DEC";
				}
			};

			this.setDirection = function(bkt, direction) {
				bkt.Chg.Direction = direction;
			};

			this.getVal = function(cmp, bkt, position) {
				var valsArray = bkt.Chg.Vals;
				switch (cmp) {
					case "BETWEEN": {
						return valsArray[position];
					}
					case "AS_MUCH_AS":
					case "AT_LEAST": {
						if (
							position == valsArray.length - 1 ||
							valsArray.length == 0
						) {
							return valsArray[valsArray.length - 1];
						} else {
							return null;
						}
					}
					default: {
						return null;
					}
				}
			};

			this.getDirectionRedable = function(bucketrestriction) {
				var direction = bucketrestriction.bkt.Chg.Direction;
				var directionObj = PercentStore.directionMap[direction];
				if (directionObj) {
					return directionObj.readable ? directionObj.readable : "";
				} else {
					return "";
				}
			};

			this.getCmpRedable = function(bucketrestriction) {
				var cmp = bucketrestriction.bkt.Chg.Cmp;
				var cmpObj = PercentStore.cmpMap[cmp];
				if (cmpObj) {
					return cmpObj.displayName ? cmpObj.displayName : "";
				} else {
					return "";
				}
			};

			this.getValuesFormatted = function(bucketrestriction) {
				var bkt = bucketrestriction.bkt;
				var vals = bkt.Chg.Vals;

				if (vals) {
					var list = vals.toString();
					list = list.replace(/,/g, " - ");
					return list;
				} else {
					return "";
				}
			};

			this.getCmp = function(bkt) {
				return bkt.Chg.Cmp;
			};

			this.changeCmp = function(bkt, cmp) {
				bkt.Chg.Cmp = cmp;
			};

			this.isPeriodRangeValid = function(form) {
				var valid = true;
				var confFrom = getConfigField(0);
				var confTo = getConfigField(1);
				if (form[confFrom.name] && !form[confFrom.name].$valid) {
					valid = false;
				}
				if (form[confTo.name] && !form[confTo.name].$valid) {
					valid = false;
				}
				return valid;
			};

			this.getErrorMsg = function(fromVisible, toVisible) {
				if (fromVisible === true && toVisible == true) {
					return "Enter a valid range";
				} else {
					return "Enter a valid number";
				}
			};
			this.updateBkt = function(original, changed) {
				original.Chg.Direction = changed.Chg.Direction;
				original.Chg.Cmp = changed.Chg.Cmp;
				original.Chg.Vals = changed.Chg.Vals;
			};
		}
	]);
