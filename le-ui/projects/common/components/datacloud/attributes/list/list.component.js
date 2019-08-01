/* jshint -W014 */
angular.module("common.attributes.list", []).component("attrResultsList", {
	templateUrl: "/components/datacloud/attributes/list/list.component.html",
	bindings: {
		filters: "<"
	},
	controller: function(
		$state,
		AttrConfigStore,
		BrowserStorageUtility,
		Banner
	) {
		var vm = this;

		vm.store = AttrConfigStore;
		vm.buckets = [];
		vm.attributes = {};
		vm.indeterminate = {};
		vm.startChecked = {};
		vm.allCheckedMap = {};
		vm.accesslevel = "";
		vm.allChecked = false;
		vm.refreshDisabled = false;
		vm.lastRefresh = AttrConfigStore.data.lastRefresh;
		vm.showRefresh = false;
		vm.$onInit = function() {
			vm.accesslevel = vm.store.getAccessRestriction();
			vm.section = vm.store.getSection();
			vm.category = vm.store.get("category");
			vm.data = vm.store.get("data");
			vm.buckets = vm.data.buckets;

			vm.autoDrillDown();
			vm.parseData();
			vm.countSelected();

			vm.store.setData(
				"original",
				JSON.parse(JSON.stringify(vm.data.config))
			);

			if (
				(vm.isUser() || vm.isExternalAdmin()) &&
				vm.section == "activate"
			) {
				vm.showImmutable();
			}
		};

		vm.showImmutable = function() {
			Banner.info({
				title: "Immutable Data Notification",
				message:
					"Your access level has placed certain restrictions on modifications in this section."
			});
		};

		vm.autoDrillDown = function() {
			if (
				vm.data.config &&
				vm.data.config.Subcategories &&
				vm.data.config.Subcategories.length == 1
			) {
				vm.go(vm.data.config.Subcategories[0].DisplayName);
			}
		};

		vm.parseData = function() {
			var total = [];

			vm.data.config.Subcategories.forEach(function(item) {
				var selected = item.Attributes.filter(function(attr) {
					if (attr.IsPremium) {
						vm.filters.disabled = false;
					}

					return attr.Selected;
				});

				selected.forEach(function(attr) {
					vm.startChecked[attr.Attribute] = true;
				});

				item.checked = selected.length;
				item.Selected = vm.isChecked(item);

				vm.allCheckedMap[item.DisplayName] =
					item.checked == item.TotalAttrs;
				vm.attributes[item.DisplayName] = item.Attributes;

				total = total.concat(selected);
			});

			vm.store.set("selected", total);
			vm.store.set("start_selected", total);
		};

		vm.countSelected = function() {
			var total = [];

			Object.keys(vm.attributes).forEach(function(key) {
				var subcategory = vm.getSubcategory(key);
				var attributes = vm.attributes[key];
				var selected = [];

				attributes.forEach(function(attr, index) {
					attr.SubCategory = key;

					if (attr.Selected === true) {
						selected.push(attr);
						total.push(attr);
					}
				});

				if (
					selected.length > 0 &&
					selected.length != attributes.length
				) {
					vm.indeterminate[key] = true;
				} else {
					delete vm.indeterminate[key];
				}

				subcategory.checked = selected.length;
			});

			vm.store.set("selected", total);
			vm.data.config.Selected = vm.store.get("selected").length;
		};

		vm.getResults = function() {
			if (vm.subcategory && !vm.buckets[vm.subcategory]) {
				vm.buckets[vm.subcategory] = { Bkts: { List: [] } };
				vm.store.getBucketData(vm.category, vm.subcategory);
			}

			var ret = vm.subcategory
				? vm.attributes[vm.subcategory]
				: vm.data.config.Subcategories;

			//console.log(vm.getPageSize(), (vm.filters.page - 1) * vm.getPageSize(), ret, filtering);
			return ret;
		};

		vm.getCount = function() {
			return vm.subcategory
				? vm.getSubcategory(vm.subcategory).TotalAttrs
				: vm.data.config.TotalAttrs;
		};

		vm.getBuckets = function(attribute) {
			if (!vm.buckets[vm.subcategory]) {
				return [];
			}

			var bucket = vm.buckets[vm.subcategory][attribute];

			if (bucket && !bucket.Bkts) {
				bucket.Bkts = {
					List: [
						{
							Lbl: "<string>",
							Cnt: bucket.Cnt
						}
					]
				};
			}

			return bucket ? bucket.Bkts.List || [] : [];
		};

		vm.getSubcategory = function(name) {
			return vm.data.config.Subcategories.filter(function(item) {
				return item.DisplayName == name;
			})[0];
		};

		vm.back = function() {
			vm.go("");
		};

		vm.click = function(item) {
			if (item.Attributes) {
				vm.go(item.DisplayName);
			} else {
				vm.toggleSelected(item);
			}
		};

		vm.isChecked = function(item) {
			if (item.Attributes) {
				if (vm.indeterminate[item.DisplayName] === true) {
					vm.setIndeterminate(item.DisplayName, true);

					item.Selected = true;

					return true;
				} else {
					var condition = item.checked == item.TotalAttrs;

					item.Selected = condition;

					return item.checked == item.TotalAttrs;
				}
			} else {
				return item.Selected;
			}
		};

		vm.isAllChecked = function() {
			var subcategory, selected, total, indeterminate;

			if (!vm.subcategory) {
				selected = vm.store.get("selected").length;
				total = vm.data.config.TotalAttrs;
			} else {
				subcategory = vm.getSubcategory(vm.subcategory);
				selected = subcategory.checked;
				total = subcategory.TotalAttrs;
			}

			indeterminate = selected !== 0 && selected != total;
			vm.setIndeterminate(vm.checkboxName(), indeterminate);

			return selected !== 0;
		};

		vm.isDisabled = function(item) {
			var hasFrozen = item.HasFrozenAttrs;
			var isFrozen = item.IsFrozen;
			var overLimit = false;

			if (vm.store.get("limit") >= 0) {
				overLimit =
					vm.store.getSelectedTotal() >= vm.store.get("limit") &&
					!vm.isChecked(item);
			}

			return item.Attributes ? overLimit : isFrozen || overLimit;
		};

		vm.isUser = function() {
			var access = [
				"INTERNAL_USER",
				"LATTICE_USER",
				"EXTERNAL_USER",
				"USER"
			];
			return access.indexOf(this.accesslevel) >= 0;
		};

		vm.isInternalAdmin = function() {
			var access = ["INTERNAL_ADMIN", "LATTICE_ADMIN", "SUPER_ADMIN"];
			return access.indexOf(this.accesslevel) >= 0;
		};

		vm.isExternalAdmin = function() {
			var access = ["EXTERNAL_ADMIN"];
			return access.indexOf(this.accesslevel) >= 0;
		};

		vm.isStartsDisabled = function(item) {
			if (item.Attributes || vm.section == "enable") {
				return false;
			}

			if (
				!item.Selected &&
				(vm.isInternalAdmin() || vm.isExternalAdmin())
			) {
				return false;
			}

			if (item.Selected && vm.isInternalAdmin()) {
				return false;
			}

			if (vm.isUser()) {
				return true;
			}

			var startsDisabled = vm.startChecked[item.Attribute];

			return startsDisabled;
		};

		vm.toggleSelected = function(item) {
			if (vm.isDisabled(item) || vm.isStartsDisabled(item)) {
				return false;
			}

			if (item.Attributes) {
				vm.setIndeterminate(item.DisplayName, false);

				item.Attributes.sort(vm.sortAttributes).forEach(function(attr) {
					if (vm.isDisabled(attr) || vm.isStartsDisabled(attr)) {
						return;
					}

					attr.Selected = item.checked != item.TotalAttrs;

					if (attr.Selected) {
						vm.store.get("selected").push(attr);
					}
				});
			} else {
				item.Selected = !item.Selected;
			}

			vm.countSelected();
		};

		vm.toggleAll = function() {
			if (vm.subcategory) {
				vm.allCheckedMap[vm.subcategory] =
					vm.allCheckedMap[vm.subcategory] !== undefined
						? !vm.allCheckedMap[vm.subcategory]
						: false;

				vm.attributes[vm.subcategory]
					.sort(vm.sortAttributes)
					.forEach(function(attr) {
						if (vm.isDisabled(attr) || vm.isStartsDisabled(attr)) {
							return;
						}

						attr.Selected = vm.allCheckedMap[vm.subcategory];

						if (attr.Selected) {
							vm.store.get("selected").push(attr);
						}
					});
			} else {
				vm.allChecked = !vm.allChecked;

				Object.keys(vm.attributes)
					.sort(vm.sortSubcategories)
					.forEach(function(key) {
						vm.setIndeterminate(key, false);
						vm.attributes[key].checked =
							vm.attributes[key].TotalAttrs;

						vm.attributes[key]
							.sort(vm.sortAttributes)
							.forEach(function(attr) {
								if (
									vm.isDisabled(attr) ||
									vm.isStartsDisabled(attr)
								) {
									return;
								}

								attr.Selected = vm.allChecked;

								if (attr.Selected) {
									vm.store.get("selected").push(attr);
								}
							});
					});
			}

			vm.countSelected();
		};

		vm.checkboxName = function() {
			return vm.subcategory
				? "check_all_" + vm.subcategory
				: "all_attributes";
		};

		vm.setIndeterminate = function(checkbox, value) {
			vm.indeterminate[checkbox] = value;
			//$('[name="' + checkbox + '"]').prop('indeterminate', value);
		};

		vm.sortAttributes = function(a, b) {
			return a.Attribute.toLowerCase().localeCompare(
				b.Attribute.toLowerCase()
			);
		};

		vm.sortSubcategories = function(a, b) {
			return a.toLowerCase().localeCompare(b.toLowerCase());
		};

		vm.getAttributes = function(filtered) {
			var treeroot = [];

			vm.data.config.Subcategories.forEach(function(item) {
				treeroot = item.Attributes.concat(treeroot);
			});

			vm.store.set("TotalFilteredAttrs", treeroot);

			return vm.subcategory ? filtered : treeroot;
		};

		vm.getPageSize = function() {
			return vm.filters.pagesize;
		};

		vm.getFiltering = function(root) {
			var obj = vm.store.getFiltering();

			return root || vm.subcategory ? obj : { Attributes: obj };
		};

		vm.refreshAttributes = () => {
			vm.refreshDisabled = true;
			AttrConfigStore.refreshAttributes().then(() => {
				vm.refreshDisabled = false;
			});
		};

		vm.go = function(subcategory) {
			vm.subcategory = subcategory;
			vm.filters.page = 1;

			$state.go(".", {
				subcategory: subcategory
			});
		};
	}
});
