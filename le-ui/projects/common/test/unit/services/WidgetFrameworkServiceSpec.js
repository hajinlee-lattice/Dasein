'use strict';

describe('WidgetFrameworkServiceSpec Tests', function () {
	var widgetFramework,
		widgetUtil,
		childIndices;

	beforeEach(function () {
		module('mainApp.appCommon.utilities.WidgetConfigUtility')
		module('mainApp.appCommon.services.WidgetFrameworkService');

		inject(['WidgetFrameworkService', 'WidgetConfigUtility',
			function (WidgetFrameworkService, WidgetConfigUtility) {
				widgetFramework = WidgetFrameworkService;
				widgetUtil = WidgetConfigUtility;
				childIndices = [0 ,1, 2];
			}
		]);

	});

	describe('IsChildActive given invalid parameters', function () {
		it('should return true when the activeWidgets parameter is undefined', function () {
			testIsChildActive(
				undefined,
				childIndices,
				[true, true, true]);
		});

		it('should return true when the activeWidgets parameter is undefined', function () {
			testIsChildActive(
				null,
				childIndices,
				[true, true, true]);
		});

		it('should return true when no active widget states are given', function () {
			testIsChildActive(
				[],
				childIndices,
				[true, true, true]);
		});

		it('should return true when invalid active widget states are given', function () {
			testIsChildActive(
				["NotAValidState", "MeNeither"],
				childIndices,
				[true, true, true]);
		})
	});

	describe('IsChildActive given just one active widget value', function () {
		it('should work for "All"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_ALL],
				childIndices,
				[true, true, true]);
		});

		it('should work for "Even"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_EVEN],
				childIndices,
				[false, true, false]);
		});

		it('should work for "First"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_FIRST],
				childIndices,
				[true, false, false]);
		});

		it('should work for "Last"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_LAST],
				childIndices,
				[false, false, true]);
		});

		it('should work for "None"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_NONE],
				childIndices,
				[false, false, false]);
		});

		it('should work for "Odd"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_ODD],
				childIndices,
				[true, false, true]);
		});

	});

	describe ('IsChildActive given multiple active widget values', function () {
		beforeEach(function () {
			childIndices = [0, 1, 2, 3, 4];
		});

		it('should work for "First" and "Even"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_FIRST, widgetUtil.ACTIVE_WIDGET_EVEN],
				childIndices,
				[true, true, false, true, false]);
		});

		it('should work for "Even" and "Odd"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_EVEN, widgetUtil.ACTIVE_WIDGET_ODD],
				childIndices,
				[true, true, true, true, true]);
		});

		it('should work for "First" and "Last"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_FIRST, widgetUtil.ACTIVE_WIDGET_LAST],
				childIndices,
				[true, false, false, false, true]);
		});

		it('should work for "First", "Even", and "Last', function () {
			testIsChildActive(
				[	
					widgetUtil.ACTIVE_WIDGET_FIRST,
					widgetUtil.ACTIVE_WIDGET_EVEN,
					widgetUtil.ACTIVE_WIDGET_LAST
				],
				childIndices,
				[true, true, false, true, true]);
		});
	});

	describe ('IsChildActive given "All" or "None" values', function () {
		beforeEach(function () {
			childIndices = [0, 1, 2, 3, 4];
		});

		it('should only use "All" when it exists', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_FIRST, widgetUtil.ACTIVE_WIDGET_ALL],
				childIndices,
				[true, true, true, true, true]);
		});

		it('should only use "None" when it exists', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_FIRST, widgetUtil.ACTIVE_WIDGET_NONE],
				childIndices,
				[false, false, false, false, false]);
		})

		it('should only use "All" when given both "All" and "None"', function () {
			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_ALL, widgetUtil.ACTIVE_WIDGET_NONE],
				childIndices,
				[true, true, true, true, true]);

			testIsChildActive(
				[widgetUtil.ACTIVE_WIDGET_NONE, widgetUtil.ACTIVE_WIDGET_ALL],
				childIndices,
				[true, true, true, true, true]);
		});

	});

	function testIsChildActive(activeWidgets, childIndices, expectedActiveValues) {
		var isChildActive;
		for (var i=0; i<childIndices.length; i++) {
			isChildActive = widgetFramework.IsChildActive(
				activeWidgets, childIndices[i], childIndices.length);
			expect(isChildActive).toBe(expectedActiveValues[i]);
		}
	}

});