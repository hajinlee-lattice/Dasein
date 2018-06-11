'use strict';

describe('Purchase History Widget Filters Tests', function () {
	var _$filter;
	var identity = function(_){return _;}

	beforeEach(function () {
		module('mainApp.appCommon.widgets.PurchaseHistoryWidget');
		inject(['$filter',
			function ($filter) {
				_$filter = $filter;
			}
		]);
	});

	describe('sortWithNulls filter', function () {
		var toReturn;

		it('should exist', function () {
			toReturn = _$filter('sortWithNulls');
			expect(toReturn).not.toBeNull();
		});

		it('should sort and push nulls to end on flat arrays descending', function () {
			var arr = [0,null,4,null,2,3];
			var toReturn = _$filter('sortWithNulls')(arr, identity, false);
			expect(toReturn).toEqual([0,2,3,4,null,null]);
		});

		it('should sort and push nulls to end on flat arrays ascending', function () {
			var arr = [0,null,4,null,2,3];
			var toReturn = _$filter('sortWithNulls')(arr, identity, true);
			expect(toReturn).toEqual([4,3,2,0,null,null]);
		});

		it('should sort without nulls too', function () {
			var arr = [0,5,4,1,2,3];
			var toReturn = _$filter('sortWithNulls')(arr, identity, true);
			expect(toReturn).toEqual([5,4,3,2,1,0]);
		});

		it('should sort and push nulls to end in array of objects descending', function () {
			var arr = [{
					a: {b: 0}
				},{
					a: {b: null}
				},{
					a: {b: 1}
				}];
			var expected = [{
					a: {b: 0}
				},{
					a: {b: 1}
				},{
					a: {b: null}
				}];
			var toReturn = _$filter('sortWithNulls')(arr, 'a.b', false);

			expect(angular.equals(toReturn, expected)).toBeTruthy();
		});

		it('should sort and push nulls to end in array of objects ascending', function () {
			var arr = [{
					a: {b: 0}
				},{
					a: {b: null}
				},{
					a: {b: 1}
				}];
			var expected = [{
					a: {b: 1}
				},{
					a: {b: 0}
				},{
					a: {b: null}
				}];
			var toReturn = _$filter('sortWithNulls')(arr, 'a.b', true);

			expect(angular.equals(toReturn, expected)).toBeTruthy();
		});

		it('should return itself if not an array', function () {
			var obj = {a: {b: 0}};

			var toReturn = _$filter('sortWithNulls')(obj, 'a.b', true);
			expect(angular.equals(toReturn, obj)).toBeTruthy();
		})
	});
});
