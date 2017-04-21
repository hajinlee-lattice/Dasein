angular.module('common.datacloud.query.factory.restriction', [])
.factory('BucketRestriction', function() {
    function BucketRestriction(columnName, objectType, range) {
        this.lhs = {
            columnLookup: {
                column_name: columnName,
                object_type: objectType || 'BucketedAccountMaster'
            }
        };
        this.range = range;
    }

    BucketRestriction.isBucketRestrictionLike = function(bucketRestriction) {
        return bucketRestriction instanceof BucketRestriction ||
            (bucketRestriction.hasOwnProperty('range') &&
            bucketRestriction.hasOwnProperty('lhs') &&
            bucketRestriction.lhs.hasOwnProperty('columnLookup'));
    };

    BucketRestriction.isEqualRange = function (a, b) {
        return (a.max === b.max) &&
            (a.min === b.min) &&
            (a.is_null_only === b.is_null_only);
    };

    BucketRestriction.getColumnName = function(bucketRestriction) {
        return bucketRestriction.lhs.columnLookup.column_name;
    };

    BucketRestriction.getObjectType = function(bucketRestriction) {
        return bucketRestriction.lhs.columnLookup.object_type || 'BucketedAccountMaster';
    };

    BucketRestriction.getRange = function(bucketRestriction) {
        return bucketRestriction.range;
    };

    return BucketRestriction;
});
