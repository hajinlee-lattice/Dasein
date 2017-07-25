angular.module('common.datacloud.query.factory.restriction', [])
.factory('BucketRestriction', function() {
    function BucketRestriction(columnName, objectType, range, attr, bkt) {
        if (attr === null || attr === undefined) {
            this.lhs = {
                columnLookup: {
                    column_name: columnName,
                    object_type: objectType || 'LatticeAccount'
                }
            };
            this.attr = objectType + '.' + columnName;
        } else {
            this.attr = attr;
        }
        if (bkt === null || attr === undefined) {
            this.bkt = oldFormatRangeToBkt(range);
            this.range = range;
        } else {
            this.bkt = bkt;
            if (bkt.rng === null || attr === undefined) {
                this.range = {
                    'min' : bkt.lbl,
                    'max' : bkt.lbl
                };
            } else {
                this.range = {
                    'min' : bkt.rng[0],
                    'max' : bkt.rng[1]
                };
            }
        }
    }

    // used to convert bucket range in old data object
    function oldFormatRangeToBkt(range) {
        if (!range || range.is_null_only) {
            return null;
        }
        
        if (range.min = range.max) {
            return {
              'Lbl': range.min
            };
        } else {
            return {
                'Rng': [range.min, range.max]
            };
        }
    }

    BucketRestriction.isBucketRestrictionLike = function(bucketRestriction) {
        return bucketRestriction instanceof BucketRestriction ||
            (bucketRestriction.hasOwnProperty('bkt') &&
            bucketRestriction.hasOwnProperty('attr')) ||
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
        if (bucketRestriction.attr === null || bucketRestriction.attr === undefined) {
            return bucketRestriction.lhs.columnLookup.column_name;
        } else {
            return bucketRestriction.attr.split(".")[1];
        }
    };

    BucketRestriction.getObjectType = function(bucketRestriction) {
        if (bucketRestriction.attr === null || bucketRestriction.attr === undefined) {
            if (bucketRestriction.lhs.columnLookup.object_type === 'BucketedAccountMaster') {
                bucketRestriction.lhs.columnLookup.object_type = 'LatticeAccount'
            }
            return bucketRestriction.lhs.columnLookup.object_type || 'LatticeAccount';
        } else {
            return bucketRestriction.attr.split(".")[0];
        }
    };

    BucketRestriction.getRange = function(bucketRestriction) {
        return bucketRestriction.range;
    };

    return BucketRestriction;
});
