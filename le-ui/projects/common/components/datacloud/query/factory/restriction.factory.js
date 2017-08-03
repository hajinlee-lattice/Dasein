angular.module('common.datacloud.query.factory.restriction', [])
.factory('BucketRestriction', function() {
    function BucketRestriction(columnName, objectType, range, attr, bkt) {

        // console.log(columnName, objectType, range, attr, bkt);


        if (attr === null || attr === undefined) {
            this.attr = objectType + '.' + columnName;
        } else {
            this.attr = attr;
        }
        if (bkt === null || attr === undefined) {
            this.bkt = bkt;
            this.bkt.Rng = range;
        } else {
            this.bkt = bkt;
        }
    }

    // // used to convert bucket range in old data object
    // function oldFormatRangeToBkt(range) {

    //     if (!range) {
    //         return null;
    //     }
        
    //     if (range.min = range.max) {
    //         return {
    //           'Lbl': bkt.Rng.min
    //         };
    //     } else {
    //         return {
    //             'Rng': [bkt.Rng.min, bkt.Rng.max]
    //         };
    //     }
    // }

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


        console.log(bucketRestriction);


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
