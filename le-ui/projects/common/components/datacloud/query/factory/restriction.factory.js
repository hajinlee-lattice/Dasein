angular.module('common.datacloud.query.factory.restriction', [])
.factory('BucketRestriction', function() {
    function BucketRestriction(columnName, bucket) {
        this.bucketRestriction = {
            lhs: {
                columnLookup: {
                    column_name: columnName
                }
            },
            range: bucket
        };
    }

    BucketRestriction.isBucketRestrictionLike = function(bucketRestriction) {
        return bucketRestriction.hasOwnProperty('bucketRestriction') &&
            bucketRestriction.bucketRestriction.hasOwnProperty('range');
    };

    BucketRestriction.isEqualBucket = function (a, b) {
        if (BucketRestriction.isBucketRestrictionLike(a)) {
            a = {
                bucket: a.bucketRestriction.range
            };
        }

        if (BucketRestriction.isBucketRestrictionLike(b)) {
            b = {
                bucket: b.bucketRestriction.range
            };
        }

        return (a.bucket.max === b.bucket.max) &&
            (a.bucket.min === b.bucket.min) &&
            (a.bucket.is_null_only === b.bucket.is_null_only);
    };

    BucketRestriction.getColumnFromBucket = function(bucket) {
        return bucket.bucketRestriction.lhs.columnLookup.column_name;
    };

    return BucketRestriction;
});
