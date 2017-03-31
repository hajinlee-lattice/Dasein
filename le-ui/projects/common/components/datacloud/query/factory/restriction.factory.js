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

    return BucketRestriction;
});
