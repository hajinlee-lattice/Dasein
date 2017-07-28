angular.module('common.datacloud.query.factory.restriction', [])
.factory('BucketRestriction', function() {
    function BucketRestriction(columnName, objectType, Bkt) {


        this.Attr = {
            'Id': columnName,
            'Entity': objectType,
            'Bkt': Bkt
        };
        console.log(this.Attr);

        if (Attr === null || Attr === undefined) {
            this.Attr = objectType + '.' + columnName;
        } else {
            this.Attr = Attr;
        }
        if (this.Attr.Bkt === null || this.Attr === undefined) {
            this.Attr.Bkt = oldFormatRangeToBkt(this.Attr.Bkt);
        } else {
            this.Attr.Bkt = function(){
                if (Bkt.Rng === null || Attr === undefined) {
                    this.Rng = {
                        'min' : Bkt.Lbl,
                        'max' : Bkt.Lbl
                    };
                } else {
                    this.Rng = {
                        'min' : Bkt.Rng[0],
                        'max' : Bkt.Rng[1]
                    };
                }
            };
        };

    }

    // used to convert bucket range in old data object
    function oldFormatRangeToBkt(Rng) {
        if (!Rng || Rng.is_null_only) {
            return null;
        }
        
        if (Rng.min = Rng.max) {
            return {
              'Lbl': Rng.min
            };
        } else {
            return {
                'Rng': [Rng.min, Rng.max]
            };
        }
    }

    BucketRestriction.isBucketRestrictionLike = function(bucketRestriction) {
        return bucketRestriction instanceof BucketRestriction || 
        (bucketRestriction.hasOwnProperty('Bkt') && bucketRestriction.hasOwnProperty('Attr')) ||
        (bucketRestriction.hasOwnProperty('Rng') && bucketRestriction.hasOwnProperty('lhs') && bucketRestriction.lhs.hasOwnProperty('columnLookup'));
    };

    BucketRestriction.isEqualRange = function (a, b) {
        return (a.max === b.max) &&
            (a.min === b.min);
            // (a.min === b.min) &&
            // (a.is_null_only === b.is_null_only);
    };

    BucketRestriction.getColumnName = function(bucketRestriction) {
        if (bucketRestriction.Attr === null || bucketRestriction.Attr === undefined) {
            return bucketRestriction.lhs.columnLookup.column_name;
        } else {
            return bucketRestriction.Attr.split(".")[1];
        }
    };

    BucketRestriction.getObjectType = function(bucketRestriction) {
        if (bucketRestriction.Attr === null || bucketRestriction.Attr === undefined) {
            if (bucketRestriction.lhs.columnLookup.object_type === 'BucketedAccountMaster') {
                bucketRestriction.lhs.columnLookup.object_type = 'Account'
            }
            return bucketRestriction.lhs.columnLookup.object_type || 'Account';
        } else {
            return bucketRestriction.Attr.split(".")[0];
        }
    };

    BucketRestriction.getRange = function(bucketRestriction) {
        return bucketRestriction.Rng;
    };

    return BucketRestriction;
});
