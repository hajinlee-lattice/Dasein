
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re

def IsStandardType( t ):

    c = re.search( '^VarChar\(-?\d+\)$', t )
    if c:
        return True

    c = re.search( '^NVarChar\(-?\d+\)$', t )
    if c:
        return True

    if t in [ 'Date','DateTime', 'Float', 'Double', 'Bit', 'Int', 'Long' ]:
        return True

    return False


def AggregationForStandardType( t ):

    c = re.search( '^VarChar\(\d+\)$', t )
    if c:
        return 'Combine'

    c = re.search( '^NVarChar\(\d+\)$', t )
    if c:
        return 'Combine'

    if t in [ 'Float', 'Double', 'Int', 'Long' ]:
        return 'Sum'

    if t in [ 'DateTime', 'Bit' ]:
        return 'Max'

    return 'Max'
    
