import os
from latticeengines.cf.tomcat import parse_profile, update_xmx_by_type


def test_update_xmx():
    opts = update_xmx_by_type('', 't2.medium')
    assert opts == '-Xmx3796m'

    opts = update_xmx_by_type('-Xmx1g -Dpar=val', 'm4.large')
    assert opts == '-Xmx7892m -Dpar=val'

def test_parse_profile():
    profile = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'resources', 'test.profile')
    parse_profile(profile)