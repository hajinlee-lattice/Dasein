'use strict';

describe('smoketest main flow of app', function() {

    var login = require('./po/login.po');

    it('should allow adtester to login', function () {
        login.loginAsADTester();

        runMaiFlowTests();

        login.logout();
    });

    function runMaiFlowTests() {
        // assert logged in
        login.assertLoggedIn(true);
    }
});
