describe('brute login logout tests', function() {

    var loginPage = require('./po/login.po');

    it('should all be fine', function () {
        testLoginLogout();
        testLoginLogoutLogin();
        testLoginLogoutLogout();
        testLoginLogin();
        testLogoutLogout();
    });


    function testLoginLogout() {
        loginPage.loginAsSuperAdmin();
        loginPage.assertLoggedIn(true);
        loginPage.logout();
        loginPage.assertLoggedIn(false);
    }

    function testLogoutLogout() {
        loginPage.logout();
        loginPage.assertLoggedIn(false);
        loginPage.logout();
        loginPage.assertLoggedIn(false);
    }

    function testLoginLogoutLogout() {
        loginPage.loginAsSuperAdmin();
        loginPage.assertLoggedIn(true);
        loginPage.logout();
        loginPage.assertLoggedIn(false);
        loginPage.logout();
        loginPage.assertLoggedIn(false);
    }

    function testLoginLogoutLogin() {
        loginPage.loginAsSuperAdmin();
        loginPage.assertLoggedIn(true);
        loginPage.logout();
        loginPage.loginAsExternalUser();
        loginPage.assertLoggedIn(true);
    }

    function testLoginLogin() {
        loginPage.loginAsSuperAdmin();
        loginPage.assertLoggedIn(true);
        loginPage.loginAsExternalUser();
        loginPage.assertLoggedIn(true);
    }

});
