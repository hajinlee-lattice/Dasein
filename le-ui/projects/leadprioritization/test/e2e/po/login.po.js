'use strict';

var tenants = require('./tenantselection.po');
var userDropdown = require('./userdropdown.po');

var Login = function() {
	this.email = element(by.model('username'));
	this.password = element(by.model('password'));
    this.loginButton = element(by.id('loginButton'));

    this.loginUser = function(name, password, tenantId) {
        this.login(name, password, tenantId);
        browser.wait(function(){
            return element(by.css('div.page-title')).isPresent();
        }, 100000, 'page title should appear with in 100 sec.');
    };

    this.login = function(name, password, tenantId) {
        tenantId = tenantId || browser.params.tenantId;
        getWebApp();
        browser.driver.sleep(2000);
        isLoginPage().then(function(ispresent){
            if (!ispresent) { logout(); }
            submitLoginCredentials(name, password);
            tenants.tenantSelectionIsPresent().then(function(present){
                if (present) {
                    browser.driver.sleep(500);
                    element(by.id('tenantSelectionInput')).click();
                    tenants.selectTenantByName(tenantId);
                }
            });
        });
    };

    function getWebApp() {
        var width = 1100;
        var height = 768;
        browser.driver.manage().window().setSize(width, height);
        browser.get('/', 30000);
    }

    function isLoginPage() {
        return element(by.css('div.login-wrap')).isPresent();
    }

    function submitLoginCredentials(name, password) {
        browser.waitForAngular();
        element(by.model('username')).sendKeys(name);
        element(by.model('password')).sendKeys(password);
        element(by.id('loginButton')).click();
        browser.waitForAngular();
    }

    function logout() {
        getWebApp();
        isLoginPage().then(function(ispresent){
            if (!ispresent) {
                userDropdown.toggleDropdown();
                browser.driver.wait(function(){
                    return userDropdown.signout.isPresent();
                }, 10000, 'dropdown menu should appear with in 10 sec.');
                userDropdown.signout.click();
                browser.driver.sleep(10000);
            }
        });

    }

    this.logout = function(){ logout(); };

    this.assertLoggedIn = function(expected) {
        if (expected) {
            expect(element(by.css('div.page-title')).isPresent()).toBe(true);
        } else {
            expect(element(by.id('loginMainView')).isPresent()).toBe(true);
        }
    };

    this.loginAsSuperAdmin = function(tenant) {
        this.loginUser(browser.params.superAdminUsername, browser.params.testingUserPassword, tenant);
    };
    this.loginAsInternalAdmin = function(tenant) {
        this.loginUser(browser.params.internalAdminUsername, browser.params.testingUserPassword, tenant);
    };
    this.loginAsInternalUser = function(tenant) {
        this.loginUser(browser.params.internalUserUsername, browser.params.testingUserPassword, tenant);
    };
    this.loginAsExternalAdmin = function(tenant) {
        this.loginUser(browser.params.externalAdminUsername, browser.params.testingUserPassword, tenant);
    };
    this.loginAsExternalUser = function(tenant) {
        this.loginUser(browser.params.externalUserUsername, browser.params.testingUserPassword, tenant);
    };

    this.get = getWebApp;

};

module.exports = new Login();