'use strict';

var userDropdown = require('./userdropdown.po');

var Login = function() {
    this.loginUser = function(name, password) {
        getWebApp();
        isLoginPage().then(function(ispresent){
            if (!ispresent) { logout(); }
            submitLoginCredentials(name, password);
        });
        browser.wait(function(){
            return element(by.css('nav.navbar')).isPresent();
        }, 5000, 'page title should appear with in 5 sec.');
    };

    function getWebApp() {
        var width = 1100;
        var height = 768;
        browser.driver.manage().window().setSize(width, height);
        browser.get('/');
    }


    function isLoginPage() {
        return element(by.css('div.login-form')).isPresent();
    }

    function submitLoginCredentials(name, password) {
        browser.waitForAngular();
        element(by.model('Username')).sendKeys(name);
        element(by.model('Password')).sendKeys(password);
        element(by.buttonText('Log In')).click();
        browser.waitForAngular();
    }

    function logout() {
        getWebApp();
        isLoginPage().then(function(ispresent){
            if (!ispresent) {
                userDropdown.toggleDropdown();
                browser.wait(function(){
                    return userDropdown.signout.isPresent();
                }, 5000, 'dropdown menu should appear with in 5 sec.');
                userDropdown.signout.click();
                browser.driver.sleep(2000);
            }
        });

    }

    this.logout = function(){ logout(); };

    this.assertLoggedIn = function(expected) {
        if (expected) {
            expect(element(by.css('nav.navbar')).isDisplayed()).toBe(true);
        } else {
            expect(element(by.id('div.login-form')).isDisplayed()).toBe(true);
        }
    };

    this.loginAsADTester = function() {
        this.loginUser(browser.params.adtesterusername, browser.params.adtesterpassword);
    };

    this.get = getWebApp;

};

module.exports = new Login();