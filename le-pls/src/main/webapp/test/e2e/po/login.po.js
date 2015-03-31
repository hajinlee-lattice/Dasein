'use strict';

var tenants = require('./tenantselection.po');

var Login = function() {
	this.email = element(by.model('username'));
	this.password = element(by.model('password'));
    this.loginButton = element(by.id('loginButton'));
 
	this.get = function() {
	    var width = 1100;
        var height = 768;
        browser.driver.manage().window().setSize(width, height);
        browser.get('/');
	};

    this.loginUser = function(name, password) {
        this.get();
        browser.driver.wait(function(){
            return element(by.css('div.login-wrap')).isPresent();
        }, 30000, 'login page should appear with in 30 sec.').then(function(){
            browser.waitForAngular();
            element(by.model('username')).sendKeys(name);
            element(by.model('password')).sendKeys(password);
            element(by.id('loginButton')).click();
            browser.waitForAngular();
        });
    };

  	this.loginAsAdmin = function() {
  		this.loginUser(browser.params.adminUsername, browser.params.adminPassword);
    };

  	this.loginAsNonAdmin = function() {
  		this.loginUser(browser.params.nonAdminUsername, browser.params.nonAdminPassword);
    };

    this.loginAsAdminToTenant = function(i) {
        this.loginAsAdmin();
        tenants.selectTenantByIndex(i);
    };

    this.loginAsNonAdminToTenant = function(i) {
        this.loginAsNonAdmin();
        tenants.selectTenantByIndex(i);
    };
};

module.exports = new Login();