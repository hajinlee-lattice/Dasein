'use strict';

var Logout = function() {
    var loginPage = require('./login.po');
    var userDropdown = require('./userdropdown.po');    
 
    this.logout = function(name) {
        userDropdown.getUserLink(name).click();  
        browser.waitForAngular(); 
        browser.driver.sleep(1000);
        userDropdown.signout.click();
        browser.driver.sleep(3000);
    };

  	this.logoutAsAdmin = function() {
        this.logout(browser.params.adminDisplayName);
    };

  	this.logoutAsNonAdmin = function() {
        this.logout(browser.params.nonAdminDisplayName);
    };

};

module.exports = new Logout();