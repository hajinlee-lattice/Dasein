'use strict';

var Setup = function() {
    this.getNavLinkByNodeName = function(nodeName) {
        return element(by.css('.setup-sidebar li[node-name="' + nodeName + '"]'));
    };

};

module.exports = new Setup();