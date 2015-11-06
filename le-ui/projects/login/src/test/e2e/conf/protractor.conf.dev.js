'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "DevelopTestPLSTenant2.DevelopTestPLSTenant2.Production";
config.params.alternativeTenantId = "DevelopTestPLSTenant1.DevelopTestPLSTenant1.Production";

exports.config = config;