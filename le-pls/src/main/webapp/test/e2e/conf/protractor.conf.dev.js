'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "DevelopTestTenant2.DevelopTestTenant2.Production";
config.params.alternativeTenantId = "DevelopTestTenant1.DevelopTestTenant1.Production";

exports.config = config;