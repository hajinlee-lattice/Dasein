'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantName = "Tenant2";
config.params.alternativeTenantName = "Tenant1";

exports.config = config;