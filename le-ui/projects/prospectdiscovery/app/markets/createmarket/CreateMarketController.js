angular.module('pd.markets.createmarket', [
        'pd.builder.attributes'
    ])
    .controller('CreateMarketCtrl', function(AttributesModel) {
        this.AttributesModel = AttributesModel;
        AttributesModel.TargetMarketName = 'Demo Target Market';
    });