import './segmentexport/segmentexport.component';
import './orphanexport/orphan-export.component';

import Filters from './filters/filters.component';
import AttributeFeedbackButton from './attributefeedback/attributefeedbackbutton.directive';
import AttributeFeedbackModal from './attributefeedback/attributefeedbackmodal.directive';
import AttributeTile from './attributetile/attributetile.component';
import CategoryTile from './categorytile/categorytile.component';
import SubcategoryTile from './subcategorytile/subcategorytile.component';
import CompanyProfile from './companyprofile/companyprofile.component';
import LatticeRatingCard from './latticeratingcard/latticeratingcard.component';
import DataCloudController from './explorer.component';
import Utils from './explorer.utils';

const Dependencies = [
    'common.services.featureflag',
    'common.utilities.browserstorage',
    'common.utilities.number',
    'common.directives.tilebarchart',
    'common.datacloud.query',
    'common.datacloud.lookup',
    'common.notice'
];

angular
    .module('common.datacloud.explorer', Dependencies)
    .service('ExplorerUtils', Utils.ExplorerUtils)
    .directive('fallbackSrc', Utils.FallbackSrc)
    .directive('explorerFilters', Filters)
    .directive('explorerAttributeTile', AttributeTile)
    .directive('explorerCategoryTile', CategoryTile)
    .directive('explorerSubcategoryTile', SubcategoryTile)
    .directive('explorerCompanyProfile', CompanyProfile)
    .directive('explorerLatticeRatingCard', LatticeRatingCard)
    .directive('attributeFeedbackButton', AttributeFeedbackButton)
    .directive('attributeFeedbackModal', AttributeFeedbackModal)
    .controller('DataCloudController', DataCloudController);