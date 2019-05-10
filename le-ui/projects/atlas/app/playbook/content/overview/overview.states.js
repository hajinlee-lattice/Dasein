import OverviewComponent from './overview.component';
import OverviewSummaryContainer from '../overviewsummary/overviewsummary';

import ReactRouter from 'atlas/react/router';

const playbookOverview = {
    parent: 'home',
    name: "playbookOverview",
    url: "/overview",
    resolve: [{
        token: 'PlaybookWizardStore',
        resolveFn: () => {
            return ReactRouter.getRouter()['ngservices'].PlaybookWizardStore;
        }
    }],
    views: {
        'summary@': OverviewSummaryContainer,
        'main@': OverviewComponent
    }
};

const overviewstates = [playbookOverview];
export default overviewstates;