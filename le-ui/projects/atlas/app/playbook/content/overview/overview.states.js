import OverviewComponent from './overview.component';
import SummaryContainer from 'atlas/import/templates/components/summary';

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
        'summary@': SummaryContainer,
        'main@': OverviewComponent
    }
};

const overviewstates = [playbookOverview];
export default overviewstates;