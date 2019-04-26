import OverviewComponent from './overview.component';
import SummaryContainer from 'atlas/import/templates/components/summary';

const playbookOverview = {
  parent: 'home',
  name: "playbookOverview",
  url: "/overview",
  views: {
    'summary@': SummaryContainer,
    'main@': OverviewComponent
  }
};

const states = [playbookOverview];
export default states;
