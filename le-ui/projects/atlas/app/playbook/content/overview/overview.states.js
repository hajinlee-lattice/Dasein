import OverviewComponent from './overview.component';
import SummaryContainer from 'atlas/import/templates/components/summary';
const playbookOverview = {
  parent: 'home',
  name: "playbookoverview",
  url: "/playbookoverview",
  views: {
    'summary@': SummaryContainer
  }
};

const overviewStates = [playbookOverview];
export default overviewStates;
