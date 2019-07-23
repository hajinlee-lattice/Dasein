import TemplatesComponent from './templates.component';
import SummaryContainer from './components/summary';
export const templatelist = {
  parent: 'home',
  name: "templatelist",
  url: "/templatelist",
  views: {
    'summary@': SummaryContainer,
    'main@': TemplatesComponent
  }
};

const statessingle = [templatelist];
export default statessingle;