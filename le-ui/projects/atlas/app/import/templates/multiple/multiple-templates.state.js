import MultipleTemplatesList from './multiple-templates.list';
import SummaryContainer from '../components/summary';
import SystemCreationComponent from './system-creation.component';
const templatesListState = {
  name: "templateslist",
  url: "/templateslist",
  onEnter: ($transition$, $state$) => {
    console.log('ENTEReD', $transition$, $state$);
  },
  resolve: [
    {
      token: 'templateslist',
      resolveFn: () => {
        console.log('FN');
        return [];
      }
    }
  ],
  views: {
    'summary': SummaryContainer,
    'main': MultipleTemplatesList
  }
};
const creationSystemState = {
  name: 'sistemcreation',
  url: '/system',
  views: {
    'main': SystemCreationComponent
  }
};

export const mtstates = [templatesListState, creationSystemState];
