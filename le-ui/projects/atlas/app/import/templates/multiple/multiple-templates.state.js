import MultipleTemplatesList from './multiple-templates.list';
import SummaryContainer from '../components/summary';
export const multipletemplates = {
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