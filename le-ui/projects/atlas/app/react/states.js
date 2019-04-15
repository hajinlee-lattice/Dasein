import MultipleTemplatesList from 'atlas/import/templates/multiple/multiple-templates.list';
import SummaryContainer from 'atlas/import/templates/components/summary';
import SystemCreationComponent from 'atlas/import/templates/multiple/system-creation.component';
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
  name: 'templateslist.sistemcreation',
  url: '/system',
  views: {
    // Relatively target the parent-state's parent-state's 'messagecontent' ui-view
    // This could also have been written using ui-view@state addressing: 'messagecontent@mymessages'
    // Or, this could also have been written using absolute ui-view addressing: '!$default.$default.messagecontent'
    "main@^.^": SystemCreationComponent
  }
};

export const mtstates = [templatesListState, creationSystemState];
