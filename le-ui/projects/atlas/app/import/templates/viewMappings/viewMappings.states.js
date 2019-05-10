import ViewMappings from 'atlas/import/templates/viewMappings/viewMappings.component';

const mappingsListState = {
  parent: 'home',
  name: "fieldmappings",
  url: "/fieldmappings",
  onEnter: ($transition$, $state$) => {
    // console.log('ENTEReD', $transition$, $state$);
  },
  resolve: [
    {
      token: 'fieldmappings',
      resolveFn: () => {
        return [];
      },
      ngservices: (ImportWizardStore) => {
        let obj = {
            ImportWizardStore: ImportWizardStore
        }
        return obj;
      }
    }
  ],
  views: {
    'main@': ViewMappings
  }
};

const mappingsstate = [mappingsListState];
export default mappingsstate;
