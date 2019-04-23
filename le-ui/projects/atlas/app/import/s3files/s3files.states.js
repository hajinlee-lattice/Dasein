import S3FileList from 'atlas/import/s3files/s3files.component';


const templatesListState = {
  parent: 'home',
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
    'summary@': SummaryContainer,
    'main@': MultipleTemplatesList
  }
};

const creationSystemState = {
  parent: 'home',
  name: 'sistemcreation',
  url: '/system',
  views: {
    "main@^.^": SystemCreationComponent
  }
};
const states = [creationSystemState, templatesListState];
export default states;
