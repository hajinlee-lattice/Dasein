import S3FileList from 'atlas/import/s3files/s3files.component';

const s3FileListState = {
  parent: 'home',
  name: "files",
  url: "/files",
  onEnter: ($transition$, $state$) => {
    console.log('ENTEReD', $transition$, $state$);
  },
  resolve: [
    {
      token: 'files',
      resolveFn: () => {
        return [];
      }
    }
  ],
  views: {
    'main@': S3FileList
  }
};

const s3filestates = [s3FileListState];
export default s3filestates;
