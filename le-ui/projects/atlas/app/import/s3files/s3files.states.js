import S3FileList from 'atlas/import/s3files/s3files.component';


const s3FileListState = {
  parent: 'home.import.entry',
  name: "s3fileslist",
  url: "/s3files",
  onEnter: ($transition$, $state$) => {
    console.log('ENTEReD', $transition$, $state$);
  },
  resolve: [
    {
      token: 's3fileslist',
      resolveFn: () => {
        console.log('here 2');
        return [];
      }
    }
  ],
  views: {
    'summary@': {
    	templateUrl: 'app/navigation/summary/BlankLine.html'
    },
    'main@': S3FileList
  }
};

const s3filestates = [s3FileListState];
export default s3filestates;
