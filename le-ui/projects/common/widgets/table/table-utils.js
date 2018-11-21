export const DISABLED = "disabled";
export const ENABLED = "enabled";
export const VISIBLE = "visible";

export const TYPE_NUMBER = "number";
export const TYPE_STRING = "string";
export const TYPE_DATE = "date";
export const TYPE_Object = "Object";

export const DISCENDENT = "dis";
export const ASCENDENT = "asc";

export const RELOAD = false;

export const getData = api => {
  if (api) {
      console.log('======= OK ======');
    return [{
      Object: "Contacts",
      Path: "Templates/ContactSchema",
      TemplateName: "Contact Schema",
      LastEditedDate: 1541193886000,
      Exist: true,
      FeedType: "ContactSchema"
    },
    {
      Object: "Accounts",
      Path: "path/s3",
      TemplateName: "Account Schema",
      Exist: false
    },
    {
      Object: "Product Purchases",
      Path: "a/path",
      TemplateName: "Product Purchases Schema",
      Exist: false
    },
    {
      Object: "Product Bundles",
      Path: "b/path",
      TemplateName: "Product Bundles Schema",
      Exist: false
    },
    {
      Object: "Product Hierarchy",
      Path: "s/s3/path",
      TemplateName: "Product Hierarchy Schema",
      Exist: false
    }];
  } else {
    // console.log('======= NOPE ======');
    return [];
  }
};

