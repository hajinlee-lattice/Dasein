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
      Path: "/Templates/ContactSchema",
      TemplateName: "ContactSchema Renamed kjjsdf",
      LastEditedDate: 1541193886000,
      Exist: true,
      FeedType: "ContactSchema"
    },
    {
      Object: "Accounts",
      Path: "N/A",
      TemplateName: "N/A",
      Exist: false
    },
    {
      Object: "Product Purchases",
      Path: "N/A",
      TemplateName: "N/A",
      Exist: false
    },
    {
      Object: "Product Bundles",
      Path: "N/A",
      TemplateName: "N/A",
      Exist: false
    },
    {
      Object: "Product Hierarchy",
      Path: "N/A",
      TemplateName: "N/A",
      Exist: false
    }];
  } else {
    console.log('======= NOPE ======');
    return [];
  }
};

