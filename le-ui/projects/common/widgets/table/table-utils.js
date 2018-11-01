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
    return [
      {
        TemplateName: "Test123 Account",
        Object: "Account",
        Path: "s3://lattice-engines.com/home/customer/adobe/accounts",
        edited: "n/a",
        templateCreated: false
      },
      {
        TemplateName: "Test456 Contacts",
        Object: "Contacts",
        Path: "path/to/the/file",
        edited: "n/a",
        templateCreated: true
      },
      {
        TemplateName: "Test123",
        Object: "Products Purchases",
        Path: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      },
      {
        TemplateName: "Test123",
        Object: "Product Bundles",
        Path: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      },
      {
        TemplateName: "Test123",
        Object: "Product Hierarchy",
        Path: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      }
    ];
  } else {
    console.log('======= NOPE ======');
    return [];
  }
};

