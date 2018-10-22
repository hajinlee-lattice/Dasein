export const DISABLED = "disabled";
export const ENABLED = "enabled";
export const VISIBLE = "visible";

export const TYPE_NUMBER = "number";
export const TYPE_STRING = "string";
export const TYPE_DATE = "date";
export const TYPE_OBJECT = "object";

export const DISCENDENT = "dis";
export const ASCENDENT = "asc";

export const RELOAD = false;

export const getData = api => {
  if (api) {
      console.log('======= OK ======');
    return [
      {
        name: "Test123 Account",
        object: "Account",
        location: "s3://lattice-engines.com/home/customer/adobe/accounts",
        edited: "n/a",
        templateCreated: false
      },
      {
        name: "Test456 Contacts",
        object: "Contacts",
        location: "path/to/the/file",
        edited: "n/a",
        templateCreated: true
      },
      {
        name: "Test123",
        object: "Products Purchases",
        location: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      },
      {
        name: "Test123",
        object: "Product Bundles",
        location: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      },
      {
        name: "Test123",
        object: "Product Hierarchy",
        location: "path/to/the/file",
        edited: "n/a",
        templateCreated: false
      }
    ];
  } else {
    console.log('======= NOPE ======');
    return [];
  }
};
