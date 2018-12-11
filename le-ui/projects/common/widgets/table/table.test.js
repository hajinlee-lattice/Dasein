import React from "common/react-vendor";
import { shallow, mount } from 'enzyme';


import LeTable from "./table";
import LeTableHeader from "./table-header";
import LeTableRow from "./table-row";
import LeTableCell from "./table-cell";

let config = { name: "Some Fake Table", emptymsg: "no results", header: [] };
let mapping = { foo: { colSpan: 2, colName: "foo" } };
let data = { foo: "bar" };

test("unit-LeTable(empty)", () => {
    const wrapper = shallow(
        <LeTable
            config={config}
            name={config.name}
            emptymsg={config.emptymsg}
            showEmpty={true}
            data={[]}
        />
    );

    expect(wrapper.find(".le-table-row-no-select")).toBeDefined();
});

test("unit-LeTableHeader", () => {
    const wrapper = shallow(<LeTableHeader headerMapping={mapping} />);
    expect(wrapper.find(".le-table-header")).toBeDefined();
});

test("unit-LeTableRow", () => {
    const wrapper = shallow(
        <LeTableRow columnsMapping={mapping} rowIndex={0} rowData={data} />
    );

    expect(wrapper.find(".le-table-row")).toBeDefined();
});

test("unit-LeTableCell", () => {
    const wrapper = shallow(
        <LeTableCell colName="foo" columnsMapping={mapping} rowData={data} />
    );

    expect(wrapper.find(".le-table-cell")).toBeDefined();
});

test("integration-LeTable", () => {
    let config = {
        name: "Some Fake Table",
        header: [
            {
                name: "name",
                displayName: "Name",
                sortable: false
            },
            {
                name: "value",
                displayName: "Value",
                sortable: false
            }
        ],
        columns: [
            {
                colSpan: 6
            },
            {
                colSpan: 6
            }
        ]
    };

    let state = {
        forceReload: false,
        showEmpty: false,
        showLoading: false,
        data: [
            {
                name: "Foo",
                value: "Baz"
            },
            {
                name: "Bar",
                value: "Bop"
            }
        ]
    };

    const wrapper = mount(
        <LeTable
            name={config.name}
            config={config}
            showLoading={state.showLoading}
            showEmpty={state.showEmpty}
            data={state.data}
        />
    );

    expect(wrapper.find(".le-table-row").length).toBe(4);
});
