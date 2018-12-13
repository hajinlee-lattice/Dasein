import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeMenu from "./le-menu";
import LeMenuItem from "./le-menu-item";

describe("<LeMenu/> rendering", () => {
  it("<LeMenu /> rendering <LeMenuItem /> ", () => {
    const wrapper = shallow(
      <LeMenu classNames="test-menu" image="fa fa-ellipsis-v" name="main">
        <LeMenuItem
          name="edit"
          label="Edit"
          image="fa fa-pencil-square-o"
          callback={name => {
            console.log("NAME ", name);
          }}
        />

        <LeMenuItem
          name="duplicate"
          label="Duplicate"
          image="fa fa-files-o"
          callback={name => {
            console.log("NAME ", name);
          }}
        />

        <LeMenuItem
          name="delete"
          label="Delete"
          image="fa fa-trash-o"
          callback={name => {
            console.log("NAME ", name);
          }}
        />
      </LeMenu>
    );
    expect(wrapper.find(".menu-content").children()).toHaveLength(3);
  });

  it("<LeMenuItem /> icon rendering", () => {
    const menuItemName = "edit";
    const wrapper = mount(
      <LeMenu classNames="test-menu" image="fa fa-ellipsis-v" name="main">
        <LeMenuItem
          name={menuItemName}
          label="Edit"
          image="fa fa-pencil-square-o"
          callback={(name) =>{
          }}
        />
      </LeMenu>
    );
    expect(wrapper.find(LeMenuItem).html()).toContain('fa fa-pencil-square-o');
  });
});

describe("<LeMenuItem /> interactions", () => {
  it("<LeMenuItem /> click ", () => {
    const mockCallBack = jest.fn();
    const menuItemName = "edit";
    const wrapper = mount(
      <LeMenu classNames="test-menu" image="fa fa-ellipsis-v" name="main">
        <LeMenuItem
          name={menuItemName}
          label="Edit"
          image="fa fa-pencil-square-o"
          callback={mockCallBack}
        />
      </LeMenu>
    );
    wrapper.find(LeMenuItem).simulate("click");
    expect(mockCallBack.mock.calls[0][0]).toBe(menuItemName);
  });
});
