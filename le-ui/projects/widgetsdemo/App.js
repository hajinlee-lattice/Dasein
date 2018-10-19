import React, { Component } from "../common/react-vendor";
import { UIRouter, UIView } from "../common/react-vendor";
import "./App.css";
// import NavigationBar from './modules/navigation';
import {
  LeToolBar,
  HORIZONTAL,
  VERTICAL
} from "../common/widgets/toolbar/le-toolbar";
import LeToolbarItem from "../common/widgets/toolbar/le-toolbar-item";

import router from "./router";

class App extends Component {
  render() {
    return (
      <div className="main">
        <LeToolBar direction={VERTICAL}>
          {/* <img src="../common/assets/images/bkg-active-model-account.png"></img>     */}
          <LeToolbarItem
            image="fa fa-user"
            label="B"
            name="button"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("buttons");
            }}
          />
          <LeToolbarItem
            image="fa fa-calendar-check-o"
            name="dropdown"
            label="D"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("dropdowns");
            }}
          />
          <LeToolbarItem
            image="fa fa-ellipsis-v"
            label="M"
            name="menus"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("menus");
            }}
          />
          <LeToolbarItem
            image="fa fa-ellipsis-v"
            label="P"
            name="panels"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("panels");
            }}
          />

          <LeToolbarItem
            image="fa fa-table"
            label="G"
            name="grid"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("grid");
            }}
          />
          <LeToolbarItem
            image="fa fa-table"
            label="I"
            name="inputs"
            callback={name => {
              console.log("NAME ", name);
              router.stateService.go("inputs");
            }}
          />

          <LeToolbarItem
            image="fa fa-table"
            label="Table"
            name="table"
            callback={name => {
              router.stateService.go("table");
            }}
          />
        </LeToolBar>

        <UIRouter router={router}>
          <UIView />
        </UIRouter>
      </div>
    );
  }
}

export default App;
