import React, { Component } from "common/react-vendor";
import "./system-creation.component.scss";
import LeVPanel, { SPACEBETWEEN } from "common/widgets/container/le-v-panel";
import LeHPanel, {SPACEAROUND} from "common/widgets/container/le-h-panel";
import GridLayout, {
  MIN_GAP
} from "common/widgets/container/grid-layout.component";
import LeCard from "common/widgets/container/card/le-card";
import LeCardImg from "common/widgets/container/card/le-card-img";
// import LeCardBody from 'common/widgets/container/card/le-card-body';
// import { CENTER } from 'common/widgets/container/le-alignments';
// import { LeToolBar, SPACE_BETWEEN } from "../../../../../common/widgets/toolbar/le-toolbar";
import LeButton from "../../../../../common/widgets/buttons/le-button";
import ReactRouter from "atlas/react/router";
import LeInputText from "../../../../../common/widgets/inputs/le-input-text";
import ReactMainContainer from "../../../react/react-main-container";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { CENTER } from "../../../../../common/widgets/container/le-alignments";
import LeSimpleInputText from "../../../../../common/widgets/inputs/le-simple-input-text";
export default class SystemCreationComponent extends Component {
  constructor(props) {
    super(props);
    this.validate = this.validate.bind(this);
    this.systemsObj = [
      {
        name: "Salesforce",
        img: "/atlas/assets/images/salesforce_small.png",
        img_select: '/atlas/assets/images/salesforce_small_white.png',
        text: "Text"
      },
      {
        name: "Marketo",
        img: "/atlas/assets/images/marketo_small.png",
        img_select: "/atlas/assets/images/marketo_small_white.png",
        text: "Text"
      },
      { 
        name: "Eloqua", 
        img: "/atlas/assets/images/eloqua_small.png",
        img_select: "/atlas/assets/images/eloqua_small_white.png", 
        text: "Text" 
      },
      {
        name: "Other",
        img: "/atlas/assets/images/other_systems.png",
        img_select: "/atlas/assets/images/other_systems_white.png",
        text: "Text"
      }
    ];
    this.state = { systemSelected: undefined, valid: false, newSystemName: "" };
  }

  componentDidMount() {}
  componentWillUnmount() {}
  validate() {
    if (this.state.systemSelected && this.state.newSystemName.trim() != "") {
      this.setState({ valid: true });
    } else {
      this.setState({ valid: false });
    }
  }

  getSystemSupported() {
    let systems = [];
    this.systemsObj.forEach(system => {
      console.log(system);
      systems.push(
        <div onClick={() =>{
          this.setState({ systemSelected: system }, this.validate);
        }} className={`${"system-card-fix"} ${
          (this.state.systemSelected && this.state.systemSelected.name) ==
          system.name
            ? "selected"
            : ""
        }`}>
          <LeCard
          classNames={`${"system-card"}`}
        >
          <LeCardImg src={`${(this.state.systemSelected && this.state.systemSelected.name) ==
          system.name ?system.img_select: system.img}`} classNames="system-image" />
        
        </LeCard>
        </div>
      );
    }, this);

    // systems.push(<p>APIs gives us list of systems?</p>);
    return systems;
  }

  render() {
    return (
      <ReactMainContainer className="system-creation">
          
        <LeVPanel hstretch={true} vstretch={true} className="wizard-container">
        <p className="sub-header">Import Data: Add System</p>
          <LeVPanel
            vstretch={"true"}
            hstretch={"true"}
            className={"system-creation"}
          >
            
            <p className="le-sub-header">Select System</p>
            <LeHPanel hstretch={true} halignment={CENTER} className="systems-grid">
              {this.getSystemSupported()}
            </LeHPanel>
            <p className="le-description">
              The system will automatically create default field mappings
              between Lattice and Salesforce. The system will automatically
              create 3 separate field mapping one each for Salesforce Accounts,
              Contacts & Leads.
              <br />
              <br />
              These field mappings are here to help you get you started quickly
              and can be edited anytime.
            </p>

            <LeHPanel className="system-info" hstretch={true}>
              {/* <span className="le-label">Select System</span> */}
              {/* <LeInputText
                config={{
                  label: "System Name"
                }}
                callback={val => {
                  // console.log('VALUE ', val);
                  this.setState({ newSystemName: val }, this.validate);
                }}
              /> */}
              <LeSimpleInputText config={{label: 'System Name'}} callback={val => {
                this.setState({ newSystemName: val }, this.validate);
              }}/>
            </LeHPanel>
          </LeVPanel>
          <LeHPanel
            hstretch={true}
            halignment={SPACEBETWEEN}
            className="wizard-controlls"
          >
            <LeButton
              name="cancel"
              config={{
                label: "Cancel",
                classNames: "gray-button"
              }}
              callback={() => {
                ReactRouter.getStateService().go("templateslist");
                // alert('Call APIS');
              }}
            />
            <LeButton
              name="create"
              disabled={!this.state.valid}
              config={{
                label: "Create",
                classNames: "blue-button"
              }}
              callback={() => {
                var observer = new Observer(response => {
                  httpService.unsubscribeObservable(observer);
                  ReactRouter.getStateService().go("templateslist");
                });
                // pls/cdl/s3import/system?systemName={systemName}&systemType=
                // url, body, observer, headers
                let url = `${"/pls/cdl/s3import/system?systemDisplayName="}${
                  this.state.newSystemName
                }${"&systemType="}${this.state.systemSelected.name}`;
                httpService.post(url, {}, observer, {});
                // let config = {
                //     callback: (action) => {
                //         modalActions.closeModal(store);
                //     },
                //     template: () => {
                //         return (<p>Leo test</p>)
                //     },
                //     title: () => {
                //         return (<p>TEST</p>);
                //     },
                //     oneButton: false,
                //     hideFooter: false,
                //     size: MEDIUM_SIZE
                // }
                // modalActions.error(store, config);
              }}
            />
          </LeHPanel>
        </LeVPanel>
      </ReactMainContainer>
    );
  }
}
