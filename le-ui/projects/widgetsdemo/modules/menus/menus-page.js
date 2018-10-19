import React, { Component } from '../../../common/react-vendor';
import LeMenu from '../../../common/widgets/menu/le-menu';
import LeMenuItem from '../../../common/widgets/menu/le-menu-item';
import LeToolbar, { HORIZONTAL } from '../../../common/widgets/toolbar/le-toolbar';

import LeMenuButton from '../../../common/widgets/menu/le-menu-button';

import './menus-page.scss';

export default class MenusPage extends Component {

    constructor(props) {
        super(props);
    }


    render() {

        return (
            <div className="container">
                <h1>Menus</h1>
                <LeToolbar>
                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">
                        <LeMenuItem
                            name="users"
                            image="fa fa-users"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} >
                        </LeMenuItem>

                        <LeMenuItem
                            name="tenants"
                            label="Switch Tenants"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />

                        <LeMenuItem
                            name="password"
                            label="Update Password"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />
                        <LeMenuItem
                            name="sso"
                            label="SSO Settings"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />
                        <LeMenuItem
                            image="fa fa-sign-out"
                            name="signout"
                            label="Sign Out"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />
                    </LeMenu>

                    <LeMenuButton config={JSON.parse('{"primaryInfo": "User Name", "secondaryInfo": "Tenant name"}')}>
                        <LeMenuItem
                            name="sso"
                            label="SSO Settings"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />
                        <LeMenuItem
                            image="fa fa-sign-out"
                            name="signout"
                            label="Sign Out"
                            callback={(name) => {
                                console.log('NAME ', name);
                            }} />
                    </LeMenuButton>
                </LeToolbar>

            </div>


        );
    }
}