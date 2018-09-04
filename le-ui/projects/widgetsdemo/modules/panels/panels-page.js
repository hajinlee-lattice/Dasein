import React, { Component } from '../../../common/react-vendor';
import '../../../common/widgets/layout/layout.scss'
import './panel-page.scss';
import Aux from '../../../common/widgets/hoc/_Aux';

import LeMenu from '../../../common/widgets/menu/le-menu'
import LeMenuItem from '../../../common/widgets/menu/le-menu-item';
import LeButton from '../../../common/widgets/buttons/le-button';
import LeTile from '../../../common/widgets/container/tile/le-tile';
import LeTileHeader from '../../../common/widgets/container/tile/le-tile-header';
import LeTileBody from '../../../common/widgets/container/tile/le-tile-body';
import LeTileFooter from '../../../common/widgets/container/tile/le-tile-footer';

export default class PanelsPage extends Component {

    render() {
        const editConfig = {
            lable: "",
            classNames: ['button', 'borderless-button'],
            image: "fa fa-pencil-square-o"
        }
        return (
            <Aux>
                <div className="le-flex-v-panel center-h main-p">
                    <div className="le-flex-h-panel center-v">
                        <div className="test">Test1 </div>
                        <div className="test">Test2 </div>
                    </div>
                    <div className="le-flex-h-panel main-panel" >

                        <ul className="le-flex-h-panel flex-content center">
                            <li>This is OK</li>
                            <li>This ia a button</li>
                            <li>This ia a nother button</li>
                            <li>Test1</li>
                        </ul>

                        <div className="le-flex-h-panel center test"><div><button>Test1</button></div></div>
                    </div>
                </div>

            </Aux>
        );
    }
}