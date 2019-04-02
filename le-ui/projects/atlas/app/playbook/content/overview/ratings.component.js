import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeBarchart from "common/widgets/charts/le-barchart";
import { get } from 'lodash';

class RatingsComponent extends Component {
    constructor(props) {
        super(props);

    }

    getRatings() {
        console.log(this.props.play);
        if(_.get(this.props, 'play.ratings')) {
            var ratings = {};
            for(var i in this.props.play.ratings) {
                var rating = this.props.play.ratings[i];
                ratings[i] = {
                    label: rating.bucket,
                    value: rating.count
                }
            }
            return (
                <LeBarchart data={ratings}></LeBarchart>
            );
        }
    }

    render() {
        return (
            <LeVPanel>
                <h2>Account Ratings</h2>
                <h3>model name</h3>
                <div>
                    {this.getRatings()}
                </div>
            </LeVPanel>
        );
    }
}
export default RatingsComponent;