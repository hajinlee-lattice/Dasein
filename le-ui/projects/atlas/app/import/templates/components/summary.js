import React, {
    Component,
    react2angular
} from "../../../../../common/react-vendor";
import "./summary.scss";
import Aux from "../../../../../common/widgets/hoc/_Aux";
import Observer from "../../../../../common/app/http/observer";
import httpService from "../../../../../common/app/http/http-service";
import SummaryBox from "./summary-box";

export default class SummaryContainer extends Component {
    constructor(props) {
        super(props);
        this.state = {
            loading: true,
            accountsCount: 0,
            contactsCount: 0,
            productPurchasesCount: 0,
            productsCount: 0,
            OrphanContacts: 0,
            OrphanTransactions: 0,
            UnmatchedAccount: 0
        };
    }
    componentWillUnmount() {
        httpService.unsubscribeObservable(this.observer);
    }
    componentDidMount() {
        this.observer = new Observer(response => {
            console.log("counts observer:", response);
            this.setState({
                loading: false,
                accountsCount: response.data.AccountCount,
                contactsCount: response.data.ContactCount,
                productPurchasesCount: response.data.TransactionCount,
                productsCount: response.data.ProductCount
            });
        });
        httpService.get("/pls/datacollection/status", this.observer);

        this.orphanObserver = new Observer(response => {
            console.log("orphan observer:", response);
            this.setState({
                OrphanContacts: response.data["Orphan Contacts"],
                OrphanTransactions: response.data["Orphan Transactions"],
                UnmatchedAccount: response.data["Unmatched Accounts"]
            });
        });
        httpService.get(
            "/pls/datacollection/orphans/count",
            this.orphanObserver
        );
    }
    render() {
        return (
            <Aux>
                <div className="le-summary-container le-flex-v-panel">
                    <div className="le-summary-header le-flex-v-panel">
                        <p className="title">Import Templates</p>
                        <p className="description">
                            Field mapping templates store your import
                            configuration for each data object. These templates
                            support all manual and automated data import jobs.
                        </p>
                    </div>
                    <div className="le-flex-h-panel boxes-container">
                        <SummaryBox
                            name="Accounts"
                            loading={this.state.loading}
                            count={this.state.accountsCount}
                            asidename="Unmatched"
                            asidecount={this.state.UnmatchedAccount}
                        />
                        <SummaryBox
                            name="Contacts"
                            loading={this.state.loading}
                            count={this.state.contactsCount}
                            asidename="Orphaned"
                            asidecount={this.state.OrphanContacts}
                        />
                        <SummaryBox
                            name="Product Purchases"
                            loading={this.state.loading}
                            count={this.state.productPurchasesCount}
                            asidename="Transactions"
                            asidecount={this.state.OrphanTransactions}
                        />
                        <SummaryBox
                            name="Products"
                            loading={this.state.loading}
                            count={this.state.productsCount}
                        />
                    </div>
                </div>
            </Aux>
        );
    }
}

angular
    .module("le.summary", [])
    .component("leSummaryComponent", react2angular(SummaryContainer, [], []));
