import httpService from "../../../../common/app/http/http-service";
import { Observable } from "../../../../common/network.vendor";
import Observer from "../../../../common/app/http/observer";

const URLS = { temlpateUrl: "/pls/cdl/s3import/template" };
// const temlpateUrl = "/pls/cdl/s3import/template";
const TemplateService = () => {
  return new TemplateServiceClass();
};
export default TemplateService;

class TemplateServiceClass {
  constructor() {}

  getTemplates(observer) {
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok); 
        observer.next(response);

    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.get(URLS.temlpateUrl, ok);
  }
}
