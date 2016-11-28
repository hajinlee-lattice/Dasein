import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { HttpClient } from './interceptor.service';
import { StringsUtility } from "../utilities/strings.utility";
import { Observable } from 'rxjs/Rx';

// Import RxJs required methods
import 'rxjs/add/operator/map';

@Injectable()
export class StringsService {

    DefaultLocale: string = "en-US";

    constructor(
        private http: HttpClient, 
        private stringsUtility: StringsUtility
    ) { }

    public GetResourceStringsForLocale(locale:string = this.DefaultLocale, External:boolean = false): Promise<any> {
        let addr =  "assets/resources/" + locale + "/ResourceStrings" + (External ? 'External' : '') + ".txt";

        return  this.http
                .get(addr)
                .map(data => {
                    console.log('data', data);
                    if (data == null) {
                        return;
                    }

                    var resourceStrings = {};
                    var result = data["_body"].split("\r\n");

                    for (var x = 0; x < result.length; x++) {
                        if (result[x] !== "") {
                            var resourceString = result[x].split("=");
                            resourceStrings[resourceString[0]] = resourceString[1];
                        }
                    }

                    this.stringsUtility.setStrings(resourceStrings);
                    this.stringsUtility.resourceStringsInitialized = true;

                    return resourceStrings;
                })
                .toPromise();
    }

    private getResourceStringsAtWebAddress(webAddress): Observable<Response> {
        return this.http.get(webAddress).map((res:Response) => res);
    }
    
}