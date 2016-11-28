import { Injectable } from "@angular/core";
import { Http, Headers, Response, Request, BaseRequestOptions, RequestMethod } from "@angular/http";
import { Observable } from "rxjs";
import { StorageUtility } from "../utilities/storage.utility";

@Injectable()
export class HttpClient {
    constructor(
        private http: Http, 
        private storageUtility: StorageUtility
    ) {}

    public get(url:string): Observable<Response> {
        return this.request(url, RequestMethod.Get);
    }

    public post(url:string, body:any): Observable<Response> {   
        return this.request(url, RequestMethod.Post, body);
    }

    public put(url:string, body:any): Observable<Response> {   
        return this.request(url, RequestMethod.Put, body);
    }

    private request(url:string, method:RequestMethod, body?:any): Observable<Response>{
        let headers = new Headers();
        let AuthToken = this.storageUtility.getTokenDocument();
        
        headers.append('Authorization', AuthToken);
        
        let options = new BaseRequestOptions();
        
        options.headers = headers;
        options.url = url;
        options.method = method;
        options.body = body;
        options.withCredentials = true;

        let request = new Request(options);

        return this.http.request(request);
    }
}