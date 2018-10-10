var product_code = "AP-PUJQ09NPTAAO-2",
    disable_zendesk = false,
    disable_aptrinsic = false;

if(['app.lattice-engines.com'].indexOf(window.location.host) >= 0) { // production
    product_code = "AP-RR8EQLTW2VP9-2";
} else if(['localhost:3000','localhost:3001'].indexOf(window.location.host) >= 0) { // dev
    disable_zendesk = true;
    disable_aptrinsic = true;
}
console.log('Aptrinsic:', window.location.host, product_code, 'Enabled:', !disable_aptrinsic);

if(!disable_aptrinsic) { (function(n,t,a,e){var i="aptrinsic";n[i]=n[i]||function(){ (n[i].q=n[i].q||[]).push(arguments)},n[i].p=e; var r=t.createElement("script");r.async=!0,r.src=a+"?a="+e; var c=t.getElementsByTagName("script")[0];c.parentNode.insertBefore(r,c) })(window,document,"https://web-sdk.aptrinsic.com/api/aptrinsic.js",product_code); }

if(!disable_zendesk) { window.zE||(function(e,t,s){var n=window.zE=window.zEmbed=function(){n._.push(arguments)}, a=n.s=e.createElement(t),r=e.getElementsByTagName(t)[0];n.set=function(e){ n.set._.push(e)},n._=[],n.set._=[],a.async=true,a.setAttribute("charset","utf-8"), a.src="https://static.zdassets.com/ekr/asset_composer.js?key="+s, n.t=+new Date,a.type="text/javascript",r.parentNode.insertBefore(a,r)})(document,"script","7b73cc63-fb76-4deb-9df8-09130f1d9931"); }
console.log('aptrinsic loaded');