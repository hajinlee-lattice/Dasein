var product_code = "AP-PUJQ09NPTAAO-2",
    disable_aptrinsic = false;

if(['app.lattice-engines.com'].indexOf(window.location.host) >= 0) { // production
    product_code = "AP-RR8EQLTW2VP9-2";
} else if(['localhost:3000','localhost:3001'].indexOf(window.location.host) >= 0) { // dev
    disable_aptrinsic = true;
}

if(!disable_aptrinsic) { (function(n,t,a,e){var i="aptrinsic";n[i]=n[i]||function(){ (n[i].q=n[i].q||[]).push(arguments)},n[i].p=e; var r=t.createElement("script");r.async=!0,r.src=a+"?a="+e; var c=t.getElementsByTagName("script")[0];c.parentNode.insertBefore(r,c) })(window,document,"https://web-sdk.aptrinsic.com/api/aptrinsic.js",product_code); }
