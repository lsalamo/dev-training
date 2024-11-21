/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var patt = new RegExp("^(/mvc/property/ptacross|/mvc/property/EditPta)$".toLowerCase());
    var pattString = window.location.pathname.toLowerCase();
    if (patt.test(pattString)) {
        result = "0";
    }
    if (result === "") {
        patt = new RegExp("/mvc/property/ptacross|/mvc/property/EditPta".toLowerCase());
        pattString = document.referrer.toLowerCase();
        if (patt.test(pattString)) {
            result = "0";
        }
    }
    return (result || utag_data.ad_id || "");
})();
