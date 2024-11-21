/***
    http://www.minifier.org/
***/
(function() {
    var ad_id = "";
    var event_name = (utag_data.event_name || "");
    var patt = new RegExp("^(ad_insertion_step1|ad_insertion_error|ad_insertion_confirmation|ad_modification_confirmation)$");
    if (patt.test(event_name)) {
        ad_id = "0";
    }
    return (ad_id || utag_data.ad_id || "");
})();
