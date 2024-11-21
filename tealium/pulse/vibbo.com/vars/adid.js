/***
    http://www.minifier.org/
***/
(function() {
    var ad_id = (utag_data.ad_id || utag_data.list_id || "").replace("sm-", "");
    if (ad_id === "") {
        var event_name = (utag_data.event_name || "");
        var patt = new RegExp("^(user_new_ad_create_step1|user_new_ad_create_step2|user_new_ad_created)$");
        if (patt.test(event_name)) {
            ad_id = "0";
        }
    }
    return ad_id;
})();
