/***
    http://www.minifier.org/
***/
(function() {
    var result = (utag_data.category || "");
    var cat = (utag_data.subcategory1 || "");
    if (cat !== "") {
        result += " > " + cat;
        cat = (utag_data.subcategory2 || "");
        if (cat !== "") {
            result += " > " + cat;
            cat = (utag_data.subcategory3 || "");
            if (cat !== "") {
                result += " > " + cat;
            }
        }
    }
    var patt = null;
    var event_name = (utag_data.event_name.toLowerCase() || "");
    var isCategoryRequired = false;
    patt = new RegExp("^(detail|user_new_ad_create_step1|user_new_ad_create_step2|user_new_ad_created)$");
    if (patt.test(event_name)) {
        isCategoryRequired = true;
    }
    return (result || (isCategoryRequired ? "todas las categor\xEDas" : ""));
})();
