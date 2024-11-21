/***
    http://www.minifier.org/
***/
(function() {
    var getFirstValueArray = function(val) {
        var result = String(val).split(",");
        if (result.length > 0) {
            result = result[0];
        }
        return result;
    };

    var result = getFirstValueArray(utag_data.es_sch_ad_category_level1 || "");
    var cat = getFirstValueArray(utag_data.es_sch_ad_category_level2 || "");
    if (cat !== "") {
        result += " > " + cat;
    }
    var isCategoryRequired = (utag_data.es_sch_event_name === "search_results_offer_detail" ? true : false);
    return (result.replace(/-/g, " ").toLowerCase() || (isCategoryRequired ? "todas las categor\xEDas" : ""));
})();
