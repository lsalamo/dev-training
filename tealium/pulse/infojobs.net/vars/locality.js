/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var getFirstValueArray = function(item) {
        var result = String(item).replace(/-/g, " ").toLowerCase();
        var itemList = result.split(",");
        if (itemList.length > 0) {
            result = itemList[0];
        }
        return result;
    };
    var item = (utag_data.es_sch_ad_city || "");
    result = getFirstValueArray(item);
    return result;
})();
