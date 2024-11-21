/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var isValidItem = function(item) {
        var result = item || "";
        return (result !== "");
    };
    var getFirstValueArray = function(item) {
        var result = item;
        var itemList = item.split(",");
        if (itemList.length > 0) {
            result = itemList[0];
        }
        return result;
    };
    var getItemValue = function(item) {
        var result = "";
        if (isValidItem(item)) {
            result = getFirstValueArray(String(item));
            result = result.replace(/-/g, " ").toLowerCase();
        }
        return result;
    };
    result = getItemValue(utag_data.es_sch_ad_workday);
    return result;
})();
