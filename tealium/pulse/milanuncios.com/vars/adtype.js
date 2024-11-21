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
    var item = getItemValue(utag_data.sell_type);
    var cat = getItemValue(utag_data.category_id);
    if (cat === "30") {
        item = "jobOffer";
    } else if (item === "demanda") {
        item = "buy";
    } else if (item === "oferta") {
        item = "sell";
    } else {
        item = "";
    }
    return item;
})();
