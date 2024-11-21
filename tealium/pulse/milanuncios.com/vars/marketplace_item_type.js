/***
http://www.minifier.org/
***/
(function() {
    var result = "";
    var cat = String(utag_data.category_id || "");
    var event_name = String(utag_data.event_name || "");
    if (cat !== "" && event_name === "detail") {
        switch (cat) {
            case "10":
                result = "Vehicle";
                break;
            case "20":
                result = "Property";
                break;
            case "30":
                result = "Job";
                break;
            default:
                result = "MarketplaceItem";
        }
    }
    return result;
})();
