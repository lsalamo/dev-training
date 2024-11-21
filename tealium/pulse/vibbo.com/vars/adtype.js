/***
    http://www.minifier.org/
    enum: buy, sell, rent, let, lease, swap, give, jobOffer
***/
(function() {
    var result = "";
    switch(String(utag_data.transaction_id)) {
        case "1":
            result = "sell";
            break;
        case "2":
            result = "buy";
            break;
        case "5":
        case "3":
            result = "rent";
            break;
        default:
            var category = (utag_data.subcategory1_id || "");
            if (category === "11") {
                result = "jobOffer";
            }
    }
    return result;
})();
