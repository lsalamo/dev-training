/***
    http://www.minifier.org/
    enum: buy, sell, rent, let, lease, swap, give, jobOffer
***/
(function() {
    var result = "";
    switch(String(utag_data.transaction_id)) {
        case "2":
        case "3":
        case "5":
        case "7":
        case "8":
            result = "rent";
            break;
        default:
            result = "sell";
    }
    return result;
})();
