/***
    http://www.minifier.org/
    enum: buy, sell, rent, let, lease, swap, give, jobOffer
***/
(function() {
    var result = "";
    switch(String(utag_data.transmission_id)) {
        case "1":
            result = "Autom\xE1tico";
            break;
        case "2":
            result = "Manual";
            break;
    }
    return result;
})();
