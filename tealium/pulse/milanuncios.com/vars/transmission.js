/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var transmission = (utag_data.transmission || "");
    if (transmission !== "") {
        result = (/^manual/i.test(transmission) ? "manual" : "");
        if (!result) result = "automatic";
    }
    return result;
})();
