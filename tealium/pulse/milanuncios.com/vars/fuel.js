/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var fuel = (utag_data.fuel || "");
    if (fuel !== "") {
        result = (/^gasolina$/i.test(fuel) ? "gasoline" : "");
        if (!result) result = (/^diesel$/i.test(fuel) ? "diesel" : "");
        if (!result) result = (/^eléctrico$/i.test(fuel) ? "electricity" : "");
        if (!result) result = (/^glp$/i.test(fuel) ? "natural-gas" : "");
        if (!result) result = (/^híbrido$/i.test(fuel) ? "hybrid" : "");
        if (!result) result = "other";
    }
    return result;
})();
