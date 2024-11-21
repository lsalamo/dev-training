/***
    http://www.minifier.org/
***/
(function() {
    var result = [];
    var region_level2 = (utag_data.region_level2 || "");
    var city = (utag_data.city || "");
    if (region_level2 !== "") {
        result.push(region_level2);
    }
    if (city !== "") {
        result.push(city);
    }
    return result.join(", ");
})();
