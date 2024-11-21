/***
    http://www.minifier.org/
***/
(function() {
    var result = utag_data.brand || "";
    var version = utag_data.version || "";
    if (version !== "") {
        result += " " + version;
    }
    return result;
})();
