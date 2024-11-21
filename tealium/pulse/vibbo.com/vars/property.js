/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var cat = (utag_data.category_id || "");
    if (String(cat) === "107") {
        result = (utag_data.subcategory1 || "");
    }
    return result.toLowerCase();
})();
