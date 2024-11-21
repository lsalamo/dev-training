/***
    http://www.minifier.org/
***/
(function() {
    var result = "Web";
    var product_type = parseInt((utag_data["dom.viewport_width"] || "") || "");
    if (product_type !== "" && product_type <= 640) {
        result = "M-Site";
    }
    return result;
})();
