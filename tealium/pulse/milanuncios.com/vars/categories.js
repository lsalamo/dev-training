/***
    http://www.minifier.org/
***/
(function() {
    var result = (window.top.utag_data.category || "");
    var cat = (window.top.utag_data.subcategory1 || "");
    if (cat !== "") {
        result += " > " + cat;
        cat = (window.top.utag_data.subcategory2 || "");
        if (cat !== "") {
            result += " > " + cat;
            cat = (window.top.utag_data.subcategory3 || "");
            if (cat !== "") {
                result += " > " + cat;
            }
        }
    }
    var getIsCategoryRequired = function() {
        var result = false;
        var patt = new RegExp("^(/textos-del-anuncio|/publicado)");
        var pathname = window.location.pathname.toLowerCase();
        result = patt.test(pathname);
        if (!result) {
            var event_name = (window.top.utag_data.event_name || "").toLowerCase();
            result = (event_name === "detail");
        }
        if (!result) {
            var qp_avisoren = (utag_data["qp.avisoren"] || "");
            if ((qp_avisoren !== "") && (pathname.indexOf("/mis-anuncios") >= 0)) {
                result = true;
            }
        }
        return result;
    };
    return (result.toLowerCase() || (getIsCategoryRequired() ? "todas las categor\xEDas" : ""));
})();
