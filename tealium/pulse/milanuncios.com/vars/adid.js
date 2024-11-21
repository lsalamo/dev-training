/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var utag_data_window_top = window.top.utag_data;
    var pathname = window.location.pathname.toLowerCase();
    var patt = new RegExp("^(/textos-del-anuncio|/publicado)");
    if (patt.test(pathname)) {
        result = "0";
    }
    if (result === "") {
        var qp_avisoren = (utag_data_window_top["qp.avisoren"] || "");
        if ((qp_avisoren !== "") && (pathname.indexOf("/mis-anuncios") >= 0)) {
            result = "0";
        }
    }
    return (result || utag_data_window_top.ad_id || "");
})();
