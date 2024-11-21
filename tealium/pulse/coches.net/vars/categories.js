/***
    http://www.minifier.org/
***/
(function() {
    var patt = null;
    var result = "";
    var cat = (utag_data.category_level1 || "");
    if (cat !== "") {
        result = cat;
        patt = new RegExp("Autom\xF3vil");
        if (patt.test(cat)) {
            cat = (utag_data.car_body || "");
        } else {
            cat = (utag_data.category_level2 || "");
        }
        if (cat !== "") {
            result += " > " + cat;
        }
    }
    if (result === "") {
        //workaround
        if (window.location.pathname.indexOf("segunda-mano") >= 0) {
            result = "Autom\xF3vil";
            cat = (utag_data.car_body || "");
            if (cat !== "") {
                patt = new RegExp("^(4x4|Cabrio|Monovolumen|Familiar|Berlina|Coupé|Pick Up)$");
                if (patt.test(cat)) {
                    result += " > " + cat;
                }
            }
        } else if (window.location.pathname.indexOf("vehiculos-industriales") >= 0) {
            result = "Veh\xedculos Industriales";
        } else if (window.location.pathname.indexOf("clasicos-competicion") >= 0) {
            result = "Cl\xe1sicos y competici\xf3n";
        } else if (window.location.pathname.indexOf("autocaravanas-y-remolques") >= 0) {
            result = "Autocaravanas - Remolques";
        } else if (window.location.pathname.indexOf("sin-carnet") >= 0) {
            result = "Sin carnet";
        } else if (window.location.pathname.indexOf("accesorios") >= 0) {
            result = "Accesorios";
        }
    }
    var event_name = (utag_data.event_name.toLowerCase() || "");
    var isCategoryRequired = false;
    patt = new RegExp("^(detail|ad_insertion_step1|ad_insertion_error|ad_insertion_confirmation|ad_modification_confirmation)$");
    if (patt.test(event_name)) {
        isCategoryRequired = true;
    }
    return (result || (isCategoryRequired ? "todas las categor\xEDas" : ""));
})();
