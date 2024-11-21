/***
    http://www.minifier.org/
***/
(function() {
    var getProperty = function(id) {
        var result = "";
        switch(String(id)) {
            case "1":
                result = "Obra nueva";
                break;
            case "2":
                result = "Viviendas";
                break;
            case "3":
                result = "Garajes";
                break;
            case "4":
                result = "Terrenos";
                break;
            case "5":
                result = "Locales";
                break;
            case "6":
                result = "Oficinas";
                break;
            case "7":
                result = "Trasteros";
                break;
        }
        return result;
    };

    var getPropertySub = function(id) {
        var result = "";
        id = String(id);
        if (id.split(",").length > 0) {
            id = id.split(",")[0];
        }
        switch(id) {
            case "0":
                result = "Locales comerciales";
                break;
            case "1":
                result = "Pisos";
                break;
            case "2":
                result = "Apartamentos";
                break;
            case "3":
                result = "Chalets";
                break;
            case "4":
                result = "";
                break;
            case "5":
                result = "Adosados";
                break;
            case "6":
                result = "Aticos";
                break;
            case "7":
                result = "Duplex";
                break;
            case "8":
                result = "Lofts";
                break;
            case "9":
                result = "Casas rurales";
                break;
            case "16":
                result = "Naves industriales";
                break;
            case "52":
                result = "Bajos";
                break;
            case "54":
                result = "Estudios";
                break;
        }
        return result;
    };

    var getIsCategoryRequired = function() {
        var result = false;
        var patt = new RegExp("^(/mvc/property/ptacross|/mvc/property/EditPta)$".toLowerCase());
        var pattString = window.location.pathname.toLowerCase();
        if (patt.test(pattString)) {
            result = true;
        }
        if (!result) {
            pattString = document.referrer.toLowerCase();
            patt = new RegExp("/mvc/property/ptacross|/mvc/property/EditPta".toLowerCase());
            if (patt.test(pattString)) {
                result = true;
            }
        }
        if (!result) {
            pattString = (utag_data.event_name.toLowerCase() || "");
            patt = new RegExp("^(detail)$");
            if (patt.test(event_name)) {
                result = true;
            }
        }
        return result;
    };

    var property = getProperty(utag_data.property_id);
    var property_sub = getPropertySub(utag_data.property_sub_id);
    if (property_sub !== "") property_sub = " > " + property_sub;
    var result = property + property_sub;

    return (result || (getIsCategoryRequired() ? "todas las categor\xEDas" : ""));
})();
