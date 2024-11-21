/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    switch(String(utag_data.fuel_id)) {
        case "1":
            result = "Gasolina";
            break;
        case "2":
            result = "Di\xE9sel";
            break;
        case "3":
            result = "El\xE9ctrico/H\xEDbrido";
            break;
        case "4":
            result = "Otros";
            break;
    }
    return result;
})();
