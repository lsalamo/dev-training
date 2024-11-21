/***
    http://www.minifier.org/
***/
(function() {
    var cat1 = "";
    var cat2 = "";
    var isCategory = function(cat) {
        var category = cat || "";
        return (category !== "");
    };
    var getCategory = function() {
        if (isCategory(utag_data.category_level1)) {
            cat1 = utag_data.category_level1;
            if ((isCategory(utag_data.category_level2)) && (utag_data.category_level1 !== utag_data.category_level2)) {
                cat2 = " > " + utag_data.category_level2;
            }
        } else {
            return "Motos";
        }

        return cat1 + cat2;
    };
    return getCategory();
})();
