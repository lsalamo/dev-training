/***
    http://www.minifier.org/
***/
(function(category) {
    var result = "";
    var item = (category || "");
    if (item !== "") {
        if (item.split(" > ").length > 0) {
            result = item.split(" > ")[0];
        }
    }
    return result;
})(b.spt_format_category);
