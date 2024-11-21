/***
    http://www.minifier.org/
***/
(function() {
    var result = "";
    var item = document.body.querySelectorAll(".re-Breadcrumb-text");
    if (item.length === 1) {
        result = item[0].innerText;
    }
    return result;
})();
