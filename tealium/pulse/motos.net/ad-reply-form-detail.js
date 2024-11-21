var sptCategory = (b.spt_format_category || "");
(function(category) {
    var getItems = function(id) {
        var items = document.body.querySelectorAll(id);
        return items;
    };
    var setClickToPulse = function() {
        var site = "motosnet";
        var id = (utag_data.ad_id || "");
        if (id !== "") {
            var target = {
                items: [{
                    "@id": "sdrn:" + site + ":classified:" + id,
                    "@type": "ClassifiedAd",
                    "category": this.cat
                }]
            };
            AutoTrack.activity().events.trackBasic("View", "Form", id, target, "Send").send();
        }
    };
    var items = getItems("#main.ficha a.new");
    if (items.length === 2) {
        var data = {
            "cat" : category
        };
        items[0].addEventListener('click', setClickToPulse.bind(data));
    }
})(sptCategory);
