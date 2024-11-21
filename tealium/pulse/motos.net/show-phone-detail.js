var sptCategory = (b.spt_format_category || "");
(function(category) {
    var setClickToPulse = function(e) {
        if (this.id !== "") {
            var site = "motosnet";
            var intent = "Show";
            var type = "PhoneContact";
            var target = {
                items: {
                    "@type": type,
                    "inReplyTo": {
                        "@id": "sdrn:" + site + ":classified:" + this.id,
                        "@type": "ClassifiedAd",
                        "category": this.cat,
                        "name": this.name
                    }
                }
            };
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "UIElement", this.identifier, intent, target).send();
            AutoTrack.activity().events.trackBasic(intent, type, this.id, target.items).send();
        }
    };
    var items = document.body.querySelectorAll(".lnk_vertelf");
    for (var i = 0; i < items.length; i++) {
        var data = {
            "id": (utag_data.ad_id || ""),
            "name": (utag_data.ad_title || ""),
            "cat": category,
            "identifier": ".lnk_vertelf"
        };
        items[i].addEventListener('click', setClickToPulse.bind(data));
    }
}
)(sptCategory);
