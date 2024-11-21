var sptCategory = (b.spt_format_category || "");
(function(category) {
    var setClickToPulse = function() {
        if (this.id !== "") {
            var site = "motosnet";
            var intent = "Send";
            var type = "Message";
            var target = {
                items: [{
                    "@id": "sdrn:" + site + ":classified:" + this.id,
                    "@type": "ClassifiedAd",
                    "category": this.cat,
                    "name": this.name
                }]
            };
            AutoTrack.activity().events.trackBasic("View", "Confirmation", this.id, target, intent).send();
            target = {
                "inReplyTo": {
                    "@id": "sdrn:" + site + ":classified:" + this.id,
                    "@type": "ClassifiedAd",
                    "category": this.cat,
                    "name": this.name
                },
                "@id": "sdrn:" + site + ":message:" + this.id,
                "@type": "Message"
            };
            AutoTrack.activity().events.trackBasic(intent, type, this.id, target).send();
        }
    };
    var items = document.body.querySelectorAll("#bt_contactar");
    for (var i = 0; i < items.length; i++) {
        var data = {
            "id": (utag_data.ad_id || ""),
            "name": (utag_data.ad_title || ""),
            "cat" : category
        };
        items[i].addEventListener('click', setClickToPulse.bind(data));
    }
})(sptCategory);
