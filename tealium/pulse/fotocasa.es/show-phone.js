(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var target = {};
            target.items = {
                "@type": "PhoneContact",
                "inReplyTo": {
                    "@id": "sdrn:" + this.site + ":classified:" + this.id,
                    "@type": "ClassifiedAd",
                    "category": (this.data.spt_format_category || ""),
                    "name": this.name
                }
            };
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, "Show", target).send();
            AutoTrack.activity().events.trackBasic("Show", "PhoneContact", this.id, target.items).send();
        }
    };

    var i = 0;
    var element = null;
    var dataPulse = {"site" : "fotocasaes", "data": data};

    dataPulse.identifier = "#ctl00_buttonContextualShowPhone a";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
