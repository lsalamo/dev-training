(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var target = {};
            target.items = {
                "@id": "sdrn:" + this.site + ":classified:" + this.id,
                "@type": "ClassifiedAd",
                "category": (this.data.spt_format_category || ""),
                "name": this.name
            };
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, "Share", target).send();
            AutoTrack.activity().events.trackBasic("Share", "ClassifiedAd", this.id, target.items).send();
        }
    };
    var i = 0;
    var dataPulse = {"site" : "vibbocom", "data": data};
    dataPulse.identifier = ".ad-sharing__links a";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
