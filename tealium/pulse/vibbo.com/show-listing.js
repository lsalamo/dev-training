(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var target = {};
            target.items = {
                "@id": "sdrn:" + this.site + ":listing:" + this.id,
                "@type": "Listing",
                "category": (this.data.spt_format_category || ""),
                "name": this.name
            };
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, "Show", target).send();
            AutoTrack.activity().events.trackBasic("Show", "Listing", this.id, target.items).send();
        }
    };
    var i = 0;
    var dataPulse = {"site" : "vibbocom", "data": data};

    dataPulse.identifier = ".sellerBox__store__info__adsLink a";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }

    dataPulse.identifier = ".sellerBox__info__adsNumber a";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);  
