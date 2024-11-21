(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var target = {};
            target.items = [{
                "@id": "sdrn:" + this.site + ":message:" + this.id,
                "@type": "Message",
                "inReplyTo": {
                    "@id": "sdrn:" + this.site + ":classified:" + this.id,
                    "@type": "ClassifiedAd",
                    "category": (this.data.spt_format_category || ""),
                    "name": this.name
                }
            }];
            AutoTrack.activity().events.trackBasic("View", "Form", this.id, target, "Send").send();
        }
    };
    var i = 0;
    var dataPulse = {"site" : "vibbocom", "data": data};
    dataPulse.identifier = "#sellerBox__startChat";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }

    dataPulse.identifier = "#sellerBox-show-chatFormMobile";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
