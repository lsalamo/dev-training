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

    var element = null;
    var dataPulse = {"site" : "cochesnet", "data": data};

    dataPulse.identifier = "#_ctl0_ContentPlaceHolder1_SellerPhone_lnkPhone1";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (var i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }

    dataPulse.identifier = "#_ctl0_ContentPlaceHolder1_AdSeller_SellerPhone_lnkPhone1";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (var j = 0; j < items.length; j++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[j].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }

    dataPulse.identifier = "#_ctl0_ContentPlaceHolder1_AdHeader_PhoneCall";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (var k = 0; k < items.length; k++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[k].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }

    dataPulse.identifier = "#_ctl0_ContentPlaceHolder1_SellerPhone_ViewPhone";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (var l = 0; l < items.length; l++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[l].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
