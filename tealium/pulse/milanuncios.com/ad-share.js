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
    var element = null;
    var dataPulse = {"site" : "milanuncios", "data": data};

    dataPulse.identifier = ".favourite";
    var items = document.body.querySelectorAll(".aditem");
    for (i = 0; i < items.length; i++) {
        element = (items[i].querySelector(".aditem-footer") || "");
        dataPulse.id = (element.id || "" ? element.id.replace("ph", "") : "");
        element = (items[i].querySelector("a.aditem-detail-title") || "");
        dataPulse.name = (element.innerHTML.toLowerCase() || "");
        element = (items[i].querySelector("a i[class*='share']") || "");
        if (element) {
            element.parentNode.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }

    dataPulse.identifier = ".pagAnuSocialTable button";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
