(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var targetPageId = (this.target === "list" ? "listing" : "classified");
            var targetPageType = (this.target === "list" ? "Listing" : "ClassifiedAd");
            var target = {};
            target.items = {
                "@id": "sdrn:" + this.site + ":" + targetPageId + ":" + this.id,
                "@type": targetPageType,
                "category": (this.data.spt_format_category || "")
            };
            if (this.target === "detail") {
                target.items.name = (this.data.ad_title || "");
            } else if (this.target === "list") {
                target.items.filters = {
                    "query": (this.data.spt_keywords || ""),
                    "region": (this.data.spt_region || ""),
                    "locality": (this.data.spt_locality || ""),
                    "adType":  (this.data.spt_ad_type || ""),
                    "experience": (this.data.spt_experience || ""),
                    "contractType": (this.data.spt_contract_type || ""),
                    "contractLength": (this.data.spt_contract_length || ""),
                    "education": (this.data.spt_education || "")
                };
            }
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, this.intent, target).send();
            AutoTrack.activity().events.trackBasic(this.intent, this.type, this.id, target.items).send();
        }
    };

    var i = 0;
    var element = null;
    var dataPulse = {"site" : "infojobsnet", "data": data};
    dataPulse.identifier = "#save_busqueda";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (utag_data["dom.url"] || "");
        dataPulse.type = "Listing";
        dataPulse.intent = "Save";
        dataPulse.target = "list";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
    dataPulse.identifier = "#btn-eliminar";
    items = document.body.querySelectorAll("#ul-alertas li[id*='alerta']");
    for (i = 0; i < items.length; i++) {
        element = (items[i].querySelector("h3 a") || "");
        dataPulse.id = (element.href || "");
        dataPulse.type = "Listing";
        dataPulse.intent = "Unsave";
        dataPulse.target = "list";
        element = items[i].querySelector(dataPulse.identifier);
        if (element) {
            element.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }
}
)(b);
