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
                target.items.name = this.name;
                if (this.data.event_name === "detail") {
                    var element = e.currentTarget.querySelector(".js-favoriteIcon").getAttribute("xlink:href");
                    this.intent = (element ? element.indexOf("star-full") >= 0 ? "Unsave" : "Save" : "Save");
                } else if (this.data.event_name === "list") {
                    this.intent = (e.currentTarget.className.indexOf("1_xs") >= 0 ? "Unsave" : "Save");
                }
            } else if (this.target === "list") {
                target.items.filters = {
                    "query": (utag_data.search_terms || ""),
                    "region": (utag_data.region_level2 || ""),
                    "adType": "Sell",
                    "minYear": (utag_data.year_min || ""),
                    "maxYear": (utag_data.year_max || ""),
                    "minPrice": (utag_data.price_min || ""),
                    "maxPrice": (utag_data.price_max || ""),
                    "minMileage": (utag_data.km_min || ""),
                    "maxMileage": (utag_data.km_max || ""),
                    "fuelType": (utag_data.fuel || ""),
                    "transmission": (utag_data.transmission || "")
                };
            }
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, this.intent, target).send();
            AutoTrack.activity().events.trackBasic(this.intent, this.type, this.id, target.items).send();
        }
    };
    var i = 0;
    var element = null;
    var dataPulse = {"site" : "cochesnet", "data": data};
    dataPulse.identifier = "#btnSaveAlert";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (utag_data["dom.url"] || "");
        dataPulse.name = "";
        dataPulse.type = "Listing";
        dataPulse.intent = "Save";
        dataPulse.target = "list";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
    dataPulse.identifier = "#linkDeleteAlert";
    items = document.body.querySelectorAll(".alerta");
    for (i = 0; i < items.length; i++) {
        element = (items[i].querySelector("a.blue-color") || "");
        dataPulse.id = (element.href || "");
        dataPulse.name = "";
        dataPulse.type = "Listing";
        dataPulse.intent = "Unsave";
        dataPulse.target = "list";
        element = items[i].querySelector(dataPulse.identifier);
        if (element) {
            element.parentNode.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }
    dataPulse.identifier = ".mt-CardAd-favorite";
    items = document.body.querySelectorAll(".mt-SerpList-item");
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (items[i].id || "" ? items[i].id.replace("#", "") : "");
        element = (items[i].querySelector(".mt-CardAd-titleHiglight") || "");
        dataPulse.name = (element.innerHTML || "");
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        element = items[i].querySelector(dataPulse.identifier);
        if (element) {
            element.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }
    dataPulse.identifier = ".js-checkFavorite";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title || "");
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
