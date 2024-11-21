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
                this.intent = (e.currentTarget.className.indexOf("active") >= 0 ? "Unsave" : "Save");
            } else if (this.target === "list") {
                target.items.filters = {
                    "query": (this.data.spt_keywords || ""),
                    "region": (this.data.spt_region || ""),
                    "locality": (this.data.spt_locality || ""),
                    "adType":  (this.data.spt_ad_type || ""),
                    "minPrice": (this.data.spt_max_price || ""),
                    "maxPrice": (this.data.spt_min_price || ""),
                    "minSurface": (this.data.spt_min_surface || ""),
                    "maxSurface": (this.data.spt_max_surface || ""),
                    "minRooms": (this.data.spt_min_rooms || ""),
                    "maxRooms": (this.data.spt_max_rooms || ""),
                    "propertyType": (this.data.spt_property_type || "")
                };
            }
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, this.intent, target).send();
            AutoTrack.activity().events.trackBasic(this.intent, this.type, this.id, target.items).send();
        }
    };

    var i = 0;
    var element = null;
    var dataPulse = {"site" : "fotocasaes", "data": data};
    dataPulse.identifier = ".fc-Save-search a";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (utag_data["dom.url"] || "");
        dataPulse.type = "Listing";
        dataPulse.intent = "Save";
        dataPulse.target = "list";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
    dataPulse.identifier = "#deleteSearch";
    items = document.body.querySelectorAll(".user-search");
    for (i = 0; i < items.length; i++) {
        element = (items[i].querySelector("a[id*='ShowSearchResults']") || "");
        dataPulse.id = (element.href || "");
        dataPulse.type = "Listing";
        dataPulse.intent = "Unsave";
        dataPulse.target = "list";
        element = items[i].querySelector("a[id*='deleteSearch']");
        if (element) {
            element.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }
    dataPulse.identifier = "#liFavorite";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
