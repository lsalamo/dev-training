(function(data) {
    var setPulseTrackClick = function(e) {
        var category = (this.data.spt_format_category || "");
        if (this.id !== "" && category !== "") {
            var targetPageId = (this.target === "list" ? "listing" : "classified");
            var targetPageType = (this.target === "list" ? "Listing" : "ClassifiedAd");
            var target = {};
            target.items = {
                "@id": "sdrn:" + this.site + ":" + targetPageId + ":" + this.id,
                "@type": targetPageType,
                "category": category
            };
            if (this.target === "detail") {
                target.items.name = this.name;
                var element = null;
                if (this.data.event_name === "detail") {
                    this.intent = (e.currentTarget.className.indexOf("pagAnuFavButtonOn") >= 0 ? "Unsave" : "Save");
                } else if (this.data.event_name === "list") {
                    element = e.currentTarget.firstElementChild;
                    if (element) {
                        this.intent = (element.className.indexOf("icon-favourite-fill") >= 0 ? "Unsave" : "Save");
                    } else {
                        this.intent = "Save";
                    }
                }
            } else if (this.target === "list") {
                target.items.filters = {
                    "query": (this.data.spt_keywords || ""),
                    "region": (this.data.spt_region || ""),
                    "locality": (this.data.spt_locality || ""),
                    "adType": (this.data.spt_ad_type || ""),
                    "minPrice": (this.data.spt_max_price || ""),
                    "maxPrice": (this.data.spt_min_price || ""),
                    "minYear": (this.data.spt_min_year || ""),
                    "maxYear": (this.data.spt_max_year || ""),
                    "minSurface": (this.data.spt_min_surface || ""),
                    "maxSurface": (this.data.spt_max_surface || ""),
                    "minRooms": (this.data.spt_min_rooms || ""),
                    "maxRooms": (this.data.spt_max_rooms || ""),
                    "propertyType": (this.data.spt_property_type || ""),
                    "fuelType": (this.data.spt_fuel_type || ""),
                    "transmission": (this.data.spt_transmission || ""),
                    "minMileage": (this.data.spt_min_mileage || ""),
                    "maxMileage": (this.data.spt_max_mileage || "")
                };
            }
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "Button", this.identifier, this.intent, target).send();
            AutoTrack.activity().events.trackBasic(this.intent, this.type, this.id, target.items).send();
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
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        element = (items[i].querySelector("a i[class*='favourite']") || "");
        if (element) {
            element.parentNode.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }

    dataPulse.identifier = "button[class*='pagAnuFav']";
    items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
