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
                this.intent = (e.currentTarget.className.indexOf("icon-ics_actions_ic_FavoriteOn") >= 0 ? "Unsave" : "Save");
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
    var dataPulse = {"site" : "vibbocom", "data": data};
    dataPulse.identifier = ".addToFav";
    var items = document.body.querySelectorAll(".list_ads_row");
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (items[i].id || "");
        element = (items[i].querySelector("a.subjectTitle") || "");
        dataPulse.name = (element.innerHTML.toLowerCase() || "");
        dataPulse.type = "ClassifiedAd";
        dataPulse.intent = "";
        dataPulse.target = "detail";
        element = (items[i].querySelector(".addToFav a") || "");
        if (element) {
            element.addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
        }
    }

    dataPulse.identifier = "#SAdFAV";
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
