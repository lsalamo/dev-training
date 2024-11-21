window.PulseAdapter = {};
window.PulseAdapter.site = (b.spt_client_id || "");
window.PulseAdapter.category = (b.spt_format_category || "");
window.PulseAdapter.data = (b || "");
window.PulseAdapter.intentIndex = {
    "save": 0,
    "unsave": 1
};
window.PulseAdapter.eventFavourite = function(e) {
    var id = (e.id || "");
    var item = (e.item || "");
    var type = [{
        name: "ClassifiedAd",
        sdrn: "classified"
    }, {
        name: "Listing",
        sdrn: "listing"
    }];
    var typeName = type[e.typeIndex].name;
    var typeSdrn = type[e.typeIndex].sdrn;
    var intent = window.PulseAdapter.getFavouriteIntent(e);
    var typeObject = typeName;
    var target = {};
    target.items = {
        "@id": "sdrn:" + window.PulseAdapter.site + ":" + typeSdrn + ":" + id,
        "@type": typeName,
        "category": window.PulseAdapter.category
    };
    if (typeName === "ClassifiedAd") {
        target.items.name = (e.title || "");
    } else if (typeName === "Listing") {
        target.items.filters = window.PulseAdapter.getListFilters();
    }
    //window.AutoTrack.callQueue.push(["events", ["trackEngagement", 0, 0, "Click", "Button", item, intent, target], ["send"]]);
    window.AutoTrack.callQueue.push(["events", ["trackBasic", intent, typeObject, id, target.items], ["send"]]);
};

window.PulseAdapter.eventPhone = function(e) {
    var id = (e.id || "");
    var item = (e.item || "");
    var type = [{name : "ClassifiedAd", sdrn : "classified"}, {name : "Listing", sdrn : "listing"}];
    var typeName = type[e.typeIndex].name;
    var typeSdrn = type[e.typeIndex].sdrn;
    var intent = "Show";
    var typeObject = "PhoneContact";
    if (id !== "") {
        var target = {};
        target.items = {
            "@type": typeObject,
            "inReplyTo": {
                "@id": "sdrn:" + window.PulseAdapter.site + ":" + typeSdrn + ":" + id,
                "@type": typeName,
                "category": window.PulseAdapter.category,
                "name": (e.title || "")
            }
        };
        //window.AutoTrack.callQueue.push(["events", ["trackEngagement", 0, 0, "Click", "Button", item, intent, target], ["send"]]);
        window.AutoTrack.callQueue.push(["events", ["trackBasic", intent, typeObject, id, target.items], ["send"]]);
    }
};

window.PulseAdapter.eventShare = function(e) {
    var id = (e.id || "");
    var item = (e.item || "");
    var type = [{
        name: "ClassifiedAd",
        sdrn: "classified"
    }, {
        name: "Listing",
        sdrn: "listing"
    }];
    var typeName = type[e.typeIndex].name;
    var typeSdrn = type[e.typeIndex].sdrn;
    var intent = "Share";
    var typeObject = typeName;
    if (id !== "") {
        var target = {};
        target.items = {
            "@id": "sdrn:" + window.PulseAdapter.site + ":" + typeSdrn + ":" + id,
            "@type": typeName,
            "category": window.PulseAdapter.category
        };
        //window.AutoTrack.callQueue.push(["events", ["trackEngagement", 0, 0, "Click", "Button", item, intent, target], ["send"]]);
        window.AutoTrack.callQueue.push(["events", ["trackBasic", intent, typeObject, id, target.items], ["send"]]);
    }
};
