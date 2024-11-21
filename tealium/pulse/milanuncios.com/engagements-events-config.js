(function() {
    var id = (utag_data.ad_id || window.top.utag_data ? window.top.utag_data.ad_id : "");
    var name = (utag_data.ad_title || window.top.utag_data ? window.top.utag_data.ad_title : "");
    document.querySelectorAll(".pagAnuFavMobileButton").forEach(function(e) {
        e.addEventListener("click", function() {
            if (event.currentTarget.className.indexOf("pagAnuFavButtonOff") >= 0) {
                window.PulseAdapter.eventFavouriteAdSave(id, name);
            } else {
                window.PulseAdapter.eventFavouriteAdUnsave(id, name);
            }
        }, true);
    });

    document.querySelectorAll("a#tracking-phone").forEach(function(e) {
        e.addEventListener("click", function() {
            window.PulseAdapter.eventShowPhone(id, name);
        }, true);
    });

    document.querySelectorAll(".telefonos a").forEach(function(e) {
        e.addEventListener("click", function() {
            window.PulseAdapter.eventCallPhone(id, name);
        }, true);
    });

    document.querySelectorAll(".pagAnuSocialTable button").forEach(function(e) {
        e.addEventListener("click", function() {
            window.PulseAdapter.eventShare(id, name);
        }, true);
    });
})();
