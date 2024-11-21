(function() {
    var page = (utag_data.event_name || "");
    if (page === "detail") {
        var id = (utag_data.ad_id || "");
        var name = (utag_data.ad_title || "");
        document.querySelectorAll("#lnk_favtop").forEach(function(e) {
            e.addEventListener("click", function(){
                if (event.currentTarget.firstElementChild.className.indexOf("s1") >= 0) {
                    window.PulseAdapter.eventFavouriteAdSave(id, name);
                } else {
                    window.PulseAdapter.eventFavouriteAdUnsave(id, name);
                }
            }, true);
        });

        document.querySelectorAll("#lnk_fav").forEach(function(e) {
            e.addEventListener("click", function(){
                if (event.currentTarget.className.indexOf("s1") >= 0) {
                    window.PulseAdapter.eventFavouriteAdSave(id, name);
                } else {
                    window.PulseAdapter.eventFavouriteAdUnsave(id, name);
                }
            }, true);
        });

        document.querySelectorAll(".lnk_vertelf").forEach(function(e) {
            e.addEventListener("click", function(){
                window.PulseAdapter.eventShowPhone(id, name);
            }, true);
        });

        document.querySelectorAll("#main.ficha a.new").forEach(function(e) {
            e.addEventListener("click", function(){
                window.PulseAdapter.eventFormSend(id, name);
            }, true);
        });

        document.querySelectorAll(".bt_contact").forEach(function(e) {
            e.addEventListener("click", function(){
                window.PulseAdapter.eventConfirmationSend(id, name);
                window.PulseAdapter.eventSendMessage(id, name);
            }, true);
        });

    }
})();
