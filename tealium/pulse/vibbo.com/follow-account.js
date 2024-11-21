(function(data) {
    var setPulseTrackClick = function(e) {
        if (this.id !== "") {
            var name = (this.data.ad_publisher_type_id === "1" ? "Company" : "Seller");
            var follow = (e.currentTarget.firstElementChild.style.display.match(/display|inline/));
            var intent = (follow ? "Follow" : "Unfollow");
            var targetPageType = (utag_data.ad_publisher_type_id === "1" ? "Organization" : "Account");
            var target = {};
            target.items = {
                "@id": "sdrn:" + this.site + ":user:" + this.id,
                "@type": targetPageType,
                "name": (follow ? "Follow " + name : "Unfollow " + name)
            };
            AutoTrack.activity().events.trackEngagement(0, 0, "Click", "UIElement", this.identifier, intent, target).send();
            AutoTrack.activity().events.trackBasic(intent, targetPageType, this.id, target.items).send();
        }
    };
    var i = 0;
    var dataPulse = {"site" : "vibbocom", "data": data};
    dataPulse.identifier = "a.vb-FollowersCard-button-linkFollow";
    var items = document.body.querySelectorAll(dataPulse.identifier);
    for (i = 0; i < items.length; i++) {
        dataPulse.id = (data.spt_ad_id || "");
        dataPulse.name = (data.ad_title.toLowerCase() || "");
        items[i].addEventListener('click', setPulseTrackClick.bind(JSON.parse(JSON.stringify(dataPulse))));
    }
}
)(b);
