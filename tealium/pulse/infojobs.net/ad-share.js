var setClickShare = function() {
    var getItems = function(id) {
        var items = document.body.querySelectorAll(id);
        return items;
    };
    var setClickToPulse = function() {
        var spt_ad_id = (utag_data.es_sch_ad_id || "");
        if (spt_ad_id !== "") {
            var spt_intent = 'Share';
            var spt_interaction = 'Click';
            var spt_item_identifier = '.btn-group';
            var spt_item_type = 'UIElement';
            var spt_site = 'infojobsnet';
            var spt_target = {
                item: {
                    "@id": "sdrn:" + spt_site + ":classified:" + spt_ad_id,
                    "@type": "ClassifiedAd"
                }
            };
            AutoTrack.activity().events.trackEngagement(0, 0, spt_interaction, spt_item_type, spt_item_identifier, spt_intent, spt_target).send();
            AutoTrack.activity().events.trackBasic(spt_intent, 'ClassifiedAd', spt_ad_id, null).send();
        }
    };
    var items = getItems(".btn-group a");
    for (var i = 0; i < items.length; i++) {
        items[i].parentNode.addEventListener('click', setClickToPulse);
    }
}.bind(this);
setClickShare();
