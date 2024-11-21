var setClickShare = function() {
    var getItems = function() {
        var items = document.body.querySelectorAll('button[data-tagging="c_detail_share_ad"]');
        return items;
    };
    var setClickShareToPulse = function() {
        var spt_intent = 'Share';
        var spt_ad_id = utag_data.ad_id;
        var spt_interaction = 'Click';
        var spt_item_identifier = 'c_detail_share_ad';
        var spt_item_type = 'UIElement';
        var spt_site = 'cochesnet';
        var spt_target = {
            item: {
                "@id": "sdrn:" + spt_site + ":classified:" + spt_ad_id,
                "@type": "ClassifiedAd"
            }
        };
        AutoTrack.activity().events.trackEngagement(0, 0, spt_interaction, spt_item_type, spt_item_identifier, spt_intent, spt_target).send();
        AutoTrack.activity().events.trackBasic(spt_intent, 'ClassifiedAd', spt_ad_id, null).send();
    };
    var items = getItems();
    for (var i = 0; i < items.length; i++) {
        var item = items[i];
        item.parentNode.addEventListener('click', setClickShareToPulse);
    }
}.bind(this);
setClickShare();
