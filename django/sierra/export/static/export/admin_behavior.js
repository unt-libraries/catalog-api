var export_form_filter_control = function() {
    var filter_val = $('#id_export_filter').val();
    $('.field-wrapper.date-range').toggleClass('hidden', !filter_val.match(/date_range$/));
    $('.field-wrapper.record-range').toggleClass('hidden', !filter_val.match(/record_range$/));
    $('.field-wrapper.location-code').toggleClass('hidden', !filter_val.match(/location$/));
    $('.field-wrapper.which-location').toggleClass('hidden', !filter_val.match(/location$/));
};

/*Document Ready Function*/
$(document).ready(function() {
    export_form_filter_control();
    if ($('#id_export_filter').length) {
        $('#id_export_filter').on('change', export_form_filter_control);
    }
});
