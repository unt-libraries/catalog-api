from datetime import datetime, time

def _get_test_filter_params(date_from, date_to):
    date_from = datetime.combine(date_from, time(0, 0))
    date_to = datetime.combine(date_to, time(23, 59, 59, 99))
    filter = {
        'record_metadata__record_last_updated_gmt__gte': date_from,
        'record_metadata__record_last_updated_gmt__lte': date_to,
    }
    order_by = ['record_metadata__record_last_updated_gmt']
    prefetch_related = [
        'record_metadata__varfield_set',
        'checkout_set',
        'bibrecorditemrecordlink_set',
        'bibrecorditemrecordlink_set__bib_record__record_metadata',
        'bibrecorditemrecordlink_set__bib_record__record_metadata'
            + '__varfield_set',
    ]
    select_related = ['record_metadata', 'location', 'itype']
    return [
        {
            'filter': filter,
            'order_by': '',
            'prefetch_related': '',
            'select_related': ''
        },
        {
            'filter': filter,
            'order_by': order_by,
            'prefetch_related': '',
            'select_related': ''
        },
        {
            'filter': filter,
            'order_by': '',
            'prefetch_related': prefetch_related,
            'select_related': select_related
        },
        {
            'filter': filter,
            'order_by': order_by,
            'prefetch_related': prefetch_related,
            'select_related': select_related
        },
    ]

def _apply_filter(qs, params):
    filter = params['filter']
    order_by = params['order_by']
    prefetch_related = params['prefetch_related']
    select_related = params['select_related']
    set = qs
    if filter:
        set = set.filter(**filter)
    if order_by:
        set = set.order_by(*order_by)
    if select_related:
        set = set.select_related(*select_related)
    if prefetch_related:
        set = set.prefetch_related(*prefetch_related)
    return set

def get_test_sets(date_from, date_to, model):
    sets = []
    p = _get_test_filter_params(date_from, date_to)
    for i in p:
        sets.append(_apply_filter(model.objects.all(), i))
    return sets

def list_recnums(set):
    rlist = []
    for rec in set:
        rlist.append(rec.record_metadata.get_iii_recnum(True))
    return len(rlist)

def timeit(func, *args, **kwargs):
    t0 = datetime.now()
    result = func(*args, **kwargs)
    t1 = datetime.now()
    diff = t1 - t0
    return {'secs': diff.total_seconds(), 'return_value': result}

def _average_results(raw):
    avg_results = []
    for test_set in raw:
        result_set = {}
        for key in test_set[0].keys():
            results_array = [float(results[key]) for results in test_set]
            result_set[key] = sum(results_array) / float(len(results_array))
        avg_results.append(result_set)
    return avg_results

def count_test(set):
    return [set.count, ]

def len_test(set):
    return [len, set]

def list_test(set):
    return [list_recnums, set]

def run_benchmarks(date_from, date_to, model, tests, num_tests=1, reset=True):
    results = {}
    sets = get_test_sets(date_from, date_to, model)
    for test in tests:
        this_test_results = []
        for i in sets:
            this_test_results.append([])
        print 'Running ' + test['name'] + ' tests...'
        for count in range(0, num_tests):
            set_num = 0
            print '     Run ' + str(count + 1)
            for set in sets:
                print '        Test ' + str(set_num + 1)
                args = test['function'](set)
                this_test_results[set_num].append(timeit(*args))
                set_num += 1
            if reset:
                del sets
                sets = get_test_sets(date_from, date_to, model)
        if test['name'] not in results:
            results[test['name']] = []
        results[test['name']] = _average_results(this_test_results)
    return results

def print_results(results):
    for test_name in results.keys():
        print test_name + ' Test Results ---------------------------------\n'
        count = 1
        for test_set in results[test_name]:
            print 'Test Set ' + str(count) + ':'
            print '    ' + str(int(test_set['return_value'])) + ' records'
            print '    ' + str(round(test_set['secs'], 2)) + ' seconds\n'
            count += 1

if __name__ == "__main__":
    from base.models import ItemRecord
    
    date_from = datetime(2014, 02, 01)
    date_to = datetime(2014, 02, 05)
    tests = [
        {'name': 'COUNT', 'function': count_test},
        {'name': 'LEN', 'function': len_test},
        {'name': 'LIST', 'function': list_test},
    ]
    results = run_benchmarks(date_from, date_to, ItemRecord, tests)
    print_results(results)
