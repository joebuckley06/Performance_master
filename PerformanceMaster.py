import os
import pickle
import collections
import numpy as np
import pandas as pd

#################### variables for function use ################################
sites = ('qz', 'wrk', 'zty')

creative_type_metrics = {
    'branded driver': ('DFP CTR', '3P CTR'),
    'traffic driver': ('DFP CTR', '3P CTR'),
    'video': ('DFP CTR','3P CTR', 'VSR'),
    'interactive non video': ('DFP CTR', '3P CTR','IR'),
    'interactive video': ('DFP CTR', '3P CTR','IR', 'VSR'),
}

creative_types = {
    'branded driver',
    'brand survey',
    'co-branded driver',
    'interactive non video',
    'interactive video',
    'no match',
    'traffic driver',
    'video',
    'video autoplay'
}

metric_dict = {
    'DFP': {
        'branded driver':
            [('DFP CTR'),
             ('DFP Creative ID Clicks', 'DFP Creative ID Impressions')],
        'traffic driver':
            [('DFP CTR'),
             ('DFP Creative ID Clicks', 'DFP Creative ID Impressions')],
        'video':
            [('DFP CTR', 'VSR'),
             ('DFP Creative ID Clicks', 'DFP Creative ID Impressions', 'result_5')],
        'interactive non video':
            [('DFP CTR','IR'),
             ('DFP Creative ID Clicks', 'DFP Creative ID Impressions', 'int sessions')],
        'interactive video':
            [('DFP CTR', 'IR', 'VSR'),
             ('DFP Creative ID Clicks', 'DFP Creative ID Impressions', 'int sessions','result_5')]
    },
    '3P': {
        'branded driver':
            [('3P CTR'), ('Normalized 3P Clicks', 'Normalized 3P Impressions')],
        'traffic driver':
            [('3P CTR'), ('Normalized 3P Clicks', 'Normalized 3P Impressions')],
        'video':
            [('3P CTR', '3P VSR'),
             ('Normalized 3P Clicks', 'Normalized 3P Impressions', 'result_5')],
        'interactive non video':
            [('3P CTR','3P IR'),
             ('Normalized 3P Clicks', 'Normalized 3P Impressions', 'int sessions')],
        'interactive video':
            [('3P CTR', '3P IR', '3P VSR'),
             ('Normalized 3P Clicks', 'Normalized 3P Impressions', 'int sessions', 'result_5')]
    }
}

############################# functions below ##################################
def metric_calcs(df, metric='DFP CTR'):
    if metric == 'DFP CTR':
        x = (df['DFP Creative ID Clicks'] / df['DFP Creative ID Impressions']) * 100
        return x.round(2)

    elif metric == '3P CTR':
        x = (df['Normalized 3P Clicks'] / df['Normalized 3P Impressions']) * 100
        return x.round(2)

    elif metric == 'VSR':
        x = (df['result_5'] / df['DFP Creative ID Impressions']) * 100
        return x.round(2)

    elif metric == '3P VSR':
        x = (df['result_5'] / df['Normalized 3P Impressions']) * 100
        return x.round(2)

    elif metric == 'IR':
        x = ((df['int sessions'] + df['DFP Creative ID Clicks']) /
             df['DFP Creative ID Impressions']) * 100
        return x.round(2)

    elif metric == '3P IR':
        x = ((df['int sessions'] + df['Normalized 3P Clicks']) /
             df['Normalized 3P Impressions']) * 100
        return x.round(2)

    elif metric == 'View %':
        x = (df['Ad server Active View viewable impressions'] /
             df['DFP Creative ID Impressions']) * 100
        return x.astype(int)

    else:
        raise ValueError('unrecognized metric')

def site_summary(df, d1='2017-11-01', d2='2017-11-10', ad_server='DFP'):
    """
    INPUTS
        df - performance dataframe
        d1 - beginning date inclusive
        d2 - end date inclusive
        ad_server - 'DFP', '3P'
    RETURNS
        summary of creative.types across sites
    """
    if ad_server == 'DFP':
        imps = 'DFP Creative ID Impressions'
    elif ad_server == '3P':
        imps = 'Normalized 3P Impressions'
    else:
        raise ValueError('ad_server kwarg should be "DFP", "3P"')

    dfx = df[(df['Date'] >= d1) & (df['Date'] <= d2)]

    dfx = dfx.groupby(('creative.type', 'site'), as_index=False).sum()

    dfx = dfx[['creative.type', 'site', imps]]
    dfx = dfx.sort_values(imps, ascending=False)

    dfx['share'] = (dfx[imps] / dfx[imps].sum()) * 100
    dfx['share cumsum'] = dfx['share'].cumsum()
    dfx['share cumsum'] = dfx['share cumsum'].astype(int)
    dfx['share'] = dfx['share'].astype(int)

    dfx.index = range(len(dfx))
    dfx[imps] = dfx[imps].apply(lambda x: format(x, ','))
    return dfx

def metric_report(df, d1='2017-11-01', d2='2017-11-10', site='qz',
    creative_type='branded driver', ad_server='DFP'):
    """
    INPUTS
        df - performance dataframe
        d1 - beginning date inclusive
        d2 - end date inclusive
        site - 'qz', 'wrk', 'zty'
        creative_type - 'branded driver', 'traffic driver', 'video', 'interactive non video', 'interactive video'
    RETURNS
        metrics for Sam and Natalie!
    """

    #check for kwarg errors
    if site not in sites:
        raise ValueError('site kwarg should be "qz", "wrk", "zty"')
    if creative_type not in creative_type_metrics.keys():
        raise ValueError('creative_type kwarg should be: ' + (
            "; ".join(sorted(creative_type_metrics.keys()
                ))))

    if ad_server == 'DFP':
        view_cols = ['Ad server Active View viewable impressions']
    elif ad_server == '3P':
        view_cols = ['Ad server Active View viewable impressions',
                     'DFP Creative ID Impressions']
    else:
         raise ValueError('ad_server kwarg should be "DFP", "3P"')

    groupons = ['Advertiser', 'placement']
    metrics = metric_dict[ad_server][creative_type][0]
    metric_components = metric_dict[ad_server][creative_type][1]

    categories = groupons + view_cols + list(metric_components)
    imp_col = [i for i in categories if 'Impressions' in i][0]

    dfx = df[(df['Date'] >= d1) & (df['Date'] <= d2)]
    dfx = dfx[(dfx['creative.type'] == creative_type) & (dfx['site'] == site)]
    dfx = dfx.groupby(groupons, as_index=False).sum()[categories]
    dfx = dfx.sort_values(imp_col, ascending=False)

    if isinstance(metrics, str):
        dfx[metrics] = metric_calcs(dfx, metric=metrics)
        display_cols = groupons + [imp_col, 'share', 'share cumsum'] + [metrics] + ['View %']

    elif isinstance(metrics, (list, tuple)):
        for metric in metrics:
            dfx[metric] = metric_calcs(dfx, metric=metric)
        display_cols = groupons + [imp_col, 'share', 'share cumsum'] + list(metrics) + ['View %']

    dfx['View %'] = metric_calcs(dfx, metric='View %')
    dfx['share'] = (dfx[imp_col] / dfx[imp_col].sum()) * 100
    dfx['share cumsum'] = dfx['share'].cumsum()
    dfx['share cumsum'] = dfx['share cumsum'].astype(int)
    dfx['share'] = dfx['share'].astype(int)
    dfx.index = range(len(dfx))


    return dfx[display_cols]

def no_match_sorting(df, d1, d2, imp_thresh=1000):
    """
    df - weekly dataframe
    d1 - start date, inclusive
    d2 - end date, inclusive
    imp_thresh - the minimum number of impressions to evaluate
    """

    no_match = collections.namedtuple(
        'no_match', (
            'Advertiser', 'Order', 'site', 'Line_item', 'status', 'impressions'
        )
    )

    DF = df[(df['Date'] >= d1) & (df['Date'] <= d2)]

    s1 = []
    for order in set(DF['Order']):
        dfx = DF[
            (DF['Order'] == order) &
            (DF['creative.type'] == 'no match')]

        dfx = dfx.groupby(('Advertiser', 'site', 'Line item'), as_index=False).sum()
        dfx = dfx[dfx['DFP Creative ID Impressions'] > imp_thresh]

        if not dfx.empty:
            for i, row in dfx.iterrows():
                advert = row['Advertiser']
                site = row['site']
                line_item = row['Line item']
                status = 'no match'
                impressions = row['DFP Creative ID Impressions']
                s1.append(
                    no_match(
                        advert, order, site, line_item, status, impressions
                    )
                )

    no_match = pd.DataFrame(s1)
    return no_match

def mismatched_checker():
    """
    Finds all campaigns where creative.type is pulling in an incorrect type

    ex: VSR = NaN or IR = NaN


    """

def ctr_checker():
    """
    Finds all campaigns without any CTR

    """

def benchmark_compare():
    """
    Flags all placements that are underperforming relative to their main KPIs


    """

def viewability_checker():
    """
    Flags all placements that are below the normal range for viewability

    adding back in for test


    writing the next test step
    """

