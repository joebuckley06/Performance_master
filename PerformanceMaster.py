import os
import pickle
import numpy as np
import pandas as pd

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
            'Advertiser', 'Order', 'site', 'Line item', 'status', 'impressions'
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



def mismatched_checker(df, d1, d2, imp_thresh=1000):
    """
    Finds all campaigns where creative.type is pulling in an incorrect type.
    Returns creative versions where the main KPI for that creative.type is receiving no actions.
        ex: VSR = NaN, IR = NaN, CTR = 0.0000


    Inputs:
    df = DataFrame of all creatives and interactions
    d1 = start date
    d2 = end date

    Outputs:
    DataFrame with all "mis-matches" with impressions greater than 50 (to remove testing/one-off creatives)

    """
    storage=[]

    metric_dict= {
        'branded driver':
            ['DFP Creative ID Clicks', 'DFP Creative ID Impressions'],
        'traffic driver':
            ['DFP Creative ID Clicks', 'DFP Creative ID Impressions'],
        'video autoplay':
            ['DFP Creative ID Clicks', 'DFP Creative ID Impressions'],
        'co-branded driver':
            ['DFP Creative ID Clicks', 'DFP Creative ID Impressions'],
        'video':
            ['result_5', 'DFP Creative ID Impressions'],
        'interactive non video':
            ['int sessions','DFP Creative ID Impressions'],
        'brand survey':
            ['int sessions','DFP Creative ID Impressions'],
        'interactive video':
            ['result_5','DFP Creative ID Impressions'],
        'no match':
            ['DFP Creative ID Clicks', 'DFP Creative ID Impressions']
    }


    df = df[(df['Date'] >= d1) & (df['Date'] <= d2)]


    for creative_type in set(df['creative.type']):
        groupons = ['Advertiser', 'placement', 'creative.name.version', 'site', 'creative.type']
        if creative_type == 'interactive non video' or 'survey' in creative_type:

            metrics = metric_dict[creative_type]

            dfx = df[(df['creative.type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh]
            dfx = dfx[dfx['int sessions'].isnull()].copy()

            if dfx.empty:
                print('no '+creative_type+' mismatches')

            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('DFP Creative ID Impressions', ascending=False)
            del dfx['int sessions']
            storage.append(dfx)


        elif 'no match' in creative_type:

            metrics = metric_dict[creative_type]
            groupons = ['Advertiser', 'placement', 'site', 'creative.type']

            dfx = df[(df['creative.type'] == creative_type)].copy()
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh]
            dfx['creative.name.version'] = 'no match'
            dfx = dfx[dfx['DFP Creative ID Clicks']==0].copy()
            
            if dfx.empty:
               print('no '+creative_type+' mismatches')
            
            
            dfx['mis_match'] = 'no_clicks'
            dfx = dfx.sort_values('DFP Creative ID Impressions', ascending=False)
            del dfx['DFP Creative ID Clicks']
            storage.append(dfx)


        elif 'driver' in creative_type or 'autoplay' in creative_type:
            metrics = metric_dict[creative_type]

            dfx = df[(df['creative.type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh]
            dfx = dfx[dfx['DFP Creative ID Clicks']==0].copy()
            
            if dfx.empty:
               print('no '+creative_type+' mismatches')
            
            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('DFP Creative ID Impressions', ascending=False)
            del dfx['DFP Creative ID Clicks']
            storage.append(dfx)
        
        elif 'video' in creative_type:
            metrics = metric_dict[creative_type]

            dfx = df[(df['creative.type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh]
            dfx = dfx[dfx['result_5'].isnull()].copy()
            
            if dfx.empty:
               print('no '+creative_type+' mismatches')
            
            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('DFP Creative ID Impressions', ascending=False)
            del dfx['result_5']
            storage.append(dfx)


        


    df_all = pd.concat(storage)
    df_all=df_all.sort_values('DFP Creative ID Impressions',ascending=False)
    col_order=['Advertiser', 'site','creative.name.version','placement',
       'creative.type', 'DFP Creative ID Impressions', 'mis_match']
    df_all=df_all[col_order]
    return df_all


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

