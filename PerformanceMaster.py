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
    DataFrame with all "mis-matches" with impressions greater than imp_thresh

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

def benchmark_compare(df, df_benchmarks, d1, d2, imp_thresh=1000,site='qz'):
    """
    Flags all placements that are underperforming relative to their main KPIs
    df = campaign DataFrame
    df_benchmarks = benchmarks DataFrame file ('2017_display_benchmarks_CS.xlsx')
    d1 = start date
    d2 = end date
    imp_thresh = number of impressions to include in checker
    site = 'qz' (no benchmarks for other sites yet)

    """
    #Clean the benchmark dataframe
    df_bm = df_benchmarks[df_benchmarks['Data Source'] == 'DFP'].copy()
    df_bm['Placement'] = df_bm['Placement'].str.lower()
    df_bm = df_bm.drop('Data Source',1)
    df_bm = df_bm.rename(
        columns={'1H2017 BM': 'Benchmark','Placement': 'placement'})

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
                ['DFP Creative ID Clicks','result_5', 'DFP Creative ID Impressions'],
            'interactive non video':
                ['DFP Creative ID Clicks','int sessions','DFP Creative ID Impressions'],
            'brand survey':
                ['DFP Creative ID Clicks','int sessions','DFP Creative ID Impressions'],
            'interactive video':
                ['DFP Creative ID Clicks','result_5','DFP Creative ID Impressions'],
            'no match':
                ['DFP Creative ID Clicks', 'DFP Creative ID Impressions']
    }

    df = df[(df['Date'] >= d1) &
            (df['Date'] <= d2) &
            (df['site'] == site)]

    storage=[]

    groupons = ['Advertiser', 'placement', 'creative.name.version', 'site',
        'creative.type']

    def merger(df, df_bm):
        dft = pd.merge(df, df_bm, how='left', on=['placement', 'KPI'])
        dft['Below_Bench'] = dft['KPI_Rate'] - dft['Benchmark']
        dft = dft.sort_values('DFP Creative ID Impressions', ascending=False)
        return dft

    def CTR_KPI(dfx, imp_thresh):
        dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh].copy()
        dfx['KPI_Rate'] = dfx['DFP Creative ID Clicks'] / dfx['DFP Creative ID Impressions']
        dfx['KPI'] = 'CTR'
        return dfx

    for creative_type in set(df['creative.type']):
        if creative_type == 'interactive non video' or 'survey' in creative_type:

            dfx = df[(df['creative.type'] == creative_type)]

            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh].copy()
            dfx['KPI_Rate'] = ((dfx['int sessions'] + dfx['DFP Creative ID Clicks'])
                / dfx['DFP Creative ID Impressions'])
            dfx['KPI'] = 'IR'

            dft = merger(dfx, df_bm)

            del dft['int sessions']
            del dft['DFP Creative ID Clicks']

            storage.append(dft)

        elif 'no match' in creative_type:
            groupons_no_match = ['Advertiser', 'placement', 'site', 'creative.type']

            dfx = df[(df['creative.type'] == creative_type)]
            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons_no_match, as_index=False)[metrics].sum()

            dfx = CTR_KPI(dfx, imp_thresh)

            dft = merger(dfx, df_bm)

            dft['creative.name.version'] = 'no match'
            del dft['DFP Creative ID Clicks']
            storage.append(dft)

        elif 'driver' in creative_type or 'autoplay' in creative_type:

            dfx = df[(df['creative.type'] == creative_type)]
            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = CTR_KPI(dfx, imp_thresh)
            dft = merger(dfx, df_bm)

            del dft['DFP Creative ID Clicks']
            storage.append(dft)

        elif 'video' in creative_type:

            dfx = df[(df['creative.type'] == creative_type)]

            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = dfx[dfx['DFP Creative ID Impressions'] >= imp_thresh].copy()
            dfx['KPI_Rate'] = (dfx['result_5']) / dfx['DFP Creative ID Impressions']
            dfx['KPI'] = 'VID'

            dft = merger(dfx, df_bm)

            del dft['result_5']
            del dft['DFP Creative ID Clicks']
            storage.append(dft)

    # from functools import reduce
    # df_all = reduce(lambda left,right: pd.merge(left,right,on=col_order,how='outer'), storage)

    col_order=['Advertiser', 'site','creative.name.version','placement',
    'creative.type', 'DFP Creative ID Impressions','KPI_Rate','KPI',
    'Benchmark','Below_Bench']

    df_all = pd.concat(storage)
    df_all = df_all[col_order]
    df_all = df_all.sort_values('DFP Creative ID Impressions',ascending=False)

    # add in summary
    # the number of placements ABOVE benchmarck & below benchmark
    print(df_all[df_all['Below_Bench']>=0].shape)
    print(df_all[df_all['Below_Bench']<0].shape)

    return df_all[df_all['Below_Bench']<0]

def viewability_checker(df, df_viewability, d1, d2, imp_thresh=1000, site='qz'):
    """
    Flags all placements that are below the normal range for viewability

    df : dataset
    df_viewability : viewability benchmarks
    d1 : initial date
    d2 : end date
    imp_thresh : only include ads with impressions greater than this number
    site : 'qz' only qz for now

    Returns DF with ad placements where viewability is below QZ average
    """
    groupons = ['Advertiser', 'site', 'placement', 'creative.name.version',
                'creative.type']

    return_cols =['DFP Creative ID Impressions', 'Ad_viewable', 'QZ_Viewability',
                  'Below_view']

    col_order = groupons + return_cols

    V_num = 'Ad server Active View viewable impressions'    #numerator
    V_den = 'DFP Creative ID Impressions'                   #denominator

    df = df[(df['Date'] >= d1) &
            (df['Date'] <= d2) &
            (df['site'] == site)]

    df = df.groupby(groupons, as_index=False)[V_num, V_den].sum()
    df = df[df[V_den] >= imp_thresh]
    df['Ad_viewable'] = df[V_num] / df[V_den]

    df = pd.merge(df, df_viewability, how='left', on='placement')
    df['Below_view'] = df['Ad_viewable'] - df['QZ_Viewability']
    df = df.sort_values('Below_view', ascending=True)

    del df[V_num]
    df = df[df['Below_view']<0]
    df = df[col_order]

    return df
