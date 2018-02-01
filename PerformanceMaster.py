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
    'no_match',
    'traffic driver',
    'video',
    'video autoplay'
}

metric_dict = {
    'DFP': {
        'branded driver':
            [('DFP CTR'),
             ('dfp_clicks', 'dfp_impressions')],
        'traffic driver':
            [('DFP CTR'),
             ('dfp_clicks', 'dfp_impressions')],
        'video':
            [('DFP CTR', 'VSR'),
             ('dfp_clicks', 'dfp_impressions', 'result_5')],
        'interactive non video':
            [('DFP CTR','IR'),
             ('dfp_clicks', 'dfp_impressions', 'int_sessions')],
        'interactive video':
            [('DFP CTR', 'IR', 'VSR'),
             ('dfp_clicks', 'dfp_impressions', 'int_sessions','result_5')]
    },
    '3P': {
        'branded driver':
            [('3P CTR'), ('normalized_impressions', 'normalized_clicks')],
        'traffic driver':
            [('3P CTR'), ('normalized_impressions', 'normalized_clicks')],
        'video':
            [('3P CTR', '3P VSR'),
             ('normalized_impressions', 'normalized_clicks', 'result_5')],
        'interactive non video':
            [('3P CTR','3P IR'),
             ('normalized_impressions', 'normalized_clicks', 'int_sessions')],
        'interactive video':
            [('3P CTR', '3P IR', '3P VSR'),
             ('normalized_impressions', 'normalized_clicks', 'int_sessions', 'result_5')]
    }
}

############################# functions below ##################################
def metric_calcs(df, metric='DFP CTR'):
    if metric == 'DFP CTR':
        x = (df['dfp_clicks'] / df['dfp_impressions']) * 100
        return x.round(2)

    elif metric == '3P CTR':
        x = (df['normalized_impressions'] / df['normalized_clicks']) * 100
        return x.round(2)

    elif metric == 'VSR':
        x = (df['result_5'] / df['dfp_impressions']) * 100
        return x.round(2)

    elif metric == '3P VSR':
        x = (df['result_5'] / df['normalized_clicks']) * 100
        return x.round(2)

    elif metric == 'IR':
        x = ((df['int_sessions'] + df['dfp_clicks']) /
             df['dfp_impressions']) * 100
        return x.round(2)

    elif metric == '3P IR':
        x = ((df['int_sessions'] + df['normalized_impressions']) /
             df['normalized_clicks']) * 100
        return x.round(2)

    elif metric == 'View %':
        x = (df['ad_server_impressions'] /
             df['dfp_impressions']) * 100
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
        summary of creative_types across sites
    """
    if ad_server == 'DFP':
        imps = 'dfp_impressions'
    elif ad_server == '3P':
        imps = 'normalized_clicks'
    else:
        raise ValueError('ad_server kwarg should be "DFP", "3P"')

    dfx = df[(df['date'] >= d1) & (df['date'] <= d2)]

    dfx = dfx.groupby(('creative_type', 'site'), as_index=False).sum()

    dfx = dfx[['creative_type', 'site', imps]]
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
        view_cols = ['ad_server_impressions']
    elif ad_server == '3P':
        view_cols = ['ad_server_impressions',
                     'dfp_impressions']
    else:
         raise ValueError('ad_server kwarg should be "DFP", "3P"')

    groupons = ['advertiser', 'placement']
    metrics = metric_dict[ad_server][creative_type][0]
    metric_components = metric_dict[ad_server][creative_type][1]

    categories = groupons + view_cols + list(metric_components)
    imp_col = [i for i in categories if 'impressions' in i and 'server' not in i][0]

    dfx = df[(df['date'] >= d1) & (df['date'] <= d2)]
    dfx = dfx[(dfx['creative_type'] == creative_type) & (dfx['site'] == site)]
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
            'advertiser', 'order', 'site', 'line_item', 'status', 'impressions'
        )
    )

    # import pdb; pdb.set_trace()
    DF = df[(df['date'] >= d1) & (df['date'] <= d2) & (df['creative_type'] == 'no_match')]

    s1 = []
    for order in set(DF['order']):
        dfx = DF[DF['order'] == order]


        dfx = dfx.groupby(('advertiser', 'site', 'line_item'), as_index=False).sum()
        # if not dfx.empty:

        dfx = dfx[dfx['dfp_impressions'] > imp_thresh]

        if not dfx.empty:
            for i, row in dfx.iterrows():
                advert = row['advertiser']
                site = row['site']
                line_item = row['line_item']
                status = 'no_match'
                impressions = row['dfp_impressions']
                s1.append(
                    no_match(
                        advert, order, site, line_item, status, impressions
                    )
                )

    no_match = pd.DataFrame(s1)
    return no_match

def mismatched_checker(df, d1, d2, imp_thresh=1000):
    """
    Finds all campaigns where creative_type is pulling in an incorrect type.
    Returns creative versions where the main KPI for that creative_type is receiving no actions.
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
            ['dfp_clicks', 'dfp_impressions'],
        'traffic driver':
            ['dfp_clicks', 'dfp_impressions'],
        'video autoplay':
            ['dfp_clicks', 'dfp_impressions'],
        'co-branded driver':
            ['dfp_clicks', 'dfp_impressions'],
        'video':
            ['result_5', 'dfp_impressions'],
        'interactive non video':
            ['int_sessions','dfp_impressions'],
        'brand survey':
            ['int_sessions','dfp_impressions'],
        'interactive video':
            ['result_5','dfp_impressions'],
        'no_match':
            ['dfp_clicks', 'dfp_impressions']
    }


    df = df[(df['date'] >= d1) & (df['date'] <= d2)]


    for creative_type in set(df['creative_type']):
        groupons = ['advertiser', 'placement', 'creative_name_version', 'site', 'creative_type']
        if creative_type == 'interactive non video' or 'survey' in creative_type:

            metrics = metric_dict[creative_type]

            dfx = df[(df['creative_type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh]
            dfx = dfx[dfx['int_sessions'].isnull()].copy()

            if dfx.empty:
                print('no '+creative_type+' mismatches')

            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('dfp_impressions', ascending=False)
            del dfx['int_sessions']
            storage.append(dfx)


        elif 'no_match' in creative_type:

            metrics = metric_dict[creative_type]
            groupons = ['advertiser', 'placement', 'site', 'creative_type']

            dfx = df[(df['creative_type'] == creative_type)].copy()
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh]
            dfx['creative_name_version'] = 'no_match'
            dfx = dfx[dfx['dfp_clicks']==0].copy()

            if dfx.empty:
               print('no '+creative_type+' mismatches')


            dfx['mis_match'] = 'no_clicks'
            dfx = dfx.sort_values('dfp_impressions', ascending=False)
            del dfx['dfp_clicks']
            storage.append(dfx)


        elif 'driver' in creative_type or 'autoplay' in creative_type:
            metrics = metric_dict[creative_type]

            dfx = df[(df['creative_type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh]
            dfx = dfx[dfx['dfp_clicks']==0].copy()

            if dfx.empty:
               print('no '+creative_type+' mismatches')

            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('dfp_impressions', ascending=False)
            del dfx['dfp_clicks']
            storage.append(dfx)

        elif 'video' in creative_type:
            metrics = metric_dict[creative_type]

            dfx = df[(df['creative_type'] == creative_type)]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()
            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh]
            dfx = dfx[dfx['result_5'].isnull()].copy()

            if dfx.empty:
               print('no '+creative_type+' mismatches')

            dfx['mis_match'] = 'no_kpi_actions'
            dfx = dfx.sort_values('dfp_impressions', ascending=False)
            del dfx['result_5']
            storage.append(dfx)





    df_all = pd.concat(storage)
    df_all=df_all.sort_values('dfp_impressions',ascending=False)
    col_order=['advertiser', 'site','creative_name_version','placement',
       'creative_type', 'dfp_impressions', 'mis_match']
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
                ['dfp_clicks', 'dfp_impressions'],
            'traffic driver':
                ['dfp_clicks', 'dfp_impressions'],
            'video autoplay':
                ['dfp_clicks', 'dfp_impressions'],
            'co-branded driver':
                ['dfp_clicks', 'dfp_impressions'],
            'video':
                ['dfp_clicks','result_5', 'dfp_impressions'],
            'interactive non video':
                ['dfp_clicks','int_sessions','dfp_impressions'],
            'brand survey':
                ['dfp_clicks','int_sessions','dfp_impressions'],
            'interactive video':
                ['dfp_clicks','result_5','dfp_impressions'],
            'no_match':
                ['dfp_clicks', 'dfp_impressions']
    }

    df = df[(df['date'] >= d1) &
            (df['date'] <= d2) &
            (df['site'] == site)]

    storage=[]

    groupons = ['advertiser', 'placement', 'creative_name_version', 'site',
        'creative_type']

    def merger(df, df_bm):
        dft = pd.merge(df, df_bm, how='left', on=['placement', 'KPI'])
        dft['Below_Bench'] = dft['KPI_Rate'] - dft['Benchmark']
        dft = dft.sort_values('dfp_impressions', ascending=False)
        return dft

    def CTR_KPI(dfx, imp_thresh):
        dfx = dfx[dfx['dfp_impressions'] >= imp_thresh].copy()
        dfx['KPI_Rate'] = dfx['dfp_clicks'] / dfx['dfp_impressions']
        dfx['KPI'] = 'CTR'
        return dfx

    for creative_type in set(df['creative_type']):
        if creative_type == 'interactive non video' or 'survey' in creative_type:

            dfx = df[(df['creative_type'] == creative_type)]

            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh].copy()
            dfx['KPI_Rate'] = ((dfx['int_sessions'] + dfx['dfp_clicks'])
                / dfx['dfp_impressions'])
            dfx['KPI'] = 'IR'

            dft = merger(dfx, df_bm)

            del dft['int_sessions']
            del dft['dfp_clicks']

            storage.append(dft)

        elif 'no_match' in creative_type:
            groupons_no_match = ['advertiser', 'placement', 'site', 'creative_type']

            dfx = df[(df['creative_type'] == creative_type)]
            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons_no_match, as_index=False)[metrics].sum()

            dfx = CTR_KPI(dfx, imp_thresh)

            dft = merger(dfx, df_bm)

            dft['creative_name_version'] = 'no_match'
            del dft['dfp_clicks']
            storage.append(dft)

        elif 'driver' in creative_type or 'autoplay' in creative_type:

            dfx = df[(df['creative_type'] == creative_type)]
            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = CTR_KPI(dfx, imp_thresh)
            dft = merger(dfx, df_bm)

            del dft['dfp_clicks']
            storage.append(dft)

        elif 'video' in creative_type:

            dfx = df[(df['creative_type'] == creative_type)]

            metrics = metric_dict[creative_type]
            dfx = dfx.groupby(groupons, as_index=False)[metrics].sum()

            dfx = dfx[dfx['dfp_impressions'] >= imp_thresh].copy()
            dfx['KPI_Rate'] = (dfx['result_5']) / dfx['dfp_impressions']
            dfx['KPI'] = 'VID'

            dft = merger(dfx, df_bm)

            del dft['result_5']
            del dft['dfp_clicks']
            storage.append(dft)

    # from functools import reduce
    # df_all = reduce(lambda left,right: pd.merge(left,right,on=col_order,how='outer'), storage)

    col_order=['advertiser', 'site','creative_name_version','placement',
    'creative_type', 'dfp_impressions','KPI_Rate','KPI',
    'Benchmark','Below_Bench']

    df_all = pd.concat(storage)
    df_all = df_all[col_order]
    df_all = df_all.sort_values('dfp_impressions',ascending=False)

    # add in summary
    # the number of placements ABOVE benchmarck & below benchmark
    above_bench = len(df_all[df_all['Below_Bench']>=0])
    below_bench = len(df_all[df_all['Below_Bench']<0])
    percent_above = above_bench / (above_bench + below_bench)

    print(above_bench, 'creative placements ABOVE benchmark')
    print(below_bench, 'creative placements BELOW benchmark')
    print(int(percent_above*100), 'percent of creative placements ABOVE')

    ab_imp = df_all[df_all['Below_Bench']>=0]['dfp_impressions'].sum()
    bb_imp = df_all[df_all['Below_Bench']<0]['dfp_impressions'].sum()
    imp_report = ab_imp / (bb_imp + ab_imp)
    print(int(imp_report*100), 'percent of impressions ABOVE')

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
    groupons = ['advertiser', 'site', 'placement', 'creative_name_version',
                'creative_type']

    return_cols =['dfp_impressions', 'Ad_viewable', 'QZ_Viewability',
                  'Below_view']

    col_order = groupons + return_cols

    V_num = 'ad_server_impressions'    #numerator
    V_den = 'dfp_impressions'                   #denominator

    df = df[(df['date'] >= d1) &
            (df['date'] <= d2) &
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
