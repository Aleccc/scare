from timer.timer import timer
from datawarehouse.common import pd
from datawarehouse.stg import db as dw_stg
from datawarehouse.edw import db as dw_edw

"""
current_cd time: 1066.78sec (as high as 5922.04sec via VPN)
clean time: 338.66sec
assign_hdd_to_reads time: 15755.53sec
"""
# =============================================================================
# select all from a specific year's cd
# =============================================================================
@timer
def get_distinct_gas_south_premises(year, month):
    dw_edw.connect()
    sql = ("select distinct(AGLServiceLocationNumber) "
           "from D_FDCG_Customers "
           "where BillMonth='{year}{month:02d}' "
           .format(year=year,
                   month=month)
           )
    df = dw_edw.query(sql)
    dw_edw.close()
    return df


# =============================================================================
# select reads from only Gas South customers
# =============================================================================
@timer
def get_reads_for_gas_south_customers(year, month, year_prior=False, return_clean=True):
    """ get reads for Gas South customers in current FDCG.
    If year_prior == True: keep fdcg_year current, and lag AGL year by one year.

    SQL cred to Nicolas Merino."""

    fdcg_year = year
    if year_prior:
        year -= 1
    dw_stg.connect()
    sql = ("SELECT DISTINCT FDCG.AGLAccountNumber , FDCG.AGLServiceLocationNumber , AGL_CD. * "
           "FROM [EDW].[dbo].[D_FDCG_Customers] FDCG "
           "LEFT JOIN[GSDWSTG].[dbo].[FM_MM_AGL_CD_{year}] AGL_CD "
           "ON AGL_CD.AGL_Account_Number = FDCG.AGLAccountNumber AND AGL_CD.YearMonth = '{year}{month:02d}' "
           "WHERE FDCG.BillMonth = '{fdcg_year}{month:02d}' "
           .format(fdcg_year=fdcg_year, year=year, month=month)
           )
    df = dw_stg.query(sql)
    dw_stg.close()
    if return_clean:
        df = clean(df)
    return df


# =============================================================================
# select all from a specific year's cd
# =============================================================================
@timer
def get_cd(year, month, return_clean=True):
    dw_stg.connect()
    sql = ("select * "
           "from FM_MM_AGL_CD_{year} "
           "where sourcefilename='AGL_CD_{year}{month:02d}' "
           .format(year=year,
                   month=month)
           )
    df = dw_stg.query(sql)
    dw_stg.close()
    if return_clean:
        df = clean(df)
    return df


@timer
def clean(df_query):
    # =========================================================================
    # unpivot meter reads
    # =========================================================================
    cols_to_always_keep = ['agl_account_number',
                           'agl_premise_number',
                           'customer_type_and_rate',
                           'delivery_group',
                           'design_day_usage_dathm',
                           'design_day_usage_mcf', ]

    df = pd.DataFrame()
    for read in range(12):
        read += 1
        cols = {'consumption_at_meter_{}_ccf'.format(read): 'ccf',
                'consumption_month_{}'.format(read): 'readmonth',
                'meter_read_begin_date_{}'.format(read): 'begin_date',
                'meter_read_end_date_{}'.format(read): 'end_date', }
        df_read = df_query[list(cols.keys()) + cols_to_always_keep]
        df_read = df_read.rename(columns=cols)
        df = df.append(df_read)

    # =========================================================================
    # change data type for columns; add columns; remove na rows
    # =========================================================================
    df.ccf = pd.to_numeric(df.ccf, errors='coerce')
    df.design_day_usage_dathm = pd.to_numeric(df.design_day_usage_dathm, errors='coerce')
    df.design_day_usage_mcf = pd.to_numeric(df.design_day_usage_mcf, errors='coerce')
    df.begin_date = pd.to_datetime(df.begin_date, format='%Y-%m-%d', errors='coerce')
    df.end_date = pd.to_datetime(df.end_date, format='%Y-%m-%d', errors='coerce')
    df = df.dropna()
    # df['CycleDays'] = list(map(lambda x: int(x.days), list(df.end_date - df.begin_date)))
    return df
