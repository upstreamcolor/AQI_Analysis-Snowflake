# import python packages
import streamlit as st
import pandas as pd
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

# page Title
st.title("Hourly AQI Trends | Station Level")

# get Session
session = get_active_session()

# variables
state_option,city_option, station_option, date_option  = '','','',''

state_query = """
    SELECT 
        state 
    FROM 
        dev_db.consumption_sch.dim_location 
    GROUP BY 
        state 
    ORDER BY 
        1
"""

# creating state dropdown
state_list = session.sql(state_query).collect()
state_option = st.selectbox('Select State', state_list)

# verify state selection
if (state_option is not None and len(state_option) > 1):
    city_query = f"""
        SELECT 
            city 
        FROM 
            dev_db.consumption_sch.dim_location 
        WHERE 
            state = '{state_option}' 
        GROUP BY 
            city
        ORDER BY 
            1 desc
    """

    # creating city dropdown
    city_list = session.sql(city_query).collect()
    city_option = st.selectbox('Select City', city_list)

# verify city selection
if (city_option is not None and len(city_option) > 1):
    station_query = f"""
        SELECT 
            station 
        FROM 
            dev_db.consumption_sch.dim_location 
        WHERE 
            state = '{state_option}' 
            and city = '{city_option}'
        GROUP BY 
            station
        ORDER BY 
            1 desc;
    """

    # creating station dropdown
    station_list = session.sql(station_query).collect()
    station_option = st.selectbox('Select Station', station_list)

# verify station selection
if (station_option is not None and len(station_option) > 1):
    date_query = f"""
        SELECT 
            date(measurement_time) as measurement_date 
        FROM 
            dev_db.consumption_sch.dim_date
        GROUP BY 
            1 
        ORDER BY 
            1 desc;
    """

    # creating date dropdown
    date_list = session.sql(date_query).collect()
    date_option = st.selectbox('Select Date', date_list)

# verify date selection
if (date_option is not None):
    trend_sql = f"""
        SELECT 
            hour(measurement_time) as Hour
            ,l.state
            ,l.city
            ,l.station
            ,l.latitude::number(10,7) as latitude
            ,l.longitude::number(10,7) as longitude
            ,pm25_avg
            ,pm10_avg
            ,so2_avg
            ,no2_avg
            ,nh3_avg
            ,co_avg
            ,o3_avg
            ,prominent_pollutant
            ,AQI
        FROM 
            dev_db.consumption_sch.air_quality_fact f 
        JOIN 
            dev_db.consumption_sch.dim_date d on d.date_pk = f.date_fk 
            and date(measurement_time) = '{date_option}'
        JOIN 
            dev_db.consumption_sch.dim_location l 
                on l.location_pk  = f.location_fk  
                and l.state = '{state_option}' 
                and l.city = '{city_option}'  
                and l.station = '{station_option}'
        ORDER BY 
            measurement_time
    """
    sf_df = session.sql(trend_sql).collect()

    # changing sf_df row to a pandas df
    df = pd.DataFrame(sf_df, columns = ['Hour','state','city','station','lat','lon','PM2.5','PM10','SO3','CO','NO2','NH3','O3','PROMINENT_POLLUTANT','AQI'])

    # dropping un-necessary columns for each df
    df_aqi = df.drop(['state','city','station','lat','lon','PM2.5','PM10','SO3','CO','NO2','NH3','O3','PROMINENT_POLLUTANT'], axis = 1)
    df_table = df.drop(['state','city','station','lat','lon','PROMINENT_POLLUTANT','AQI'], axis = 1)
    df_map = df.drop(['Hour','state','city','station','PM2.5','PM10','SO3','CO','NO2','NH3','O3','PROMINENT_POLLUTANT','AQI'], axis = 1)

    st.subheader(f"Hourly AQI Level")
    #st.caption(f'### :blue[Temporal Distribution] of Pollutants on :blue[{date_option}]')
    st.line_chart(df_aqi, x = "Hour", color = '#FFA500')
    
    st.subheader(f"Stacked Chart:  Hourly Individual Pollutant Level")
    #st.caption(f'### :blue[Temporal Distribution] of Pollutants on :blue[{date_option}]')
    st.bar_chart(df_table, x = "Hour")
    
    st.subheader(f"Line Chart: Hourly Pollutant Levels")
    #st.caption(f'### Hourly Trends in Pollutant Levels - :blue[{date_option}]')
    st.line_chart(df_table, x = "Hour")
    
    columns_to_convert = ['lat', 'lon']
    df_map[columns_to_convert] = df_map[columns_to_convert].astype(float)
    st.subheader(f"{station_option}")
    st.map(df_map, size = 'AQI')