import streamlit as st
import pandas as pd
import pydeck as pdk
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

def aqi_to_color(aqi):
    if aqi <= 50:
        return [0, 228, 0, 160]
    elif aqi <= 100:
        return [255, 255, 0, 160]
    elif aqi <= 150:
        return [255, 126, 0, 160]
    elif aqi <= 200:
        return [255, 0, 0, 160]
    elif aqi <= 300:
        return [143, 63, 151, 160]
    else:
        return [126, 0, 35, 160]

# page Title
st.title("Max AQI | State Level")

# get Session
session = get_active_session()

# variables
state_option, date_option  = '',''

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
    result_query = f"""
        WITH city_coordinates AS
        (
            SELECT
                location_pk
                ,latitude
                ,longitude
                ,country
                ,state
                ,city
                ,RANK() OVER (PARTITION BY country, state, city ORDER BY location_pk) rank
            FROM
                dev_db.consumption_sch.dim_location
            WHERE
                state = '{state_option}'
        )
        SELECT
            a.city
            ,latitude
            ,longitude
            ,aqi
        FROM
            dev_db.consumption_sch.agg_city_fact_day_level a
        JOIN
            city_coordinates b
            ON a.country = b.country
            AND a.state = b.state
            AND a.city = b.city
            AND rank = 1
        WHERE
            a.state = '{state_option}'
            AND measurement_date = '{date_option}'
    """

    sf_df = session.sql(result_query).collect()

    df_map = pd.DataFrame(sf_df, columns = ['city','latitude','longitude','aqi'])

    columns_to_convert = ['latitude', 'longitude']
    df_map[columns_to_convert] = df_map[columns_to_convert].astype(float)
    
    # bubble color acc/to AQI value
    df_map['color'] = df_map['aqi'].apply(aqi_to_color)

    # bubble size acc/to AQI value
    df_map['radius'] = df_map['aqi'].apply(lambda x: max(2000, x * 150))

    # map zoom adjustment
    view_state = pdk.ViewState(
        latitude = df_map["latitude"].mean(),
        longitude = df_map["longitude"].mean(),
        zoom = 6,          # Good for most US states
        pitch = 0
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data = df_map,
        get_position = '[longitude, latitude]',
        get_radius = 'radius',
        get_fill_color = 'color',
        pickable = True,
        opacity = 0.8,
        stroked = False
    )

    st.subheader(f"{state_option} on {date_option}")

    st.pydeck_chart(
        pdk.Deck(
            layers = [layer],
            initial_view_state = view_state,
            tooltip = {
                "html": "<b>City:</b> {city}<br/><b>AQI:</b> {aqi}",
                "style": {"color": "white"}
            }
        )
    )
