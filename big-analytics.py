import datetime
import json
import google.cloud.bigquery as bigquery
import pendulum

DATASET_ID = "analytics_XXXXXXXXX"

CLIENT = bigquery.Client.from_service_account_json(
    "./api/usecases/bigquery/credentials.json")



HEADERS_MAP = {"user_property_user_id": "User ID",
               "app_info_version": "App Version",
               "event_name": "Event",
               "item_name": "Action",
               "content_type": "Type",
               "firebase_screen_id": "FireBase ScreenID",
               "user_property_first_open_time": "Open Time",
               "device_info_mobile_model_name": "Mobile Model Name",
               "category": "Category",
               "city": "City",
               "country": "Country",
               "region": "Region",
               "firebase_screen_class": "Screen Name",
               "operating_system": "OS",
               "operating_system_version": "OS Version",
               "item_id": "Item ID",
               "version": "Version",
               "geo_info_region": "Region",
               "geo_info_country": "Country"}


def flatten_user_json(user_json, user_id):
    flat_json = {}
    prefix = "user_property_"

    for individual_json in user_json:
        key = prefix + individual_json['key']
        property_json = (individual_json['value'])

        if 'string_value' in property_json:
            flat_json[key] = property_json['string_value']
        if 'int_value' in property_json:
            flat_json[key] = property_json['int_value']
        if str(individual_json['key']) == 'user_id':
            flat_json[key] = user_id

    return json.dumps(flat_json)


def flatten_event_json(event_params):
    flat_json = {}

    for individual_json in event_params:
        key = individual_json['key']
        property_json = (individual_json['value'])

        if 'string_value' in property_json is not None:
            flat_json[key] = property_json['string_value']

        elif 'int_value' in property_json is not None:
            flat_json[key] = property_json['int_value']

    return json.dumps(flat_json)


def flatten_input_info(input_json, required_key_set, message):
    flat_json = {}

    for key in required_key_set:
        if key in input_json:
            flat_json[key] = input_json[key]

    return json.dumps(flat_json)


def extract_values(row, field_names):
    event_name = row[field_names.index("event_name")]
    event_params = row[field_names.index("event_params")]
    user_id_index = field_names.index("user_id")
    user_id = row[user_id_index]
    user_properties = row[field_names.index("user_properties")]
    device_info = row[field_names.index("device")]
    geo_info = row[field_names.index("geo")]
    app_info = row[field_names.index("app_info")]

    # print_values(False, [row, event_name, user_id_index, user_id,
    #                      user_properties, device_info, geo_info, app_info])

    return user_properties, user_id, device_info, geo_info, app_info, event_name, event_params


def convert_from_json(rows):
    field_names = [field.name for field in rows.schema]
    events_dict_array = []
    for row in rows:
        user_properties, user_id, device_info, geo_info, app_info, event_name, event_params = extract_values(row,
                                                                                                             field_names)
        #  Separting UserJSON from the list
        user_json = flatten_user_json(user_properties, user_id)

        #  Separting DeviceType from the list
        device_info_key_set = ["category", "operating_system",
                               "operating_system_version"]

        device_json = flatten_input_info(device_info,
                                         device_info_key_set, "Device JSON")
        #  Separting GeoType from the list
        geo_json = flatten_input_info(geo_info, ["country", "region",
                                                 "city"], "GEO JSON")
        #  Separating Events From the list
        events_json = flatten_event_json(event_params)

        # Separting Version from the list
        app_info_json = flatten_input_info(app_info,
                                           ["version"], "App JSON")

        single_entry = json.loads(user_json)
        single_entry.update(json.loads(device_json))
        single_entry.update(json.loads(geo_json))
        single_entry.update(json.loads(app_info_json))
        single_entry.update(json.loads(events_json))
        events_dict_array.append(single_entry)

    return events_dict_array


def get_day_visitors(_date):
    DATA_SETS = list(CLIENT.list_datasets())
    development_data_set = None
    for data_set in DATA_SETS:
        if data_set.dataset_id == DATASET_ID:
            print(data_set)
            development_data_set = data_set
            break
    input_date = str(datetime.datetime.strftime(
        _date, '%Y%m%d'))
    print(input_date)
    table_name = 'events_' + input_date

    query = """
        SELECT COUNT(DISTINCT(user_pseudo_id)) as result FROM `%s.%s.%s` """ % (development_data_set.project,
                                                               development_data_set.dataset_id,
                                                               table_name)
    return CLIENT.query(query).result()


def todays_visitors():
    DATA_SETS = list(CLIENT.list_datasets())
    development_data_set = None
    for data_set in DATA_SETS:
        if data_set.dataset_id == DATASET_ID:
            print(data_set)
            development_data_set = data_set
            break
    input_date = str(datetime.datetime.strftime(
        datetime.datetime.now(), '%Y%m%d'))

    table_name = 'events_intraday_' + input_date

    query = """
        SELECT DISTINCT(user_pseudo_id) FROM `%s.%s.%s` """ % (development_data_set.project,
                                                               development_data_set.dataset_id,
                                                               table_name)
    return CLIENT.query(query).result()


def get_week_days_from_today():
    start = pendulum.now().start_of("week")
    days = []
    while start.strftime("%Y%m%d") != pendulum.today().strftime("%Y%m%d"):
        days.append(start)
        start += datetime.timedelta(1)
    days.append((pendulum.today()))
    return days


def get_month_days_from_today():
    start = pendulum.now().start_of("month")
    days = []
    while start.strftime("%Y%m%d") != pendulum.today().strftime("%Y%m%d"):
        days.append(start)
        start += datetime.timedelta(1)
    days.append((pendulum.today()))
    return days


def get_field_names(rows):
    return [field.name for field in rows.schema]


def get_todays_visitors():
    user_ids = []
    try:
        rows = todays_visitors()
        field_names = get_field_names(rows)
        for row in rows:
            user_ids.append(row[field_names.index("user_pseudo_id")])
    except Exception as e:
        # print(e)
        return  user_ids
    return user_ids


def get_week_visitors_from_today():
    count = []
    query = """SELECT COUNT(DISTINCT(user_pseudo_id)) as week_count FROM `analytics_214097931.events_*` WHERE _TABLE_SUFFIX BETWEEN '{}' 
            AND '{}'; """.format(pendulum.now().start_of("week").strftime("%Y%m%d"), pendulum.now().strftime("%Y%m%d"))
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("week_count")])
    return count[0] + len(list(set(get_todays_visitors())))


def get_monthly_visitors_from_today():
    count = []
    query = """SELECT COUNT(DISTINCT(user_pseudo_id)) as month_count FROM `analytics_214097931.events_*` WHERE _TABLE_SUFFIX BETWEEN '{}' 
        AND '{}'; """.format(pendulum.now().start_of("month").strftime("%Y%m%d"), pendulum.now().strftime("%Y%m%d"))
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("month_count")])
    return count[0] + len(list(set(get_todays_visitors())))


# Returns  value of count of yearly visitors
def get_yearly_visitors_from_today():
    count = []
    query = """SELECT COUNT(DISTINCT(user_pseudo_id)) as year_count FROM `analytics_214097931.events_*` WHERE _TABLE_SUFFIX BETWEEN '{}' 
    AND '{}'; """.format(pendulum.now().start_of("year").strftime("%Y%m%d"), pendulum.now().strftime("%Y%m%d"))
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("year_count")])
    return count[0] + len(list(set(get_todays_visitors())))


def get_total_visitors_from_today():
    count = []
    query = """SELECT COUNT(DISTINCT(user_pseudo_id)) as year_count FROM `analytics_214097931.events_*` WHERE _TABLE_SUFFIX BETWEEN '{}' 
    AND '{}'; """.format("20200101", pendulum.now().strftime("%Y%m%d"))
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("year_count")])
    return count[0] + len(list(set(get_todays_visitors())))


def get_average_time_spent():
    count = []
    query = """SELECT AVG(V) AS avg_time_spent FROM(
      (SELECT SUM(e.value.int_value/1000) AS V FROM
      `analytics_214097931.events_*`, UNNEST(event_params) 
      as e WHERE e.key="engagement_time_msec" GROUP BY user_pseudo_id
      ));"""
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("avg_time_spent")])
    return count[0]/60


def get_active_users():
    count = []
    query = """SELECT
      COUNT(DISTINCT user_id) AS n_day_active_users_count
    FROM
      `analytics_214097931.events_*`
    WHERE
      event_name = 'user_engagement'
      AND event_timestamp >
          UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 10 DAY))
      -- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE.
      AND _TABLE_SUFFIX BETWEEN '{}' AND FORMAT_DATE("%Y%m%d",CURRENT_DATE());""".format(pendulum.now().start_of("year").strftime("%Y%m%d"))
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("n_day_active_users_count")])
    return count[0]


def get_registered_users():
    count = []
    query = """SELECT
      COUNT(DISTINCT user_id) AS registered_users
    FROM
      `analytics_214097931.events_*`
    WHERE
      user_id is not null;"""
    rows = CLIENT.query(query).result()
    field_names = get_field_names(rows)
    for row in rows:
        count.append(row[field_names.index("registered_users")])
    return count[0]


def get_y_m_d(date):
    return date.strftime("%Y-%m-%d")


def get_day_count(date, count):
    return {"name": get_y_m_d(date), "count": count}


def get_week_daily_visitors():
    user_count = []

    stop = pendulum.now() - datetime.timedelta(6)
    start = pendulum.now() - datetime.timedelta(1)

    try:
        count = len(get_todays_visitors())
        user_count.append(get_day_count(pendulum.now(), count))

    except Exception as e:
        user_count.append(get_day_count(pendulum.now(), 0))

    while start.strftime("%Y%m%d") != stop.strftime("%Y%m%d"):
        try:
            rows = get_day_visitors(start)
            field_names=get_field_names(rows)
            print("FIELD_NAMES: {}".format(field_names))
            for row in rows:
                result = row[field_names.index("result")]
                print("RESULT: {}".format(result))
                user_count.append(get_day_count(start, result) if result is not None else get_day_count(start, 0))
            start -= datetime.timedelta(1)

        except Exception as e:
            start -= datetime.timedelta(1)
            user_count.append(get_day_count(start, 0))
            continue
    print(user_count)
    return user_count


def get_dashboard_stats():
    today = []
    try:
        today = get_todays_visitors()
    except Exception as e:
        today = []
    return {
        "today": len(today),
        "this_week": get_week_visitors_from_today(),
        "month": get_monthly_visitors_from_today(),
        "year": get_yearly_visitors_from_today(),
        "avg_time": get_average_time_spent(),
        "active_visitors": get_active_users(),
        "registered_visitors": get_total_visitors_from_today(),
        "reverse_daily_visitors": get_week_daily_visitors()
    }
