from datetime import timedelta, datetime

def parse_duration(duration_str):

    """This transformation is necessary since a video"s duartion is specified in ISO 8601 FORMAT 
    and typically contains duration as PT#M#S, in which the letters PT indicate that the value specifies a period of time, 
    and the letters M and S refer to length in minutes and seconds,
    OR for a day long video teh format is this: P#DT#H#M#S"""

    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D":0, "H":0, "M":0, "S":0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)
    
    total_duration = timedelta(
        days = values["D"],
        hours = values["H"],
        minutes = values["M"],
        seconds = values["S"]
    )

    return total_duration

""" This function will use the parsed duration from API and store it in statging layer,
further we are building a category column to catgorize a video type as 
shorts or normal video based on the duration < 1 min then shorts
"""
def transform_data(row):
    duration_td = parse_duration(row['duration'])  #we defined this row in data_utils.py

    row['duration'] = (datetime.min + duration_td).time()

    row['video_type'] = 'Shorts' if duration_td.total_seconds() <= 120 else 'Normal'

    return row
    