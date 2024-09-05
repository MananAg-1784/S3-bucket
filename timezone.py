
from datetime import datetime
import pytz

def change_timezone(date_:datetime, timezone:str = "Asia/Kolkata") -> datetime:
    '''
    Changes the datetime from utc to a particular time zone

    :param date_ : The datetime object to change the time
    :param timezone : (Optional) Timezone, Default : Kolkata. India 
    :return : Datetime object with the timezone changed date
    '''
    try:
        timezone = pytz.timezone(timezone)
        return date_.astimezone(timezone)
    except Exception as e:
        print("Cannot convert t loacl time zone: ", e)
        return date_