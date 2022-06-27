import datetime
from urllib.request import urlopen

from xmltodict import parse


def get_config(currency: str, currency_denom: str, frequency: str = 'D', exchange_rates_type: str = 'SP00',
               series_variation: str = 'A'):
    return '.'.join([frequency, currency, currency_denom, exchange_rates_type, series_variation])


def get_url(series, from_date: datetime, to_date: datetime, csv_data=True):
    base_uri = 'https://sdw-wsrest.ecb.europa.eu/service/data/EXR/'
    from_date_str = f"?startPeriod={from_date.strftime('%Y-%m-%d')}"
    to_date_str = f"?endPeriod={to_date.strftime('%Y-%m-%d')}"
    return base_uri + series + from_date_str + '&' + to_date_str


def get_data(data):
    parsed_data = []
    for series in data:
        currency_data = {item['@id']: item['@value']
                         for group in ('generic:SeriesKey', 'generic:Attributes')
                         for item in series[group]['generic:Value']
                         if item['@id'] in ('CURRENCY', 'CURRENCY_DENOM',)
                         }
        for item in series['generic:Obs']:
            row = {
                'OBS_VALUE': item['generic:ObsValue']['@value'],
                'OBS_DATE': item['generic:ObsDimension']['@value'],
                **currency_data
            }
            parsed_data.append(row)
    return parsed_data


def query(series: str,
          from_date: datetime,
          to_date: datetime):
    with urlopen(get_url(series=series, from_date=from_date, to_date=to_date)) as url:
        raw = parse(url.read().decode('utf8'))
    return get_data(data=raw['message:GenericData']['message:DataSet']['generic:Series'])


if __name__ == "__main__":
    c = get_config(currency="", currency_denom="")
    print(c)
    print(query(c, from_date=datetime.date.today() - datetime.timedelta(days=7), to_date=datetime.date.today()))
