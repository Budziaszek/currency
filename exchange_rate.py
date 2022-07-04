import csv
import datetime
import logging
from collections import defaultdict
from http import HTTPStatus
from typing import Dict, Union, List
from urllib.error import HTTPError
from urllib.request import urlopen

from xmltodict import parse

logging.getLogger().setLevel(logging.DEBUG)

ERROR_MESSAGES = {
    HTTPStatus.SEE_OTHER: "No changes since the timestamp supplied in the If-Modified-Since header.",
    HTTPStatus.BAD_REQUEST: "There is a syntactic or semantic issue with the parameters you supplied.",
    HTTPStatus.NOT_FOUND: "There are no results matching the query.",
    HTTPStatus.NOT_ACCEPTABLE: "A resource representation is not supported. See the section about content negotiation, "
                               "to view the supported representations.",
    HTTPStatus.INTERNAL_SERVER_ERROR: "There is an issue on server side. "
                                      "Feel free to try again later or to contact our support hotline.",
    HTTPStatus.NOT_IMPLEMENTED: "This web service offers a subset of the functionality offered by the SDMX RESTful "
                                "web service specification. A feature that is not yet implemented.",
    HTTPStatus.SERVICE_UNAVAILABLE: "Web service is temporarily unavailable",
}


def get_key(currency: Union[str, List], currency_denom: Union[str, List], frequency: str = 'D',
            exchange_rates_type: str = 'SP00', series_variation: str = 'A'):
    if isinstance(currency, list):
        currency = '+'.join(currency)
    if isinstance(currency_denom, list):
        currency_denom = '+'.join(currency_denom)

    key = '.'.join([frequency, currency, currency_denom, exchange_rates_type, series_variation])
    logging.debug(f"Created key: {key}")
    return key


def get_url(series, from_date: datetime, to_date: datetime, csv_data=True):
    base_uri = 'https://sdw-wsrest.ecb.europa.eu/service/data/EXR/'
    from_date_str = f"?startPeriod={from_date.strftime('%Y-%m-%d')}"
    to_date_str = f"?endPeriod={to_date.strftime('%Y-%m-%d')}"
    return base_uri + series + from_date_str + '&' + to_date_str


def was_requested(currency_data, from_currencies, to_currencies):
    if from_currencies is None or currency_data['CURRENCY'] in from_currencies:
        if to_currencies is None or currency_data['CURRENCY_DENOM'] in to_currencies:
            return True
    return False


def get_data(data, from_currencies=None, to_currencies=None):
    parsed_data = defaultdict(dict)
    for series in data:
        currency_data = {
            'CURRENCY': series['generic:SeriesKey']['generic:Value'][1]['@value'],
            'CURRENCY_DENOM': series['generic:SeriesKey']['generic:Value'][2]['@value']
        }
        if was_requested(currency_data, from_currencies, to_currencies):
            for item in series['generic:Obs']:
                row = {
                    'VALUE': item['generic:ObsValue']['@value'],
                    **currency_data
                }
                label = f'{currency_data["CURRENCY"]}-{currency_data["CURRENCY_DENOM"]}'
                parsed_data[label][item['generic:ObsDimension']['@value']] = row
    return parsed_data


def query(key: str,
          from_date: datetime,
          to_date: datetime):
    try:
        with urlopen(get_url(series=key, from_date=from_date, to_date=to_date)) as url:
            raw = parse(url.read().decode('utf8'))
    except HTTPError as e:
        e.msg = ERROR_MESSAGES[e.code]
        raise

    result = get_data(data=raw['message:GenericData']['message:DataSet']['generic:Series'])
    logging.debug(f"Request for (key={key}, from_date={from_date}, to_date={to_date}) resulted in {len(result)} items.")
    return result


def get_inverse_currency(value):
    return value['CURRENCY_DENOM'], value['CURRENCY'], 1.0 / float(value['VALUE'])


def get_currency_row(value):
    return value['CURRENCY'], value['CURRENCY_DENOM'], value['VALUE']


def post_process(data: Dict, from_date: datetime, to_date: datetime):
    dates_range = [(from_date + datetime.timedelta(days=d)).strftime('%Y-%m-%d')
                   for d in range((to_date - from_date).days)]

    yield ['date', 'currency', 'currency_denom', 'value']
    for key, value in data.items():
        last = None

        for date in dates_range:
            if date in value:
                last = value[date]

            yield get_currency_row(last)
            yield get_inverse_currency(last)


def daily_exchange_rate(from_currencies: List, to_currencies: List, from_date: datetime, to_date:datetime):
    key = get_key(currency=from_currencies, currency_denom=to_currencies)
    result = query(key=key, from_date=from_date, to_date=to_date)
    return post_process(data=result, from_date=from_date, to_date=to_date)


if __name__ == "__main__":
    from_date = datetime.date.today() - datetime.timedelta(days=7)
    to_date = datetime.date.today()
    result = daily_exchange_rate(from_currencies=[], to_currencies=[], from_date=from_date, to_date=to_date)

    with open("output.csv", 'w', newline='') as file:
        writer = csv.writer(file)
        for row in result:
            writer.writerow(row)
