import csv
import datetime
import logging
from collections import defaultdict
from http import HTTPStatus
from typing import Dict, Union, List, Set
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


def get_url(series, from_date: datetime, to_date: datetime):
    base_uri = 'https://sdw-wsrest.ecb.europa.eu/service/data/EXR/'
    from_date_str = f"?startPeriod={from_date.strftime('%Y-%m-%d')}"
    to_date_str = f"?endPeriod={to_date.strftime('%Y-%m-%d')}"
    return base_uri + series + from_date_str + '&' + to_date_str


def get_data(data):
    parsed_data = defaultdict(lambda: defaultdict(dict))
    for series in data:
        currency = series['generic:SeriesKey']['generic:Value'][1]['@value']
        for item in series['generic:Obs']:
            value = float(item['generic:ObsValue']['@value'])
            date = datetime.datetime.strptime(item['generic:ObsDimension']['@value'], '%Y-%m-%d').date()
            parsed_data[date][currency]['EUR'] = value
            parsed_data[date]['EUR'][currency] = 1.0 / value
    return parsed_data


def query(key: str, from_date: datetime, to_date: datetime):
    try:
        with urlopen(get_url(series=key, from_date=from_date, to_date=to_date)) as url:
            raw = parse(url.read().decode('utf8'))
    except HTTPError as e:
        e.msg = ERROR_MESSAGES[e.code]
        raise
    result = get_data(data=raw['message:GenericData']['message:DataSet']['generic:Series'])
    logging.debug(f"Request for (key={key}, from_date={from_date}, to_date={to_date}) resulted in {len(result)} items.")
    return result


def get_currency_to_all(data: Dict, date: datetime, currencies: Set):
    for currency in currencies:
        for currency_denom in currencies - {currency}:
            data[date][currency][currency_denom] = data[date][currency]['EUR'] / data[date][currency_denom]['EUR']


def get_rows(data: Dict, date: datetime, currencies: Set, from_currencies: List, to_currencies: List):
    from_currencies = from_currencies if from_currencies else currencies
    to_currencies = to_currencies if to_currencies else currencies

    for currency in from_currencies:
        for currency_denom in to_currencies - {currency}:
            yield date, currency, currency_denom, data[date][currency][currency_denom]


def post_process(data: Dict, from_date: datetime, to_date: datetime, from_currencies: List, to_currencies: List):
    dates_range = [(from_date + datetime.timedelta(days=d)) for d in range((to_date - from_date).days)]
    currencies = {'CHF', 'BGN', 'HKD', 'PHP', 'RON', 'NZD', 'SGD', 'KRW', 'CZK', 'PLN', 'SEK', 'DKK', 'GBP', 'ISK',
                  'USD', 'MYR', 'HUF', 'IDR', 'MXN', 'ZAR', 'AUD', 'ILS', 'CAD', 'THB', 'TRY', 'NOK', 'HRK', 'BRL',
                  'JPY', 'INR', 'CNY'}
    yield ['date', 'currency', 'currency_denom', 'value']

    for date in dates_range:
        if date in data:
            get_currency_to_all(data, date, currencies)
        else:
            if min(data.keys()) >= date:
                logging.warning(f"Data for {date} not found!")
            else:
                data[date] = data[date - datetime.timedelta(days=1)]
        yield from get_rows(data, date, currencies, from_currencies, to_currencies)


def daily_exchange_rate(from_currencies: List, to_currencies: List, from_date: datetime, to_date: datetime, t: int = 4):
    key = get_key(currency=[], currency_denom=[])
    result = query(key=key, from_date=from_date - datetime.timedelta(days=t), to_date=to_date)
    return post_process(data=result,
                        from_date=from_date,
                        to_date=to_date,
                        from_currencies=from_currencies,
                        to_currencies=to_currencies)


if __name__ == "__main__":
    from_date = datetime.date.today() - datetime.timedelta(days=7)
    to_date = datetime.date.today()
    result = daily_exchange_rate(from_currencies=[], to_currencies=[], from_date=from_date, to_date=to_date)

    with open("output.csv", 'w', newline='') as file:
        writer = csv.writer(file)
        for row in result:
            writer.writerow(row)
