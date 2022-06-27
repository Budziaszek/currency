from urllib.request import urlopen

from xmltodict import parse


def get_config(currency_a: str, currency_b: str, frequency: str = 'D', exchange_rates_type: str = 'SP00',
               series_variation: str = 'A'):
    return '.'.join([frequency, currency_a, currency_b, exchange_rates_type, series_variation])


def query(series):
    with urlopen("https://sdw-wsrest.ecb.europa.eu/service/data/EXR/" + series) as url:
        raw = parse(url.read().decode('utf8'))

    data = raw['message:GenericData']['message:DataSet']['generic:Series']['generic:Obs']
    res = {d['generic:ObsDimension']['@value']: float(d['generic:ObsValue']['@value']) for d in data}
    return res


if __name__ == "__main__":
    c = get_config("PLN", "EUR")
    print(c)
    print(query(c))