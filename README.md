#Currency exchange rate

## Notes
## CSV vs xml
Requesting results in csv formats requires less formatting. Depending on the requirements I would consider using it,
but I noticed, that it doesn't support selecting time period. If we would always define relatively short period
of time I believe that it is better to parse xml.

## Additional data
I have used loop to get all metadata about series, but then decided to get only the attributes, which I was using.
(see commit df0c2fda)

```python
currency_data = {
    item['@id']: item['@value']
    for group in ('generic:SeriesKey', 'generic:Attributes')
    for item in series[group]['generic:Value']
    if item['@id'] in ('CURRENCY', 'CURRENCY_DENOM',)
}
```

## No data available
When no data is available for a specific day, the day will include the last known values from previous days,
but there is one exception - currently, if start date given is the data for which the service will not return results.
I would solve this by running additional queries in post processing (post_process).

## Currency denomination
It seems that service has only data for EUR denomination. Am I missing something or should I calculate
values?

# TODO
* tests
* no data available (start date case)
* conversion to any currency

# Improvements
* caching
* storing copy of post processed data - if there are limits, or processing is time-consuming


