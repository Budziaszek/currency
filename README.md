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
When no data is available for a specific day, the day will include the last known values from previous days. Request
always include margin to avoid situation when the first date is a bank holiday or weekend day. Margin is currently set
to 4 days.

# Improvements
* caching
* storing copy of post processed data - if there are limits, or processing is time-consuming
* not requesting all data always (but I guess it does not change a lot)

# TODO
* tests (not required)
