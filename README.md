# Mixpanel Event Import

Import historical events from a CSV into mixpanel.

## Installation

`npm install mixpanel-event-import -g`

## Usage

`mixpanel import <file>`

Options:

```
-t, --token <token>   Mixpanel project token
-k, --api-key <key>   Mixpanel API key
--dry-run             Log, but don't fire, events
-l, --log             Report event status after firing
-d, --date-format     Format for dates (see moment.js docs for formatting options). Defaults to 'MM/DD/YYYY'.
```

Token can additional be specified by the environment variable `MIXPANEL_TOKEN`, and the api-key can be specified with `MIXPANEL_KEY`.

CSVs should be in the following format:

| Event Name | Distinct Id | Date | Key/Value | Key/Value |
|------------|-------------|------|-----------|-----------|
| Did something | kasljdf90 | 12/12/2012 | foo:bar | baz:quux |

where the key value pairs are additional properties to fire with the event. The header is for documentation purposes only. Do not include it in your implementation.
