# Release Notes

## 0.0.7 Add more retry parameters

* To make OpenTelemetry configuration more stable we have introduced more retry options. We have to propagate them to the library.
* Fix another data race caused by `client.worker`.

## 0.0.6 Fix Concurrency Issues

* OpenTelemetry can call AddEvents multiple times in parallel. Let's use another Pub/Sub to publish events into topic and then consume them independently.

## 0.0.5 Quick Hack

* OpenTelemetry can call AddEvents multiple times in parallel. Add lock so only one of them is in progress in any given time.

## 0.0.4 Fix Concurrency Issues

* sometimes not all events have been delivered exactly once

## 0.0.3 Fix Data Races

* fixed [data races](https://go.dev/doc/articles/race_detector)

## 0.0.2 Fix Data Races

* fixed [data races](https://go.dev/doc/articles/race_detector)
* added function `client.Finish` to allow waiting on processing all events
## 0.0.1 Initial Release

* support for API call [addEvents](https://app.scalyr.com/help/api#addEvents)
