# Architecture

This library is under development, and it's primarily designed to fulfil needs of the
open telemetry exporter and therefore some functionality that would be useful in other
scenarios is not implemented.

## Idea

Design is heavily influenced by peculiarities of the upstream and downstream systems
about which I know very little.

### Downstream

The downstream system is DataSet REST API and its API call [addEvents](https://app.scalyr.com/help/api#addEvents).
It has the following properties:

* `D1` - only one request for each session could be made
* `D2` - when new version is deployed there is few minutes outage
* `D3` - maximum payload size for the API is 6MB before compression

### Upstream

The upstream system is [open telemetry collector](https://opentelemetry.io/docs/collector/),
where the datasetexporter is one of many exporters that could be used
(on the right side of the image). It has the following properties:

* `U1` - data are passed in batches
* `U2` - each batch could be either accepted or rejected, if it's accepted it cannot be returned, if it's rejected, it may be retried
* `U3` - function for accepting batches could be called in parallel (with different batches)

### Consequences

As a consequence of the upstream and downstream system following has to happen (codes
between brackets points to the previous points):

* `C1` - data has to be grouped to be divided into multiple sessions to increase throughput (`D1`)
* `C2` - data from multiple input batches has to be combined (`D1`, `U1`)
* `C3` - input and output has to be decoupled (`D1`, `C1`, `C2`)
* `C4` - if the downstream returns error, system cannot return data that has already accepted, it can only start rejecting new data (`U2`, `U3`)
* `C5` - since the system cannot return data, it has to handle retries by itself (`C4`)
* `C6` - user has to be able to define the grouping mechanism to achieve maximum throughput (`C1`)

## Implementation

* `I1` - to decouple upstream from downstream we use pub/sub pattern (`C3`)
* `I2` - there are two levels of pub/sub patterns
  * `I2.1` - publisher 1 - events from incoming batch are divided based on the grouping into sessions and published into the corresponding topic (`I1`, `C6`)
  * `I2.2` - subscriber 1 / publisher 2 - reads individual events from the topic and combines them into batches, batches are published into the corresponding topic (`C2`)
  * `I2.3` - publisher 2 - output batch is published even if it's not full after some time period
  * `I2.4` - subscriber 2 - reads output batches from the topic and calls DataSet API server (`D1`)
* `I3` - each session has its own go routine (`I2.1`)

### Error Propagation

Error state is shared among all sessions. It works in the following way:

* when error is returned from the API call then:
  * system enters error mode
  * internal retry mechanism is triggered
* when the system is in error mode, then:
  * new incoming batches are rejected so that external retry mechanism is triggered
  * internal retry are still happening
* error state is cleared when:
  * some of the internal retries succeeds
  * it should be cleared after some timeout as well - https://sentinelone.atlassian.net/browse/DSET-4080

### Internal Batching

This section describes how internal batching works for data belonging to the single session:

* structure that is used for internal batching is call `Buffer`
* `Buffer` has maximum allowed size (`D3`)
* events are added into the `Buffer` until it's full (`I2.2`)
* if the buffer is not full and is older than `max_lifetime` then it's published as well (`I2.3`)
* if the event is larger than remaining space in current `Buffer`, then the current `Buffer` is published and event is added into the new `Buffer`
* if the event is larger than the maximum allowed size, then attribute values are trimmed - from the longest to the shortest
