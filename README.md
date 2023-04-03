# DataSet-Go

This repository holds the source code for the Go wrapper around [DataSet API](https://app.scalyr.com/help/api).

To learn more about DataSet, visit https://www.dataset.com.

## Features

The DataSet API supports following API calls:

* [addEvents](https://app.scalyr.com/help/api#addEvents) - This endpoint inserts one or more structured or unstructured log events. It can scale horizontally to ingest 100s of TBs per day.


## Examples

```go
package main

import (
    "fmt"
    "net/http"
    "time"

    "github.com/scalyr/dataset-go/pkg/api/add_events"
    "github.com/scalyr/dataset-go/pkg/client"
    "github.com/scalyr/dataset-go/pkg/config"
    "go.uber.org/zap"
)

func main() {
    // read configuration from env variables
    cfg, err := config.New(config.FromEnv())
    if err != nil { panic(err) }

    // build client
    cl, err := client.NewClient(
		cfg,
		&http.Client{},
        zap.Must(zap.NewDevelopment()),
    )
    if err != nil { panic(err) }

    // send all buffers when we want to finish
    defer cl.SendAllAddEventsBuffers()

	// build event
    event := &add_events.Event{
        Thread: "T",
        Log:    "L",
        Sev:    3,
        Ts:     fmt.Sprintf("%d", time.Now().Nanosecond()),
        Attrs: map[string]interface{}{
            "message": "dataset-go library - test message",
            "attribute": 42,
        },
    }

	// build thread
    thread := &add_events.Thread{Id: "T", Name: "thread-1"}

	// build log
	log := &add_events.Log{
        Id: "L",
        Attrs: map[string]interface{}{
            "attr 1": "value 1",
        },
    }

	// build bundle
    eventBundle := &add_events.EventBundle{
        Event:  event,
        Thread: thread,
        Log:    log,
    }

	// send it
    err = cl.AddEvents([]*add_events.EventBundle{eventBundle})
	if err != nil { panic(err) }
}
```


Examples are located in the [examples](examples) folder. You can check [simple client](examples/client/main.go)
## Release Notes and Changelog

For release notes please see [RELEASE_NOTES.md](RELEASE_NOTES.md) document and for changelog,
see [CHANGELOG.md](CHANGELOG.md) document.

## Developing


### Pre-Commit Hooks

[Pre-commit](https://pre-commit.com) is used to automatically run checks including Black formatting
prior to a git commit.

To use pre-commit:

- Use one of the [Installation Methods](https://pre-commit.com/#install) from the documentation.
- Install the hooks with `pre-commit install`.
- To manually execute the pre-commit hooks (including black), run `pre-commit run --all-files` (
  run it on all the files) or ``pre-commit run`` (run it only on staged files).

#### Pre-commit Configuration

- `.pre-commit-config.yaml` configures the scripts run by pre-commit

To update the Pre-commit hooks , run `pre-commit autoupdate`. This will update
`.pre-commit-config.yaml` and will need to be committed to the repository.

## Contributing

In the future, we will be pushing guidelines on how to contribute to this repository.  For now, please just
feel free to submit pull requests to the `main` branch and we will work with you.

## Copyright, License, and Contributors Agreement

Copyright 2023 SentinelOne, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in
compliance with the License. You may obtain a copy of the License in the [LICENSE](LICENSE) file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

By contributing you agree that these contributions are your own (or approved by your employer) and you
grant a full, complete, irrevocable copyright license to all users and developers of the project,
present and future, pursuant to the license of the project.
