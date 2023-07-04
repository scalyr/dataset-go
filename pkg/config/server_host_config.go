package config

import "fmt"

type DataSetServerHostSettings struct {
	UseHostName bool
	ServerHost  string
}

func NewDefaultDataSetServerHostSettings() DataSetServerHostSettings {
	return DataSetServerHostSettings{
		UseHostName: true,
		ServerHost:  "",
	}
}

func (cfg *DataSetServerHostSettings) String() string {
	return fmt.Sprintf(
		"UseHostName: %t, ServerHost: %s",
		cfg.UseHostName,
		cfg.ServerHost,
	)
}

func (cfg *DataSetServerHostSettings) Validate() error {
	if !cfg.UseHostName && len(cfg.ServerHost) == 0 {
		return fmt.Errorf("when UseHostName is False, then ServerHost has to be set")
	}
	return nil
}
