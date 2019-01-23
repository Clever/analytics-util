package analyticspipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
)

const (
	structTagKey             = "config"
	requiredTagKey           = "required"
	missingValuesErrTemplate = "Missing required fields: %s"
)

var (
	errStringAndBoolOnly      = errors.New("only string/bool values are allowed in a config struct")
	errBoolCannotBeRequired   = errors.New("boolean attributes cannot be required")
	errNotReference           = errors.New("the config struct must be a pointer to a struct")
	errStructOnly             = errors.New("config object must be a struct")
	errNoTagValue             = errors.New("config object attributes must have a 'config' tag value")
	errTooManyTagValues       = errors.New("config object attributes can only have a key and optional required attribute")
	errFlagParsed             = errors.New("the flag library cannot be used in conjunction with configure")
	errInvalidJSON            = errors.New("invalid JSON found in arguments")
	errStructTagInvalidOption = errors.New("only 'required' is a config option")
)

// Payload is the standard shape of a worker payload in the analytics pipeline
type Payload struct {
	Current    map[string]interface{}   `json:"current"`
	Remanining []map[string]interface{} `json:"remaining"`
}

// PrintPayload prints a passed in Payload
func PrintPayload(payload *Payload) {
	if payload == nil {
		return
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	_, err = fmt.Println(string(bytes))
	if err != nil {
		panic(err)
	}
}

// parseTagKey parses the values in a tag.
func parseTagKey(tag string) (key string, required bool, err error) {
	if tag == "" {
		return "", false, errNoTagValue
	}

	s := strings.Split(tag, ",")
	switch len(s) {
	case 2:
		if s[1] != requiredTagKey {
			return "", false, errStructTagInvalidOption
		}
		return s[0], true, nil
	case 1:
		return s[0], false, nil
	default:
		return "", false, errTooManyTagValues
	}
}

func createFlags(config reflect.Value, configFlags *flag.FlagSet, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) error {
	// this block creates flags for every attribute
	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		if !valueField.CanSet() {
			return errNotReference
		}

		// currently we only support strings and bools
		typedAttr := config.Type().Field(i)
		if typedAttr.Type.Kind() != reflect.String && typedAttr.Type.Kind() != reflect.Bool {
			return errStringAndBoolOnly
		}

		// get the name of the value and create a flag
		tagVal, _, err := parseTagKey(typedAttr.Tag.Get(structTagKey))
		if err != nil {
			return err
		}
		switch typedAttr.Type.Kind() {
		case reflect.String:
			flagStringValueMap[tagVal] = configFlags.String(tagVal, "", "generated field")
		case reflect.Bool:
			// set the default to the value passed in
			flagBoolValueMap[tagVal] = configFlags.Bool(tagVal, config.Field(i).Bool(), "generated field")
		}
	}
	return nil
}

func retrieveFlagValues(config reflect.Value, configFlags *flag.FlagSet, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) error {
	if err := createFlags(config, configFlags, flagStringValueMap, flagBoolValueMap); err != nil {
		return err
	}
	if err := configFlags.Parse(os.Args[1:]); err != nil {
		return err
	}
	return nil
}

func populateFromFlagMaps(config reflect.Value, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) (bool, error) {
	flagFound := false
	// grab values from flag map
	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return flagFound, err
		}

		typedAttr := config.Type().Field(i)
		switch typedAttr.Type.Kind() {
		case reflect.String:
			if *flagStringValueMap[tagVal] != "" {
				flagFound = true
				valueField.SetString(*flagStringValueMap[tagVal])
			}
		case reflect.Bool:
			// we can only know if a bool flag was set if the default was changed
			if *flagBoolValueMap[tagVal] != config.Field(i).Bool() {
				flagFound = true
			}
			valueField.SetBool(*flagBoolValueMap[tagVal]) // always set from flags
		}
	}
	return flagFound, nil
}

func parseFromJSON(config reflect.Value, configFlags *flag.FlagSet) error {
	jsonValues := map[string]interface{}{}
	if err := json.NewDecoder(bytes.NewBufferString(configFlags.Arg(0))).Decode(&jsonValues); err != nil {
		return errInvalidJSON
	}

	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return err
		} else if _, ok := jsonValues[tagVal]; ok {
			typedAttr := config.Type().Field(i)
			switch typedAttr.Type.Kind() {
			case reflect.String:
				valueField.SetString(jsonValues[tagVal].(string))
			case reflect.Bool:
				valueField.SetBool(jsonValues[tagVal].(bool))
			}
		}
	}

	return nil
}

func validateRequiredFields(config reflect.Value) error {
	missingRequiredFields := []string{}
	for i := 0; i < config.NumField(); i++ {
		tagKey, required, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return err
		} else if required {
			switch config.Field(i).Type().Kind() {
			case reflect.String:
				if config.Field(i).String() == "" {
					missingRequiredFields = append(missingRequiredFields, tagKey)
				}
			case reflect.Bool:
				return errBoolCannotBeRequired
			}
		}
	}
	if len(missingRequiredFields) > 0 {
		return fmt.Errorf(missingValuesErrTemplate, missingRequiredFields)
	}

	return nil
}

// AnalyticsWorker does the same as Configure, except JSON input is parsed differently.
// Instead of containing just the structure of configStruct, JSON is expected to have a "current"
// object that matches configStruct and an array of "remaining" payloads for future workers in the
// workflow. Remaining payloads are returned as a printable []byte.
func AnalyticsWorker(configStruct interface{}) (*Payload, error) {
	if flag.Parsed() {
		return nil, errFlagParsed
	}

	reflectConfig := reflect.ValueOf(configStruct)
	if reflectConfig.Kind() != reflect.Ptr {
		return nil, errStructOnly
	}

	var (
		configFlags        = flag.NewFlagSet("configure", flag.ContinueOnError)
		flagStringValueMap = map[string]*string{} // holds references to attribute string flags
		flagBoolValueMap   = map[string]*bool{}   // holds references to attribute bool flags
		config             = reflectConfig.Elem()
	)

	if err := retrieveFlagValues(config, configFlags, flagStringValueMap, flagBoolValueMap); err != nil {
		return nil, err
	}

	flagFound, err := populateFromFlagMaps(config, flagStringValueMap, flagBoolValueMap)
	if err != nil {
		return nil, err
	}

	// if no flags were found and we have a value in the first arg, we try to parse JSON from it.
	analyticsPayload := Payload{}
	if !flagFound && configFlags.Arg(0) != "" {
		if err := json.NewDecoder(bytes.NewBufferString(configFlags.Arg(0))).Decode(&analyticsPayload); err != nil {
			return nil, errInvalidJSON
		}

		if analyticsPayload.Current == nil {
			err := attemptUnwrappedPayload(configFlags, config)
			if err != nil {
				return nil, err
			}
		}

		for i := 0; i < config.NumField(); i++ {
			valueField := config.Field(i)
			tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
			if err != nil {
				return nil, err
			} else if _, ok := analyticsPayload.Current[tagVal]; ok {
				typedAttr := config.Type().Field(i)
				switch typedAttr.Type.Kind() {
				case reflect.String:
					valueField.SetString(analyticsPayload.Current[tagVal].(string))
				case reflect.Bool:
					valueField.SetBool(analyticsPayload.Current[tagVal].(bool))
				}
			}
		}
	}

	// validate that all required fields were set
	if err := validateRequiredFields(config); err != nil {
		return nil, err
	}

	result := Payload{
		Current:    map[string]interface{}{},
		Remanining: []map[string]interface{}{},
	}
	if len(analyticsPayload.Remanining) > 0 {
		result.Current = analyticsPayload.Remanining[0]
		result.Remanining = analyticsPayload.Remanining[1:]
	}

	return &result, nil
}

// attemptUnwrappedPayload attempts to parse a payload that is in the old format, without a
// current and remaining attribute.
func attemptUnwrappedPayload(configFlags *flag.FlagSet, config reflect.Value) error {
	unwrappedPayload := map[string]interface{}{}
	if err := json.NewDecoder(bytes.NewBufferString(configFlags.Arg(0))).Decode(&unwrappedPayload); err != nil {
		return errInvalidJSON
	}
	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return err
		} else if _, ok := unwrappedPayload[tagVal]; ok {
			typedAttr := config.Type().Field(i)
			switch typedAttr.Type.Kind() {
			case reflect.String:
				valueField.SetString(unwrappedPayload[tagVal].(string))
			case reflect.Bool:
				valueField.SetBool(unwrappedPayload[tagVal].(bool))
			}
		}
	}
	return nil
}
