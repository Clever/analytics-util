package analyticspipeline

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	alcsWagClient "github.com/Clever/analytics-latency-config-service/gen-go/client"
	alcs "github.com/Clever/analytics-latency-config-service/gen-go/models"
	alcsHelpers "github.com/Clever/analytics-latency-config-service/helpers"
	kvlogger "gopkg.in/Clever/kayvee-go.v6/logger"

	gomock "github.com/golang/mock/gomock"
)

const (
	expectedDistrict   = "abc123"
	expectedCollection = "schools"
)

var (
	errMissingDistrictField = fmt.Errorf(missingValuesErrTemplate, []string{"district_id"})
	emptyCurrent            = []byte("{}")
	emptyRemaining          = []byte("[]")
)

func TestAnalyticsWorker(t *testing.T) {
	for _, spec := range []struct {
		context      string
		args         []string
		err          error
		district     string
		collection   string
		newCurrent   []byte
		newRemaining []byte
		newDone      bool
	}{
		{
			context:      "normal case w/ flags",
			args:         []string{"-district_id=abc123"},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
			newDone:      true,
		},
		{
			context: "missing required field",
			err:     errMissingDistrictField,
		},
		{
			context: "given other field but not required field",
			args:    []string{"-collection=schools"},
			err:     errMissingDistrictField,
		},
		{
			context:      "normal case w/ json",
			args:         []string{`{ "current": {"district_id":"abc123"}, "remaining": [] }`},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
			newDone:      true,
		},
		{
			context:      "unwrapped json",
			args:         []string{`{"district_id":"abc123"}`},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
			newDone:      true,
		},
		{
			context:      "json w/ all fields",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
			newDone:      false,
		},
		{
			context:      "json w/ remaining",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{"district_id":"abc456"}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   []byte("{\"district_id\":\"abc456\"}"),
			newRemaining: emptyRemaining,
			newDone:      false,
		},
		{
			context:      "json w/ remaining array",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{"district_id":"abc456"},{"district_id":"abc789"}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   []byte("{\"district_id\":\"abc456\"}"),
			newRemaining: []byte("[{\"district_id\":\"abc789\"}]"),
			newDone:      false,
		},
		{
			context: "empty JSON blob",
			err:     errMissingDistrictField,
		},
		{
			context: "fails with broken JSON",
			args:    []string{`{"collection":"not closed, oops"`},
			err:     errInvalidJSON,
		},
		{
			context: "only evaluates flags if provided first",
			args:    []string{"-collection=schools", `{"district_id":"abc123"}`},
			err:     errMissingDistrictField,
		},
		{
			context: "fails with non-declared flags",
			args:    []string{"-district_id=abc123", "-random-test-flag=X"},
			err:     errors.New("flag provided but not defined: -random-test-flag"),
		},
	} {
		// NOTE: we override both the os.Args and flag.Commandline variables to allow
		// repeated calls to the flag library.
		os.Args = append([]string{"test"}, spec.args...)
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

		var config struct {
			DistrictID string `config:"district_id,required"`
			Collection string `config:"collection"`
		}
		newPayload, err := AnalyticsWorker(&config)
		if spec.err == nil {
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.district, config.DistrictID, "Case '%s'", spec.context)
			assert.Equal(t, spec.collection, config.Collection, "Case '%s'", spec.context)
			retrievedCurrent, err := json.Marshal(newPayload.Current)
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.newCurrent, retrievedCurrent)
			retrievedRemaining, err := json.Marshal(newPayload.Remanining)
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.newRemaining, retrievedRemaining)
			assert.Equal(t, spec.newDone, newPayload.Done)
		} else {
			assert.Equal(t, spec.err, err, "Case '%s'", spec.context)
		}
	}
}

func floatPtr(f float64) *float64 {
	return &f
}

func TestIsTableDataFresh(t *testing.T) {
	for _, spec := range []struct {
		description string
		database    alcs.AnalyticsDatabase
		schema      string
		table       string
		refresh     string
		latency     *float64
		queryError  error
		isFresh     bool
	}{
		{
			description: "indicates when fresh",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     "10h",
			latency:     floatPtr(5),
			queryError:  nil,
			isFresh:     true,
		},
		{
			description: "indicates when stale",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     "10h",
			latency:     floatPtr(15),
			queryError:  nil,
			isFresh:     false,
		},
		{
			description: "refreshes when no refresh set",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     alcsHelpers.NoLatencyAlert,
			latency:     floatPtr(5),
			queryError:  nil,
			isFresh:     false,
		},
		{
			description: "refreshes when latency is unset",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     "10h",
			latency:     nil,
			queryError:  nil,
			isFresh:     false,
		},
		{
			description: "refreshes when alcs errors",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     "10h",
			latency:     floatPtr(5),
			queryError:  fmt.Errorf("connection error"),
			isFresh:     false,
		},
		{
			description: "refreshes when threshold is invalid (though it shouldn't happen)",
			database:    alcs.AnalyticsDatabaseRedshiftFast,
			schema:      "schema",
			table:       "table",
			refresh:     "10j",
			latency:     floatPtr(5),
			queryError:  fmt.Errorf("connection error"),
			isFresh:     false,
		},
	} {
		mockCtrl := gomock.NewController(t)
		mockALCS := alcsWagClient.NewMockClient(mockCtrl)
		mockLogger := kvlogger.NewMockCountLogger("fresh-test")
		defer mockCtrl.Finish()

		latencyResp := &alcs.GetTableLatencyResponse{
			Database: spec.database,
			Schema:   &spec.schema,
			Table:    &spec.table,
			Latency:  spec.latency,
			Thresholds: &alcs.Thresholds{
				Refresh: spec.refresh,
			},
		}
		if spec.queryError != nil {
			latencyResp = nil
		}

		mockALCS.EXPECT().GetTableLatency(context.TODO(), &alcs.GetTableLatencyRequest{
			Database: spec.database,
			Schema:   &spec.schema,
			Table:    &spec.table,
		}).Return(latencyResp, spec.queryError)

		fresh := IsTableDataFresh(mockLogger, mockALCS, spec.database, spec.schema, spec.table)
		assert.Equal(t, spec.isFresh, fresh)
	}
}
