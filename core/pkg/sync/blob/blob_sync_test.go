package blob

import (
	"context"
	"log"
	"testing"
	"testing/fstest"
	"time"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/sync"
	synctesting "github.com/open-feature/flagd/core/pkg/sync/testing"
	"go.uber.org/mock/gomock"
)

const (
	scheme = "xyz"
	bucket = "b"
	object = "o"
)

func TestSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCron := synctesting.NewMockCron(ctrl)
	mockCron.EXPECT().AddFunc(gomock.Any(), gomock.Any()).DoAndReturn(func(spec string, cmd func()) error {
		return nil
	})
	mockCron.EXPECT().Start().Times(1)

	mapfs := fstest.MapFS{
		object: &fstest.MapFile{Data: []byte("hi"), ModTime: time.Now()},
	}
	fsMux := fsimpl.NewMux()
	fsMux.Add(fsimpl.WrappedFSProvider(mapfs, scheme))

	blobSync := &Sync{
		Bucket: scheme + "://" + bucket + "/",
		Object: object,
		FSMux:  fsMux,
		Cron:   mockCron,
		Logger: logger.NewLogger(nil, false),
	}

	ctx := context.Background()
	dataSyncChan := make(chan sync.DataSync, 1)

	config := "my-config"
	mapfs[object] = &fstest.MapFile{Data: []byte(config), ModTime: time.Now()}

	if err := blobSync.Init(ctx); err != nil {
		t.Errorf("init failed: %s", err)
		return
	}

	go func() {
		err := blobSync.Sync(ctx, dataSyncChan)
		if err != nil {
			log.Fatalf("Error start sync: %s", err.Error())
			return
		}
	}()

	data := <-dataSyncChan // initial sync
	if data.FlagData != config {
		t.Errorf("expected content: %s, but received content: %s", config, data.FlagData)
	}
	tickWithConfigChange(t, mockCron, dataSyncChan, mapfs, "new config")
	tickWithoutConfigChange(t, mockCron, dataSyncChan)
	tickWithConfigChange(t, mockCron, dataSyncChan, mapfs, "new config 2")
	tickWithoutConfigChange(t, mockCron, dataSyncChan)
	tickWithoutConfigChange(t, mockCron, dataSyncChan)
}

func tickWithConfigChange(t *testing.T, mockCron *synctesting.MockCron, dataSyncChan chan sync.DataSync, mapfs fstest.MapFS, newConfig string) {
	time.Sleep(1 * time.Millisecond) // sleep so the new file has different modification date
	mapfs[object] = &fstest.MapFile{Data: []byte(newConfig), ModTime: time.Now()}
	mockCron.Tick()
	select {
	case data, ok := <-dataSyncChan:
		if ok {
			if data.FlagData != newConfig {
				t.Errorf("expected content: %s, but received content: %s", newConfig, data.FlagData)
			}
		} else {
			t.Errorf("data channel unexpecdly closed")
		}
	default:
		t.Errorf("data channel has no expected update")
	}
}

func tickWithoutConfigChange(t *testing.T, mockCron *synctesting.MockCron, dataSyncChan chan sync.DataSync) {
	mockCron.Tick()
	select {
	case data, ok := <-dataSyncChan:
		if ok {
			t.Errorf("unexpected update: %s", data.FlagData)
		} else {
			t.Errorf("data channel unexpecdly closed")
		}
	default:
	}
}

func TestReSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCron := synctesting.NewMockCron(ctrl)

	mapfs := fstest.MapFS{
		object: &fstest.MapFile{Data: []byte("hi"), ModTime: time.Now()},
	}
	fsMux := fsimpl.NewMux()
	fsMux.Add(fsimpl.WrappedFSProvider(mapfs, scheme))

	blobSync := &Sync{
		Bucket: scheme + "://" + bucket + "/",
		Object: object,
		FSMux:  fsMux,
		Cron:   mockCron,
		Logger: logger.NewLogger(nil, false),
	}

	ctx := context.Background()
	dataSyncChan := make(chan sync.DataSync, 1)
	blobSync.Init(ctx)

	config := "my-config"
	mapfs[object] = &fstest.MapFile{Data: []byte(config), ModTime: time.Now()}

	err := blobSync.ReSync(ctx, dataSyncChan)
	if err != nil {
		log.Fatalf("Error start sync: %s", err.Error())
		return
	}

	data := <-dataSyncChan
	if data.FlagData != config {
		t.Errorf("expected content: %s, but received content: %s", config, data.FlagData)
	}
}
