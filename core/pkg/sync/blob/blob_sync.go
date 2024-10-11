package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/sync"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob" // needed to initialize GCS driver
)

type Sync struct {
	Bucket     string
	Object     string
	BlobURLMux *blob.URLMux
	FSMux      fsimpl.FSMux
	Cron       Cron
	Logger     *logger.Logger
	Interval   uint32

	uri         string
	fs          fs.FS
	ready       bool
	lastUpdated time.Time
}

// Cron defines the behaviour required of a cron
type Cron interface {
	AddFunc(spec string, cmd func()) error
	Start()
	Stop()
}

func (hs *Sync) Init(_ context.Context) error {
	if hs.Bucket == "" {
		return errors.New("no bucket string set")
	}
	if hs.Object == "" {
		return errors.New("no object string set")
	}

	hs.uri = fmt.Sprintf("gs://%s/%s", hs.Bucket, hs.Object)

	fs, err := hs.FSMux.Lookup(hs.uri)
	hs.fs = fs

	return err
}

func (hs *Sync) IsReady() bool {
	return hs.ready
}

func (hs *Sync) Sync(ctx context.Context, dataSync chan<- sync.DataSync) error {
	hs.Logger.Info(fmt.Sprintf("starting sync from %s/%s with interval %ds", hs.Bucket, hs.Object, hs.Interval))
	_ = hs.Cron.AddFunc(fmt.Sprintf("*/%d * * * *", hs.Interval), func() {
		err := hs.sync(ctx, dataSync, false)
		if err != nil {
			hs.Logger.Warn(fmt.Sprintf("sync failed: %v", err))
		}
	})
	// Initial fetch
	hs.Logger.Debug(fmt.Sprintf("initial sync of the %s/%s", hs.Bucket, hs.Object))
	err := hs.sync(ctx, dataSync, false)
	if err != nil {
		return err
	}

	hs.ready = true
	hs.Cron.Start()
	<-ctx.Done()
	hs.Cron.Stop()

	return nil
}

func (hs *Sync) ReSync(ctx context.Context, dataSync chan<- sync.DataSync) error {
	return hs.sync(ctx, dataSync, true)
}

func (hs *Sync) sync(ctx context.Context, dataSync chan<- sync.DataSync, skipCheckingModTime bool) error {
	file, err := hs.fs.Open(hs.uri)
	if err != nil {
		return fmt.Errorf("couldn't get bucket: %v", err)
	}
	defer file.Close()
	var updated time.Time
	if !skipCheckingModTime {
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("couldn't get object attributes: %v", err)
		}
		updated := stat.ModTime()
		if hs.lastUpdated == updated {
			hs.Logger.Debug("configuration hasn't changed, skipping fetching full object")
			return nil
		}
		if hs.lastUpdated.After(updated) {
			hs.Logger.Warn("configuration changed but the modification time decreased instead of increasing")
		}
	}

	bytes, err := io.ReadAll(file)
	msg := string(bytes)
	if err != nil {
		return fmt.Errorf("couldn't get object: %v", err)
	}
	hs.Logger.Debug(fmt.Sprintf("configuration updated: %s", msg))
	if !skipCheckingModTime {
		hs.lastUpdated = updated
	}
	dataSync <- sync.DataSync{FlagData: msg, Source: hs.Bucket + hs.Object, Type: sync.ALL}
	return nil
}
