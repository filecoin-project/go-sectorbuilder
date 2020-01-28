package fs

import "context"

// This very explicitly doesn't use fs locks - we generally only
//   care about sector ownership in the storage miner process, and
//   using fs locks would make things like seals workers on NFS quite tricky
// TODO: RW eventually
func (f *FS) Lock(ctx context.Context, sector SectorPath) error {
	for {
		f.lk.Lock()
		w, ok := f.locks[sector]
		if !ok {
			f.locks[sector] = make(chan struct{})
			f.lk.Unlock()
			return nil
		}
		f.lk.Unlock()

		log.Infof("Waiting for lock on %s", string(sector))

		select {
		case <-w:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *FS) Unlock(sector SectorPath) {
	f.lk.Lock()
	defer f.lk.Unlock()

	close(f.locks[sector])
	delete(f.locks, sector)
}
